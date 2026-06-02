/**
 * MIT License
 *
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
// *** LoadQueue: 加载队列，负责从yuanrong远端加载KV缓存数据到本地设备内存
// *** 设计: 双线程模型——dispatcher线程负责调度(查找yuanrong+miss shard提交给后端)，transfer线程负责数据搬运(Host->Device)
// *** 与MooncakeStore的差异：
// *** 1. 使用HeteroClient::MGetH2D替代RealClient::batch_get_into_multi_buffers
// *** 2. MGetH2D的devBlobList包含deviceIdx，需要正确设置设备索引
// *** 3. MGetH2D返回failKeys（缺失key列表），而非Mooncake的正负数编码
#include "load_queue.h"
#include <chrono>
#include "logger/logger.h"
#include "thread/cpu_affinity.h"

namespace UC::YuanrongStore {

LoadQueue::~LoadQueue() { Close(); }

// *** Close: 停止线程并清理队列中残留任务，确保所有waiter都被Done以免死锁
void LoadQueue::Close()
{
    if (stop_.exchange(true)) { return; }  // *** 原子标记停止，防止重复关闭
    if (dispatcher_.joinable()) { dispatcher_.join(); }  // *** 等待调度线程结束
    if (transfer_.joinable()) { transfer_.join(); }  // *** 等待传输线程结束

    TaskPair pair;
    while (waiting_.TryPop(pair)) {
        if (pair.second) { pair.second->Done(); }  // *** 清理等待队列中的任务，通知waiter完成
    }
    ShardTask task;
    while (running_.TryPop(task)) {
        if (task.waiter) { task.waiter->Done(); }  // *** 清理运行队列中的任务
    }
}

// *** Setup: 初始化加载队列，创建dispatcher和transfer两个工作线程
Status LoadQueue::Setup(const Config& config, TaskIdSet* failureSet,
                        std::shared_ptr<datasystem::HeteroClient> heteroClient, StoreV1* backend,
                        HostBufferPool* bufPool)
{
    failureSet_ = failureSet;  // *** 失败任务集合，用于标记和跳过失败任务
    heteroClient_ = std::move(heteroClient);  // *** yuanrong HeteroClient
    backend_ = backend;  // *** 后端存储（如Posix），用于加载yuanrong中缺失的shard
    bufPool_ = bufPool;  // *** 主机端缓冲池，用于暂存从后端加载的数据
    tensorSizes_ = config.tensorSizeList;  // *** 每个tensor的大小列表，用于scatter H2D传输
    deviceId_ = config.deviceId;
    streamNumber_ = config.streamNumber;  // *** Ascend stream数量
    cpuAffinityCores_ = config.cpuAffinityCores;  // *** CPU亲和性配置
    subTimeoutMs_ = config.timeoutMs > 0 ? config.timeoutMs : 60000;  // *** MGetH2D超时

    waiting_.Setup(config.loadQueueDepth);  // *** 等待队列深度
    running_.Setup(config.loadQueueDepth);  // *** 运行队列深度
    holder_.reserve(1024);

    dispatcher_ = std::thread{&LoadQueue::DispatchStage, this};  // *** 启动调度线程
    std::promise<Status> started;
    auto fut = started.get_future();
    transfer_ = std::thread{&LoadQueue::TransferStage, this, std::ref(started)};  // *** 启动传输线程
    return fut.get();  // *** 等待传输线程初始化完成，返回状态
}

// *** Submit: 提交加载任务到等待队列，队列满则标记失败并通知waiter
void LoadQueue::Submit(TaskPtr task, WaiterPtr waiter)
{
    waiter->Up();  // *** 增加waiter引用计数
    if (waiting_.TryPush({task, waiter})) { return; }  // *** 成功入队
    UC_ERROR("Waiting queue full, submit load task({}) failed.", task->id);  // *** 队列满
    failureSet_->Insert(task->id);  // *** 标记任务为失败
    waiter->Done();  // *** 通知waiter完成
}

// *** DispatchStage: 调度线程主循环，消费waiting_队列中的任务并分发处理
void LoadQueue::DispatchStage()
{
    if (!cpuAffinityCores_.empty()) {
        auto s = CpuAffinity::SetCpuAffinity4CurrentThread(cpuAffinityCores_);
        if (s.Failure()) { UC_WARN("Failed({}) to set affinity.", s); }
    }
    waiting_.ConsumerLoop(stop_, &LoadQueue::DispatchOneTask, this);
}

// *** DispatchOneTask: 处理单个加载任务的核心逻辑
// *** 流程：1.尝试从yuanrong批量加载；2.全部命中则完成；3.有miss则提交给后端加载
void LoadQueue::DispatchOneTask(TaskPair&& pair)
{
    auto& task = pair.first;
    auto& waiter = pair.second;
    if (failureSet_->Contains(task->id)) {  // *** 跳过已标记为失败的任务
        waiter->Done();
        return;
    }

    auto failKeys = TryYuanrongLoad(task);  // *** 尝试从yuanrong批量加载所有shard

    if (failKeys.empty()) {  // *** 全部命中，无需后端加载，直接完成
        waiter->Done();
        UC_DEBUG("Yuanrong task({}) all hit({})", task->id, task->shards.size());
        return;
    }

    if (!backend_) {  // *** 没有后端存储，miss的shard无法加载，任务直接完成
        UC_WARN("Yuanrong miss({}/{}) with no backend, task={}, will recompute.",
                 failKeys.size(), task->shards.size(), task->id);
        waiter->Done();
        return;
    }

    if (!SubmitMissShards(task, waiter, failKeys)) { return; }  // *** 将miss shard提交给后端

    UC_DEBUG("Yuanrong task({}) dispatch shards({}, miss={})", task->id,
             task->shards.size(), failKeys.size());
}

// *** TryYuanrongLoad: 尝试从yuanrong批量加载所有shard数据到设备内存
// *** 使用HeteroClient::MGetH2D，构造DeviceBlobList（含deviceIdx）
// *** 返回failKeys（缺失的key列表），而非Mooncake的正负数编码
std::vector<std::string> LoadQueue::TryYuanrongLoad(TaskPtr task)
{
    std::vector<std::string> keys;
    std::vector<datasystem::Blob> devBlobs;  // *** yuanrong Blob列表（pointer+size）

    keys.reserve(task->shards.size());
    devBlobs.reserve(task->shards.size());

    // *** 构造MGetH2D的输入参数
    for (auto& shard : task->shards) {
        keys.push_back(shard.key);
        // *** 将shard的所有tensor地址和大小组合成连续的Blob列表
        // *** 注意：yuanrong的Blob是pointer+size对，一个shard可能有多个tensor
        for (size_t j = 0; j < shard.addrs.size(); ++j) {
            devBlobs.push_back(datasystem::Blob{shard.addrs[j], shard.sizes[j]});
        }
    }

    // *** 构造DeviceBlobList
    datasystem::DeviceBlobList devBlobList;
    devBlobList.blobs = devBlobs;
    devBlobList.deviceIdx = deviceId_;  // *** 设置设备索引，yuanrong自动管理内存映射
    devBlobList.srcOffset = 0;  // *** 源偏移量，默认为0

    std::vector<std::string> failKeys;
    // *** 调用HeteroClient::MGetH2D批量加载
    auto rc = heteroClient_->MGetH2D(keys, devBlobList, failKeys, subTimeoutMs_);
    if (rc != 0) {
        UC_WARN("HeteroClient::MGetH2D failed, rc={}, treating all as miss", rc);
        // *** MGetH2D整体失败时，所有key都视为miss
        failKeys.clear();
        for (auto& shard : task->shards) {
            failKeys.push_back(shard.key);
        }
    }

    return failKeys;
}

// *** SubmitMissShards: 将yuanrong中缺失的shard提交给后端存储加载
// *** 流程：1.从bufPool获取主机端缓冲区；2.构造后端Load任务；3.将shard信息push到running_队列
bool LoadQueue::SubmitMissShards(TaskPtr task, WaiterPtr waiter,
                                 const std::vector<std::string>& failKeys)
{
    size_t missCount = failKeys.size();
    size_t pushed = 0;

    for (size_t i = 0; i < task->shards.size(); i++) {
        // *** 检查该shard是否在failKeys中
        bool isMiss = false;
        for (auto& fk : failKeys) {
            if (fk == task->shards[i].key) {
                isMiss = true;
                break;
            }
        }
        if (!isMiss) { continue; }  // *** 跳过yuanrong中已命中的shard

        auto& shard = task->shards[i];
        auto buf = bufPool_->AcquireWithTimeout(std::chrono::milliseconds(3000));  // *** 从主机端缓冲池获取缓冲区
        if (!buf) {
            UC_ERROR("Host buffer pool exhausted for key={}", shard.key);
            failureSet_->Insert(task->id);
            waiter->Done();
            return false;
        }

        Detail::TaskDesc backendTask;
        backendTask.brief = "Backend2Host";  // *** 后端加载任务：从持久化存储加载到主机内存
        backendTask.push_back(Detail::Shard{shard.owner, shard.index, {buf.get()}});  // *** 目标地址为主机缓冲区

        auto res = backend_->Load(std::move(backendTask));
        if (!res) [[unlikely]] {
            UC_ERROR("Failed({}) to submit load task({}) to backend.", res.Error(), task->id);
            failureSet_->Insert(task->id);
            waiter->Done();
            return false;
        }

        ShardTask shardTask;
        shardTask.taskHandle = task->id;
        shardTask.backendTaskHandle = res.Value();
        shardTask.hostBuf = std::move(buf);
        shardTask.shard = std::move(shard);
        ++pushed;
        shardTask.waiter = (pushed == missCount) ? waiter : nullptr;  // *** 只有最后一个miss shard持有waiter
        running_.Push(std::move(shardTask));
    }
    return true;
}

// *** TransferStage: 传输线程主循环，消费running_队列，执行Host->Device scatter搬运
void LoadQueue::TransferStage(std::promise<Status>& started)
{
    CopyStream stream;
    auto s = stream.Setup(deviceId_, streamNumber_);  // *** 初始化Ascend stream池
    started.set_value(s);  // *** 通过promise通知Setup线程初始化状态
    if (s.Failure()) [[unlikely]] { return; }
    if (!cpuAffinityCores_.empty()) {
        s = CpuAffinity::SetCpuAffinity4CurrentThread(cpuAffinityCores_);
        if (s.Failure()) { UC_WARN("Failed({}) to set affinity.", s); }
    }
    running_.ConsumerLoop(stop_, &LoadQueue::TransferOneTask, this, stream);
}

// *** TransferOneTask: 处理单个shard的Host->Device搬运
// *** 流程：1.Wait后端加载完成；2.将主机端数据scatter拷贝到设备端多个tensor地址；3.同步stream完成
void LoadQueue::TransferOneTask(CopyStream& stream, ShardTask&& task)
{
    if (failureSet_->Contains(task.taskHandle)) {  // *** 跳过已标记失败的任务
        if (task.waiter) { task.waiter->Done(); }
        return;
    }

    auto s = Status::OK();
    do {
        s = backend_->Wait(task.backendTaskHandle);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to wait backend({}) for task({}).", s, task.backendTaskHandle,
                     task.taskHandle);
            break;
        }

        s = HostToDeviceScatterAsync(stream.NextStream(), task.hostBuf.get(),
                                     task.shard.addrs.data());  // *** 异步scatter H2D
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to do H2D batch async for task({}).", s, task.taskHandle);
            break;
        }

        if (!task.waiter) {
            holder_.push_back(std::move(task));  // *** 非最后一个shard，暂存避免缓冲区提前释放
            return;
        }

        s = stream.Synchronize();  // *** 同步所有stream，确保H2D搬运完成
        holder_.clear();  // *** 清除暂存的shard，释放主机缓冲区
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to sync on stream for task({}).", s, task.taskHandle);
            break;
        }
    } while (0);

    if (s.Failure()) [[unlikely]] { failureSet_->Insert(task.taskHandle); }
    if (task.waiter) { task.waiter->Done(); }
}

// *** HostToDeviceScatterAsync: 将主机端连续缓冲区按tensorSizeList拆分，异步scatter拷贝到设备端多个tensor地址
Status LoadQueue::HostToDeviceScatterAsync(std::shared_ptr<Trans::Stream> stream, void* host,
                                           void** device)
{
    const auto number = tensorSizes_.size();
    for (size_t i = 0, offset = 0; i < number; i++) {
        auto pHost = (void*)(((int8_t*)host) + offset);  // *** 主机端按偏移拆分
        auto pDevice = device[i];  // *** 设备端每个tensor的独立地址
        auto size = tensorSizes_[i];
        auto s = stream->HostToDeviceAsync(pHost, pDevice, size);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to do H2D({}) batch({}/{}) async.", s, size, i, number);
            return s;
        }
        offset += size;
    }
    return Status::OK();
}

}  // namespace UC::YuanrongStore