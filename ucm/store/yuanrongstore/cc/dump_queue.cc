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
// *** DumpQueue: 卸载队列，负责将设备内存中的KV缓存数据写入yuanrong远端和后端存储
// *** 设计：双线程模型——dispatcher线程负责D2H拷贝+yuanrong写入+提交后端卸载，dumper线程负责等待后端卸载完成
// *** 与MooncakeStore的差异：
// *** 1. 使用HeteroClient::Exist替代RealClient::batchIsExist（返回vector<bool>）
// *** 2. 使用HeteroClient::MSetD2H替代RealClient::batch_put_from_multi_buffers
// *** 3. MSetD2H使用SetParam，而非Mooncake的ReplicateConfig（无replicaNum/withSoftPin）
#include "dump_queue.h"
#include <acl/acl_rt.h>
#include <chrono>
#include "logger/logger.h"
#include "thread/cpu_affinity.h"

namespace UC::YuanrongStore {

DumpQueue::~DumpQueue() { Close(); }

// *** Close: 停止线程并清理队列中残留任务
void DumpQueue::Close()
{
    if (stop_.exchange(true)) { return; }  // *** 原子标记停止
    if (dispatcher_.joinable()) { dispatcher_.join(); }  // *** 等待调度线程结束
    if (dumper_.joinable()) { dumper_.join(); }  // *** 等待后端卸载线程结束

    TaskPair pair;
    while (waiting_.TryPop(pair)) {
        if (pair.second) { pair.second->Done(); }
    }
    DumpCtx ctx;
    while (dumping_.TryPop(ctx)) {}
}

// *** Setup: 初始化卸载队列，创建dispatcher和dumper两个工作线程
Status DumpQueue::Setup(const Config& config, TaskIdSet* failureSet,
                        std::shared_ptr<datasystem::HeteroClient> heteroClient, StoreV1* backend,
                        HostBufferPool* bufPool)
{
    failureSet_ = failureSet;
    heteroClient_ = std::move(heteroClient);
    backend_ = backend;
    bufPool_ = bufPool;  // *** 主机端缓冲池
    tensorSizes_ = config.tensorSizeList;
    deviceId_ = config.deviceId;
    streamNumber_ = config.streamNumber;
    cpuAffinityCores_ = config.cpuAffinityCores;

    // *** MSet参数
    writeMode_ = config.writeMode;
    ttlSecond_ = config.ttlSecond;
    cacheType_ = config.cacheType;

    waiting_.Setup(config.dumpQueueDepth);
    dumping_.Setup(config.dumpQueueDepth);

    dumper_ = std::thread{&DumpQueue::BackendDumpStage, this};  // *** 先启动后端卸载线程
    std::promise<Status> started;
    auto fut = started.get_future();
    dispatcher_ = std::thread{&DumpQueue::DispatchStage, this, std::ref(started)};  // *** 启动调度线程
    return fut.get();  // *** 等待调度线程初始化完成
}

// *** Submit: 提交卸载任务到等待队列
void DumpQueue::Submit(TaskPtr task, WaiterPtr waiter)
{
    waiter->Up();  // *** 增加waiter引用计数
    if (waiting_.TryPush({task, waiter})) { return; }
    UC_ERROR("Waiting queue full, submit dump task({}) failed.", task->id);
    failureSet_->Insert(task->id);
    waiter->Done();
}

// *** DispatchStage: 调度线程主循环，先初始化CopyStream再消费等待队列
void DumpQueue::DispatchStage(std::promise<Status>& started)
{
    CopyStream stream;
    auto s = stream.Setup(deviceId_, streamNumber_);  // *** 初始化stream池
    started.set_value(s);  // *** 通知Setup线程初始化状态
    if (s.Failure()) [[unlikely]] { return; }
    if (!cpuAffinityCores_.empty()) {
        s = CpuAffinity::SetCpuAffinity4CurrentThread(cpuAffinityCores_);
        if (s.Failure()) { UC_WARN("Failed({}) to set affinity.", s); }
    }
    waiting_.ConsumerLoop(stop_, &DumpQueue::DispatchOneTask, this, stream);
}

// *** DispatchOneTask: 处理单个卸载任务
void DumpQueue::DispatchOneTask(CopyStream& stream, TaskPair&& pair)
{
    auto& task = pair.first;
    auto& waiter = pair.second;
    if (!failureSet_->Contains(task->id)) {
        auto s = DumpOneTask(stream, task);  // *** 执行完整的卸载流程
        if (s.Failure()) [[unlikely]] { failureSet_->Insert(task->id); }
    }
    waiter->Done();
}

// *** WaitPrerequisite: 等待前置事件完成（NPU计算完成后再搬运数据）
Status DumpQueue::WaitPrerequisite(TaskPtr task)
{
    if (task->prerequisiteHandle == 0) { return Status::OK(); }  // *** 无前置事件则跳过
    auto event = reinterpret_cast<aclrtEvent>(task->prerequisiteHandle);  // *** 将handle转换为Ascend event
    auto ret = aclrtSynchronizeEvent(event);  // *** 同步等待NPU计算事件完成
    if (ret != 0) {
        UC_ERROR("aclrtSynchronizeEvent failed, ret={}, task={}", ret, task->id);
        return Status::Error("aclrtSynchronizeEvent failed");
    }
    return Status::OK();
}

// *** PrepareBackendDump: 准备后端卸载——D2H异步拷贝每个shard到主机缓冲区
Status DumpQueue::PrepareBackendDump(CopyStream& stream, TaskPtr task, DumpCtx& dumpCtx,
                                     Detail::TaskDesc& backendTaskDesc)
{
    backendTaskDesc.brief = "HostBuf2Backend";  // *** 后端卸载任务：从主机缓冲区写入后端存储
    dumpCtx.taskHandle = task->id;
    for (auto& shard : task->shards) {
        auto buf = bufPool_->AcquireWithTimeout(std::chrono::milliseconds(3000));  // *** 获取主机端缓冲区
        if (!buf) {
            UC_WARN("Host buffer pool exhausted for key={}, skip backend archive", shard.key);
            break;
        }

        auto s = DeviceToHostGatherAsync(stream.NextStream(), shard.addrs.data(), buf.get());  // *** 异步D2H gather拷贝
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to do D2H batch async for task({}).", s, task->id);
            return s;
        }

        dumpCtx.hostBufs.push_back(std::move(buf));  // *** 保存主机缓冲区handle
        backendTaskDesc.push_back(
            Detail::Shard{shard.owner, shard.index, {dumpCtx.hostBufs.back().get()}});  // *** 后端卸载目标地址为主机缓冲区
    }
    return Status::OK();
}

// *** PutToYuanrong: 将缺失的shard数据通过HeteroClient::MSetD2H写入远端
// *** 与MooncakeStore的差异：使用MSetD2H替代batch_put_from_multi_buffers
// *** MSetD2H需要DeviceBlobList（含deviceIdx）和SetParam（writeMode/ttlSecond/cacheType）
Status DumpQueue::PutToYuanrong(TaskPtr task, const std::vector<std::string>& keys,
                                const std::vector<std::vector<void*>>& allBuffers,
                                const std::vector<std::vector<size_t>>& allSizes)
{
    // *** 构造SetParam
    datasystem::SetParam setParam;
    setParam.writeMode = writeMode_;  // *** 写入模式
    setParam.ttlSecond = ttlSecond_;  // *** TTL秒数
    setParam.cacheType = cacheType_;  // *** 缓存类型

    // *** 构造DeviceBlobList：每个key对应一组Blob（可能多个tensor）
    datasystem::DeviceBlobList devBlobList;
    devBlobList.deviceIdx = deviceId_;  // *** 设备索引
    devBlobList.srcOffset = 0;  // *** 源偏移量

    std::vector<datasystem::Blob> allBlobs;
    for (size_t i = 0; i < keys.size(); ++i) {
        for (size_t j = 0; j < allBuffers[i].size(); ++j) {
            allBlobs.push_back(datasystem::Blob{allBuffers[i][j], allSizes[i][j]});
        }
    }
    devBlobList.blobs = allBlobs;

    // *** 调用HeteroClient::MSetD2H批量写入
    std::vector<std::string> failedKeys;
    auto rc = heteroClient_->MSetD2H(keys, devBlobList, setParam);
    if (rc != 0) {
        UC_ERROR("HeteroClient::MSetD2H failed, rc={}, task={}", rc, task->id);
        return Status::Error("MSetD2H failed");
    }
    return Status::OK();
}

// *** SubmitBackendDump: 同步D2H拷贝完成后，提交后端卸载任务并push到dumping_队列
Status DumpQueue::SubmitBackendDump(CopyStream& stream, TaskPtr task, DumpCtx&& dumpCtx,
                                    Detail::TaskDesc&& backendTaskDesc)
{
    auto s = stream.Synchronize();  // *** 同步stream，确保所有D2H拷贝完成
    if (s.Failure()) [[unlikely]] {
        UC_ERROR("Failed({}) to sync on stream for task({}).", s, task->id);
        return s;
    }

    auto res = backend_->Dump(std::move(backendTaskDesc));  // *** 提交后端卸载任务
    if (!res) [[unlikely]] {
        UC_ERROR("Failed({}) to submit dump task({}) to backend.", res.Error(), task->id);
        return res.Error();
    }
    dumpCtx.backendTaskHandle = res.Value();  // *** 记录后端任务句柄
    dumping_.Push(std::move(dumpCtx));  // *** push到dumping_队列
    return Status::OK();
}

// *** DumpOneTask: 单个卸载任务的完整流程
// *** 1.使用HeteroClient::Exist批量检查yuanrong中已存在的key（跳过已存在的shard）
// *** 2.如有后端存储：D2H异步拷贝+准备后端卸载
// *** 3.如有缺失key：Wait前置事件+MSetD2H写入yuanrong
// *** 4.如有后端卸载：同步D2H+提交后端任务
Status DumpQueue::DumpOneTask(CopyStream& stream, TaskPtr task)
{
    auto tp = NowTime::Now();

    std::vector<std::string> keys;
    std::vector<std::vector<void*>> allBuffers;
    std::vector<std::vector<size_t>> allSizes;
    keys.reserve(task->shards.size());
    allBuffers.reserve(task->shards.size());
    allSizes.reserve(task->shards.size());
    for (auto& s : task->shards) {
        keys.push_back(s.key);
        allBuffers.push_back(s.addrs);
        allSizes.push_back(s.sizes);
    }

    // *** 使用HeteroClient::Exist查询哪些key在yuanrong中已存在
    std::vector<bool> existsResult;
    auto rc = heteroClient_->Exist(keys, existsResult);
    if (rc != 0) {
        UC_WARN("HeteroClient::Exist failed, rc={}, treating all as missing", rc);
        existsResult.assign(keys.size(), false);  // *** Exist查询失败时全部视为缺失
    }

    // *** 篮选缺失的key
    std::vector<std::string> missingKeys;
    std::vector<std::vector<void*>> missingBuffers;
    std::vector<std::vector<size_t>> missingSizes;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!existsResult[i]) {  // *** 只写入yuanrong中不存在的key
            missingKeys.push_back(keys[i]);
            missingBuffers.push_back(allBuffers[i]);
            missingSizes.push_back(allSizes[i]);
        }
    }

    UC_DEBUG("Yuanrong task({}) exists check: {}/{} keys missing.", task->id,
             missingKeys.size(), keys.size());

    Detail::TaskDesc backendTaskDesc;
    DumpCtx dumpCtx;
    // *** 如有后端存储：先设置stream等待前置事件，然后异步D2H拷贝+准备后端卸载
    if (backend_) {
        if (task->prerequisiteHandle != 0) {
            auto s = stream.WaitEvent(reinterpret_cast<void*>(task->prerequisiteHandle));  // *** stream异步等待NPU事件
            if (s.Failure()) [[unlikely]] {
                UC_ERROR("Failed({}) to set stream wait event for task({}).", s, task->id);
                return s;
            }
        }
        auto s = PrepareBackendDump(stream, task, dumpCtx, backendTaskDesc);  // *** D2H拷贝+构造后端卸载任务
        if (s.Failure()) [[unlikely]] { return s; }
    }

    // *** 写入缺失的key到yuanrong（需同步等待前置事件完成）
    if (!missingKeys.empty()) {
        auto s = WaitPrerequisite(task);  // *** 同步等待NPU计算完成
        if (s.Failure()) [[unlikely]] { return s; }
        s = PutToYuanrong(task, missingKeys, missingBuffers, missingSizes);  // *** 批量写入yuanrong
        if (s.Failure()) [[unlikely]] { return s; }
    }

    // *** 如有主机缓冲区（即有后端卸载任务）：同步stream+提交后端卸载
    if (!dumpCtx.hostBufs.empty()) {
        auto s = SubmitBackendDump(stream, task, std::move(dumpCtx), std::move(backendTaskDesc));
        if (s.Failure()) [[unlikely]] { return s; }
    }

    UC_DEBUG("Yuanrong task({}) dump done, cost={:.3f}ms.", task->id, (NowTime::Now() - tp) * 1e3);
    return Status::OK();
}

// *** BackendDumpStage: 后端卸载线程主循环，消费dumping_队列并等待每个后端卸载任务完成
void DumpQueue::BackendDumpStage()
{
    if (!cpuAffinityCores_.empty()) {
        auto s = CpuAffinity::SetCpuAffinity4CurrentThread(cpuAffinityCores_);
        if (s.Failure()) { UC_WARN("Failed({}) to set affinity.", s); }
    }
    dumping_.ConsumerLoop(stop_, [this](auto&& task) {
        if (task.backendTaskHandle > finishedBackendTaskHandle_) {  // *** 仅等待比自己之前未完成的任务
            auto s = backend_->Wait(task.backendTaskHandle);  // *** 等待后端卸载完成
            finishedBackendTaskHandle_ = task.backendTaskHandle;
            if (s.Failure()) {
                UC_ERROR("Failed({}) to wait backend({}) for task({}).", s, task.backendTaskHandle,
                         task.taskHandle);
                return;
            }
        }
    });
}

// *** DeviceToHostGatherAsync: 将设备端多个tensor地址gather拷贝到主机端连续缓冲区
Status DumpQueue::DeviceToHostGatherAsync(std::shared_ptr<Trans::Stream> stream, void** device,
                                          void* host)
{
    const auto number = tensorSizes_.size();
    for (size_t i = 0, offset = 0; i < number; i++) {
        auto pDevice = device[i];  // *** 设备端第i个tensor地址
        auto pHost = (void*)(((int8_t*)host) + offset);  // *** 主机端按偏移对应位置
        auto size = tensorSizes_[i];
        auto s = stream->DeviceToHostAsync(pDevice, pHost, size);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to do D2H({}) batch({}/{}) async.", s, size, i, number);
            return s;
        }
        offset += size;
    }
    return Status::OK();
}

}  // namespace UC::YuanrongStore