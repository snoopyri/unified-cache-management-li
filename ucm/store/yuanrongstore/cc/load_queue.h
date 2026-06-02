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
// *** LoadQueue: 加载队列头文件，双线程SPSC队列模型实现KV缓存的异步加载
// *** dispatcher线程：HeteroClient::MGetH2D尝试加载 + miss shard提交给后端
// *** transfer线程：后端加载完成后执行Host->Device scatter搬运
// *** 与MooncakeStore的差异：使用HeteroClient::MGetH2D替代RealClient::batch_get_into_multi_buffers
// *** MGetH2D返回failKeys（缺失key列表）而非正负数编码，需要适配结果判断
#ifndef UNIFIEDCACHE_YUANRONG_STORE_CC_LOAD_QUEUE_H
#define UNIFIEDCACHE_YUANRONG_STORE_CC_LOAD_QUEUE_H

#include <atomic>
#include <future>
#include <memory>
#include <thread>
#include <vector>
#include "copy_stream.h"
#include "global_config.h"
#include "host_buffer_pool.h"
#include "template/hashset.h"
#include "template/spsc_ring_queue.h"
#include "thread/latch.h"
#include "trans_task.h"
#include "type/types.h"
#include "ucmstore_v1.h"

// *** yuanrong-datasystem头文件
#include "datasystem/hetero_client.h"

namespace UC::YuanrongStore {

class LoadQueue {
    using TaskPtr = std::shared_ptr<TransTask>;  // *** 传输任务智能指针
    using WaiterPtr = std::shared_ptr<Latch>;  // *** 等待器，用于同步任务完成
    using TaskPair = std::pair<TaskPtr, WaiterPtr>;  // *** 任务+等待器配对
    using TaskIdSet = HashSet<Detail::TaskHandle>;  // *** 失败任务ID集合

    // *** ShardTask: 传输线程处理的单个shard任务描述
    struct ShardTask {
        Detail::TaskHandle taskHandle;  // *** 原始任务ID
        Detail::TaskHandle backendTaskHandle{0};  // *** 后端加载任务句柄
        TransShard shard;  // *** shard信息（含设备端地址）
        HostBufferPool::Handle hostBuf;  // *** 主机端缓冲区handle
        WaiterPtr waiter;  // *** 只有最后一个miss shard持有waiter
    };

public:
    ~LoadQueue();

    // *** Setup: 初始化队列和线程
    Status Setup(const Config& config, TaskIdSet* failureSet,
                 std::shared_ptr<datasystem::HeteroClient> heteroClient, StoreV1* backend,
                 HostBufferPool* bufPool);
    void Close();  // *** 关闭队列和线程
    void Submit(TaskPtr task, WaiterPtr waiter);  // *** 提交任务到等待队列

private:
    // *** 调度线程相关方法
    void DispatchStage();  // *** 调度线程主循环
    void DispatchOneTask(TaskPair&& pair);  // *** 处理单个任务：查找+miss提交
    std::vector<std::string> TryYuanrongLoad(TaskPtr task);  // *** 批量尝试从yuanrong加载，返回failKeys
    bool SubmitMissShards(TaskPtr task, WaiterPtr waiter,
                          const std::vector<std::string>& failKeys);  // *** 将miss shard提交给后端
    // *** 传输线程相关方法
    void TransferStage(std::promise<Status>& started);  // *** 传输线程主循环
    void TransferOneTask(CopyStream& stream, ShardTask&& task);  // *** 执行H2D搬运

    // *** HostToDeviceScatterAsync: 将主机连续缓冲区scatter拷贝到设备端多个tensor
    Status HostToDeviceScatterAsync(std::shared_ptr<Trans::Stream> stream, void* host,
                                    void** device);

private:
    alignas(64) std::atomic_bool stop_{false};  // *** 停止标记，64字节对齐避免false sharing
    TaskIdSet* failureSet_{nullptr};  // *** 失败任务ID集合指针
    std::shared_ptr<datasystem::HeteroClient> heteroClient_;  // *** yuanrong HeteroClient
    StoreV1* backend_{nullptr};  // *** 后端存储指针
    HostBufferPool* bufPool_{nullptr};  // *** 主机端缓冲池指针
    std::vector<size_t> tensorSizes_;  // *** tensor大小列表
    int32_t deviceId_{-1};  // *** NPU设备ID
    size_t streamNumber_{1};  // *** Ascend stream数量
    std::vector<ssize_t> cpuAffinityCores_;  // *** CPU亲和核心列表
    size_t subTimeoutMs_{60000};  // *** MGetH2D子操作超时（毫秒）

    SpscRingQueue<TaskPair> waiting_;  // *** 等待队列（dispatcher消费）
    SpscRingQueue<ShardTask> running_;  // *** 运行队列（transfer消费）
    std::vector<ShardTask> holder_;  // *** 暂存非最后一个shard，避免缓冲区提前释放

    std::thread dispatcher_;  // *** 调度线程
    std::thread transfer_;  // *** 传输线程
};

}  // namespace UC::YuanrongStore

#endif