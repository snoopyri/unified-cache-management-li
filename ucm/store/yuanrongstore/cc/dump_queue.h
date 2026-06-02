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
// *** DumpQueue: 卸载队列头文件，双线程SPSC队列模型实现KV缓存的异步卸载
// *** dispatcher线程：Exist跳已有 + MSetD2H写yuanrong + D2H拷贝 + 提交后端卸载
// *** dumper线程：等待后端卸载完成
// *** 与MooncakeStore的差异：
// *** 1. 使用HeteroClient::Exist替代RealClient::batchIsExist（返回vector<bool>而非optional<bool>）
// *** 2. 使用HeteroClient::MSetD2H替代RealClient::batch_put_from_multi_buffers
// *** 3. MSetD2H的devBlobList包含deviceIdx，需要正确设置设备索引
// *** 4. MSetD2H使用SetParam（writeMode/ttlSecond/cacheType），而非Mooncake的ReplicateConfig
#ifndef UNIFIEDCACHE_YUANRONG_STORE_CC_DUMP_QUEUE_H
#define UNIFIEDCACHE_YUANRONG_STORE_CC_DUMP_QUEUE_H

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

class DumpQueue {
    using TaskPtr = std::shared_ptr<TransTask>;
    using WaiterPtr = std::shared_ptr<Latch>;
    using TaskPair = std::pair<TaskPtr, WaiterPtr>;
    using TaskIdSet = HashSet<Detail::TaskHandle>;

    // *** DumpCtx: 卸载上下文，记录后端卸载任务信息
    struct DumpCtx {
        Detail::TaskHandle taskHandle;  // *** 原始任务ID
        Detail::TaskHandle backendTaskHandle{0};  // *** 后端卸载任务句柄
        std::vector<HostBufferPool::Handle> hostBufs;  // *** 主机端缓冲区handles，防止提前释放
    };

public:
    ~DumpQueue();

    Status Setup(const Config& config, TaskIdSet* failureSet,
                 std::shared_ptr<datasystem::HeteroClient> heteroClient, StoreV1* backend,
                 HostBufferPool* bufPool);
    void Close();
    void Submit(TaskPtr task, WaiterPtr waiter);

private:
    // *** 调度线程相关方法
    void DispatchStage(std::promise<Status>& started);  // *** 调度线程主循环
    void DispatchOneTask(CopyStream& stream, TaskPair&& pair);  // *** 处理单个卸载任务
    Status DumpOneTask(CopyStream& stream, TaskPtr task);  // *** 完整卸载流程
    Status WaitPrerequisite(TaskPtr task);  // *** 等待NPU前置事件完成
    Status PrepareBackendDump(CopyStream& stream, TaskPtr task, DumpCtx& dumpCtx,
                              Detail::TaskDesc& backendTaskDesc);  // *** D2H拷贝+构造后端卸载
    Status PutToYuanrong(TaskPtr task, const std::vector<std::string>& keys,
                         const std::vector<std::vector<void*>>& allBuffers,
                         const std::vector<std::vector<size_t>>& allSizes);  // *** 写入yuanrong
    Status SubmitBackendDump(CopyStream& stream, TaskPtr task, DumpCtx&& dumpCtx,
                             Detail::TaskDesc&& backendTaskDesc);  // *** 提交后端卸载
    void BackendDumpStage();  // *** 后端卸载线程主循环

    // *** DeviceToHostGatherAsync: 将设备端多个tensor gather拷贝到主机端连续缓冲区
    Status DeviceToHostGatherAsync(std::shared_ptr<Trans::Stream> stream, void** device,
                                   void* host);

private:
    alignas(64) std::atomic_bool stop_{false};  // *** 停止标记，64字节对齐避免false sharing
    Detail::TaskHandle finishedBackendTaskHandle_{0};  // *** 已完成后端卸载的最高handle
    TaskIdSet* failureSet_{nullptr};
    std::shared_ptr<datasystem::HeteroClient> heteroClient_;  // *** yuanrong HeteroClient
    StoreV1* backend_{nullptr};  // *** 后端存储指针
    HostBufferPool* bufPool_{nullptr};  // *** 主机端缓冲池
    std::vector<size_t> tensorSizes_;  // *** tensor大小列表
    int32_t deviceId_{-1};  // *** NPU设备ID
    size_t streamNumber_{1};  // *** stream数量
    std::vector<ssize_t> cpuAffinityCores_;  // *** CPU亲和核心列表

    // *** MSet参数
    int32_t writeMode_{0};  // *** 写入模式
    int32_t ttlSecond_{0};  // *** TTL秒数
    int32_t cacheType_{0};  // *** 缓存类型

    SpscRingQueue<TaskPair> waiting_;  // *** 等待队列（dispatcher消费）
    SpscRingQueue<DumpCtx> dumping_;  // *** 卸载队列（dumper消费）

    std::thread dispatcher_;  // *** 调度线程
    std::thread dumper_;  // *** 后端卸载线程
};

}  // namespace UC::YuanrongStore

#endif