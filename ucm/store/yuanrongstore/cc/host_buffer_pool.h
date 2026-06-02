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
// *** HostBufferPool: 主机端缓冲池，为Load/Dump操作提供固定大小的Pinned/DirectIO主机内存缓冲区
// *** 与MooncakeStore完全一致的设计：预分配一大块连续主机内存，按unitSize分割为多个缓冲区
// *** 支持超时等待获取缓冲区（AcquireWithTimeout），避免缓冲池耗尽时无限等待
#ifndef UNIFIEDCACHE_YUANRONG_STORE_CC_HOST_BUFFER_POOL_H
#define UNIFIEDCACHE_YUANRONG_STORE_CC_HOST_BUFFER_POOL_H

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include "logger/logger.h"
#include "status/status.h"
#include "thread/index_pool.h"
#include "trans/buffer.h"
#include "trans/device.h"

namespace UC::YuanrongStore {

class HostBufferPool {
public:
    using Handle = std::shared_ptr<void>;

    HostBufferPool() = default;
    ~HostBufferPool() = default;

    HostBufferPool(const HostBufferPool&) = delete;
    HostBufferPool& operator=(const HostBufferPool&) = delete;

    // *** Setup: 初始化缓冲池，分配count*unitSize大小的Pinned或DirectIO主机内存
    // *** ioDirect=true时使用大页内存（适用于直接IO场景），否则使用aclrtMallocHost
    Status Setup(int32_t deviceId, uint32_t count, size_t unitSize, bool ioDirect)
    {
        if (count == 0 || unitSize == 0) { return Status::OK(); }  // *** 零参数表示不使用缓冲池
        size_t totalSize = static_cast<size_t>(count) * unitSize;
        if (totalSize / unitSize != count) {  // *** 检查整数溢出
            return Status::InvalidParam("host buffer pool size overflow");
        }

        Trans::Device device;
        auto s = device.Setup(deviceId);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to setup device({}) for host buffer pool.", s, deviceId);
            return s;
        }
        auto buffer = device.MakeBuffer();
        if (!buffer) [[unlikely]] {
            UC_ERROR("Failed to make buffer factory on device({}).", deviceId);
            return Status::Error();
        }
        auto holder = ioDirect ? buffer->MakeHostBuffer4DirectIo(totalSize)  // *** DirectIO模式：使用大页内存
                               : buffer->MakeHostBuffer(totalSize);  // *** 普通模式：使用Pinned内存(aclrtMallocHost)
        if (!holder) [[unlikely]] {
            UC_ERROR("Failed to allocate {} host buffer pool, size={}B.",
                     ioDirect ? "DirectIO_HugePages" : "Pinned", totalSize);
            return Status::OutOfMemory();
        }

        unitSize_ = unitSize;
        count_ = count;
        pool_ = std::move(holder);  // *** 保存内存持有者，析构时自动释放
        index_.Setup(count);  // *** 初始化索引池，管理缓冲区的分配/释放
        UC_INFO("HostBufferPool: {} x {}B = {}B, mode={}", count, unitSize, totalSize,
                ioDirect ? "DirectIO_HugePages" : "Pinned_aclrtMallocHost");
        return Status::OK();
    }

    // *** Acquire: 获取一个缓冲区，返回shared_ptr<void>，释放时自动归还到池中
    // *** 无缓冲区可用时返回空handle
    Handle Acquire()
    {
        if (!pool_ || unitSize_ == 0) { return {}; }  // *** 未初始化时返回空
        auto idx = index_.Acquire();  // *** 从索引池获取一个空闲索引
        if (idx == IndexPool::npos) { return {}; }  // *** 没有空闲缓冲区
        return MakeHandle(idx);
    }

    // *** AcquireWithTimeout: 获取缓冲区，超时等待。当缓冲池耗尽时，等待其他任务释放缓冲区
    Handle AcquireWithTimeout(std::chrono::milliseconds timeout)
    {
        if (!pool_ || unitSize_ == 0) { return {}; }
        auto idx = index_.Acquire();
        if (idx != IndexPool::npos) { return MakeHandle(idx); }  // *** 立即可用时直接返回
        // *** 缓冲池耗尽，进入条件变量等待，直到有缓冲区释放或超时
        std::unique_lock<std::mutex> lk(cvMtx_);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (true) {
            cv_.wait_until(lk, deadline);
            idx = index_.Acquire();
            if (idx != IndexPool::npos) { return MakeHandle(idx); }
            if (std::chrono::steady_clock::now() >= deadline) { return {}; }
        }
    }

    size_t UnitSize() const { return unitSize_; }
    uint32_t Count() const { return count_; }

private:
    // *** MakeHandle: 根据索引创建缓冲区handle，释放时自动调用ReleaseByIndex归还到池中
    Handle MakeHandle(IndexPool::Index idx)
    {
        void* raw = static_cast<char*>(pool_.get()) + static_cast<size_t>(idx) * unitSize_;  // *** 计算缓冲区起始地址
        return Handle(raw, [this, idx](void*) noexcept { this->ReleaseByIndex(idx); });  // *** 自定义deleter：释放时归还索引
    }

    void ReleaseByIndex(IndexPool::Index idx) noexcept  // *** 归还索引并通知等待线程
    {
        index_.Release(idx);  // *** 归还索引到池
        cv_.notify_one();  // *** 通知等待的AcquireWithTimeout线程
    }

    std::shared_ptr<void> pool_;
    size_t unitSize_{0};
    uint32_t count_{0};
    IndexPool index_;
    std::mutex cvMtx_;
    std::condition_variable cv_;
};

}  // namespace UC::YuanrongStore

#endif