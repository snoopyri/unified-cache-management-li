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
// *** CopyStream: Ascend stream池管理器，为Load/Dump操作提供多路stream并发传输
// *** 与MooncakeStore完全一致的设计，支持轮询分配stream、等待事件、同步所有stream
#ifndef UNIFIEDCACHE_YUANRONG_STORE_CC_COPY_STREAM_H
#define UNIFIEDCACHE_YUANRONG_STORE_CC_COPY_STREAM_H

#include <memory>
#include <vector>
#include "logger/logger.h"
#include "status/status.h"
#include "trans/device.h"

namespace UC::YuanrongStore {

class CopyStream {
public:
    // *** Setup: 初始化设备并创建stream池
    Status Setup(int32_t deviceId, size_t streamNumber)
    {
        Trans::Device device;
        auto s = device.Setup(deviceId);  // *** 初始化NPU设备
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to setup device({}).", s, deviceId);
            return s;
        }
        streams_.reserve(streamNumber);
        for (size_t i = 0; i < streamNumber; ++i) {
            auto stream = device.MakeSharedStream();  // *** 创建共享stream
            if (!stream) [[unlikely]] {
                UC_ERROR("Failed to make stream on device({}).", deviceId);
                return Status::Error();
            }
            streams_.push_back(std::move(stream));
        }
        deviceId_ = deviceId;
        streamNumber_ = streamNumber;
        return Status::OK();
    }

    // *** NextStream: 轮询分配下一个stream，用于并发异步传输
    std::shared_ptr<Trans::Stream> NextStream() noexcept
    {
        if (streamNumber_ == 0) [[unlikely]] { return nullptr; }
        auto& stream = streams_[streamIndex_];
        streamIndex_ = (streamIndex_ + 1) % streamNumber_;  // *** 轮询分配
        return stream;
    }

    // *** WaitEvent: 在所有stream上等待指定事件（用于异步等待NPU计算完成）
    Status WaitEvent(void* event) noexcept
    {
        auto status = Status::OK();
        for (auto& stream : streams_) {
            auto s = stream->WaitEvent(event);
            if (s.Success()) { continue; }
            UC_ERROR("Failed({}) to wait event on stream on device({}).", s, deviceId_);
            if (status.Success()) { status = s; }
        }
        return status;
    }

    // *** Synchronize: 同步所有stream，确保所有异步操作完成
    Status Synchronize() noexcept
    {
        auto status = Status::OK();
        for (auto& stream : streams_) {
            auto s = stream->Synchronized();
            if (s.Success()) { continue; }
            UC_ERROR("Failed({}) to synchronize stream on device({}).", s, deviceId_);
            if (status.Success()) { status = s; }
        }
        return status;
    }

private:
    int32_t deviceId_{-1};  // *** NPU设备ID
    size_t streamNumber_{0};  // *** stream数量
    size_t streamIndex_{0};  // *** 当前轮询索引
    std::vector<std::shared_ptr<Trans::Stream>> streams_;  // *** stream池
};

}  // namespace UC::YuanrongStore

#endif