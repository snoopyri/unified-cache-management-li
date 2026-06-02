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
// *** TransTask/trans_task.h: 传输任务和shard的数据结构定义
// *** 与MooncakeStore的差异：TransShard不包含owner/index字段（yuanrong用key作为唯一标识）
// *** 但保留owner用于后端存储穿透时的BlockId构造
#ifndef UNIFIEDCACHE_YUANRONG_STORE_CC_TRANS_TASK_H
#define UNIFIEDCACHE_YUANRONG_STORE_CC_TRANS_TASK_H

#include <atomic>
#include <cstddef>
#include <string>
#include <vector>
#include "type/types.h"

namespace UC::YuanrongStore {

// *** 任务类型：LOAD=从远端加载，DUMP=卸载到远端
enum class TaskType { LOAD, DUMP };

// *** TransShard: 单个shard的传输描述
struct TransShard {
    std::string key;  // *** yuanrong object key（格式：hex(BlockId)_shardIndex）
    Detail::BlockId owner;  // *** 所属BlockId（16字节唯一标识，用于后端穿透）
    size_t index;  // *** shard索引（用于后端穿透时构造Shard）
    std::vector<void*> addrs;  // *** 设备端tensor地址列表
    std::vector<size_t> sizes;  // *** tensor大小列表，与addrs配对
};

// *** TransTask: 传输任务描述
struct TransTask {
    Detail::TaskHandle id{NextId()};  // *** 任务ID，全局自增生成
    TaskType type{TaskType::DUMP};  // *** 任务类型
    std::string brief;  // *** 任务简要描述
    std::vector<TransShard> shards;  // *** shard列表
    uintptr_t prerequisiteHandle{0};  // *** 前置事件句柄（aclrtEvent），Dump时需等待NPU计算完成

private:
    static Detail::TaskHandle NextId() noexcept  // *** 原子自增生成全局唯一任务ID
    {
        static std::atomic<Detail::TaskHandle> id{1};
        return id.fetch_add(1, std::memory_order_relaxed);
    }
};

}  // namespace UC::YuanrongStore

#endif