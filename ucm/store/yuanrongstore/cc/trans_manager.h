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
// *** TransManager头文件：传输管理器，继承TaskWrapper，管理Load/Dump队列和HeteroClient
// *** 与MooncakeStore的差异：使用datasystem::HeteroClient替代mooncake::RealClient
// *** 对外提供Setup/Close/Submit/Check/Wait接口，内部根据任务类型分发到对应队列
#ifndef UNIFIEDCACHE_YUANRONG_STORE_CC_TRANS_MANAGER_H
#define UNIFIEDCACHE_YUANRONG_STORE_CC_TRANS_MANAGER_H

#include <memory>
#include "dump_queue.h"
#include "global_config.h"
#include "host_buffer_pool.h"
#include "load_queue.h"
#include "template/task_wrapper.h"
#include "trans_task.h"
#include "type/types.h"

// *** yuanrong-datasystem HeteroClient头文件
#include "datasystem/hetero_client.h"

namespace UC::YuanrongStore {

class TransManager : public Detail::TaskWrapper<TransTask, Detail::TaskHandle> {  // *** 继承TaskWrapper，提供任务管理框架
public:
    Status Setup(const Config& config);  // *** 初始化传输管理器
    void Close();  // *** 关闭队列和线程
    std::shared_ptr<datasystem::HeteroClient> GetHeteroClient() const { return heteroClient_; }  // *** 获取HeteroClient

protected:
    void Dispatch(TaskPtr t, WaiterPtr w) override;

private:
    Status SetupHeteroClient(const Config& config);  // *** 创建并初始化HeteroClient

    std::shared_ptr<datasystem::HeteroClient> heteroClient_;  // *** yuanrong HeteroClient
    HostBufferPool bufPool_;  // *** 主机端缓冲池
    LoadQueue loadQ_;  // *** 加载队列
    DumpQueue dumpQ_;  // *** 卸载队列
    Config config_;  // *** 存储配置
};

}  // namespace UC::YuanrongStore

#endif