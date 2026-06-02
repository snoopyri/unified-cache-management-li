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
// *** TransManager: 传输管理器，管理Load/Dump队列和HeteroClient
// *** 继承TaskWrapper，实现任务的提交/检查/等待接口
// *** 内部持有LoadQueue和DumpQueue，根据任务类型分发到对应队列
// *** 与MooncakeStore的差异：使用HeteroClient::Init替代RealClient::setup_real
#include "trans_manager.h"
#include <numeric>
#include "logger/logger.h"

namespace UC::YuanrongStore {

// *** Setup: 初始化传输管理器
// *** 流程：1.创建HeteroClient；2.初始化主机缓冲池；3.初始化Load和Dump队列
Status TransManager::Setup(const Config& config)
{
    config_ = config;
    timeoutMs_ = config.timeoutMs;  // *** 任务超时时间

    auto s = SetupHeteroClient(config);  // *** 初始化HeteroClient
    if (s.Failure()) { return s; }

    // *** 计算主机缓冲区单元大小：所有tensor大小之和
    size_t hostBufUnitSize =
        std::accumulate(config.tensorSizeList.begin(), config.tensorSizeList.end(), uint64_t{0});
    if (hostBufUnitSize > 0 && config.hostBufPoolSize > 0) {
        s = bufPool_.Setup(config.deviceId, config.hostBufPoolSize, hostBufUnitSize,
                           config.ioDirect);  // *** 初始化主机缓冲池（Pinned内存或DirectIO HugePages）
        if (s.Failure()) { return s; }
    }

    s = loadQ_.Setup(config, &failureSet_, heteroClient_, config.storeBackend, &bufPool_);  // *** 初始化加载队列
    if (s.Failure()) { return s; }
    s = dumpQ_.Setup(config, &failureSet_, heteroClient_, config.storeBackend, &bufPool_);  // *** 初始化卸载队列
    if (s.Failure()) { return s; }

    UC_INFO("TransManager setup ok, backend={}", config.storeBackend ? "yes" : "none");
    return Status::OK();
}

void TransManager::Close()
{
    loadQ_.Close();
    dumpQ_.Close();

    // *** 关闭HeteroClient连接
    if (heteroClient_) {
        auto rc = heteroClient_->ShutDown();  // *** 调用HeteroClient::ShutDown关闭连接
        if (rc != 0) {
            UC_WARN("HeteroClient::ShutDown failed, rc={}", rc);
        }
    }
}

// *** SetupHeteroClient: 创建并初始化yuanrong HeteroClient
// *** 使用ConnectOptions配置连接参数，调用Init建立连接
Status TransManager::SetupHeteroClient(const Config& config)
{
    heteroClient_ = std::make_shared<datasystem::HeteroClient>();  // *** 创建HeteroClient实例
    if (!heteroClient_) {
        UC_ERROR("HeteroClient::create failed");
        return Status::Error("HeteroClient::create failed");
    }

    // *** 构造连接选项
    datasystem::ConnectOptions opts;
    opts.host = config.localHostname;  // *** 主机名/IP
    opts.port = config.port;  // *** 端口
    opts.deviceId = config.deviceId;  // *** 设备ID
    opts.enableRemoteH2D = config.enableRemoteH2D;  // *** 是否启用远程H2D

    auto rc = heteroClient_->Init(opts);  // *** 初始化连接
    if (rc != 0) {
        UC_ERROR("HeteroClient::Init failed, rc={}", rc);
        heteroClient_.reset();
        return Status::Error("HeteroClient::Init failed");
    }
    UC_INFO("HeteroClient::Init ok, host={}, port={}, deviceId={}, enableRemoteH2D={}",
            config.localHostname, config.port, config.deviceId, config.enableRemoteH2D);
    return Status::OK();
}

// *** Dispatch: 根据任务类型分发到Load或Dump队列
void TransManager::Dispatch(TaskPtr t, WaiterPtr w)
{
    if (t->type == TaskType::LOAD) {
        loadQ_.Submit(t, w);  // *** 加载任务提交到LoadQueue
    } else {
        dumpQ_.Submit(t, w);  // *** 卸载任务提交到DumpQueue
    }
}

}  // namespace UC::YuanrongStore