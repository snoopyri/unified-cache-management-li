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
// *** YuanrongStore: 基于yuanrong-datasystem HeteroClient的KV缓存存储后端实现
// *** 核心功能：通过HeteroClient::MGetH2D/MSetD2D/Exist实现跨节点的KV缓存加载(Load)和卸载(Dump)，支持前缀查找(LookupOnPrefix)
// *** 设计要点：支持两种模式——传输模式(deviceId>=0)启用HeteroClient数据搬运，调度模式(deviceId<0)仅做RPC查找
// *** 与MooncakeStore的关键差异：
// *** 1. 使用HeteroClient替代mooncake::RealClient/Client
// *** 2. RegisterMemory空实现（yuanrong内部通过DeviceBlobList.deviceIdx自动管理设备内存映射）
// *** 3. Exist返回vector<bool>而非optional<bool>，需要用GetMetaInfo补充-1（查询失败）逻辑
// *** 4. yuanrong没有replica_num参数，副本由集群管理
#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include "global_config.h"
#include "logger/logger.h"
#include "trans_manager.h"
#include "ucmstore_v1.h"

// *** yuanrong-datasystem HeteroClient头文件
#include "datasystem/hetero_client.h"

namespace UC::YuanrongStore {

namespace {
// *** 十六进制查找表，用于将BlockId字节序列转换为十六进制字符串键
constexpr char kHexTable[] = "0123456789abcdef";

// *** BlockId转键：将16字节BlockId转为32字符十六进制字符串，作为yuanrong中的object key
std::string BlockIdToKey(const Detail::BlockId& block)
{
    std::string out;
    out.resize(block.size() * 2);
    for (size_t i = 0; i < block.size(); ++i) {
        auto b = static_cast<uint8_t>(block[i]);
        out[i * 2] = kHexTable[b >> 4];
        out[i * 2 + 1] = kHexTable[b & 0x0F];
    }
    return out;
}
}  // namespace

// *** YuanrongStore类：继承StoreV1接口，实现yuanrong存储后端的完整生命周期
// *** 包含TransManager(传输管理器)、HeteroClient(调度模式下的查找客户端)两大核心组件
class YuanrongStore : public StoreV1 {
    TransManager transMgr_;  // *** 传输管理器：管理Load/Dump队列和HeteroClient
    Config config_;  // *** 存储配置参数
    bool transEnable_{false};  // *** 是否启用传输模式（deviceId>=0时启用）

    std::shared_ptr<datasystem::HeteroClient> rpcClient_;  // *** RPC客户端：调度模式下仅用于查找

    std::atomic<bool> closed_{false};  // *** 关闭标记，确保Close只执行一次

public:
    ~YuanrongStore() override { Close(); }

    // *** Close: 关闭存储后端，先标记closed，再关闭传输管理器
    // *** 与MooncakeStore的差异：不需要注销内存（RegisterMemory是空实现）
    void Close()
    {
        if (closed_.exchange(true, std::memory_order_acq_rel)) { return; }  // *** 原子交换确保只关闭一次
        transMgr_.Close();  // *** 关闭Load/Dump队列线程和HeteroClient

        // *** 调度模式下关闭RPC客户端
        if (rpcClient_) {
            auto rc = rpcClient_->ShutDown();  // *** 调用HeteroClient::ShutDown关闭连接
            if (rc != 0) {
                UC_WARN("RPC HeteroClient::ShutDown failed, rc={}", rc);
            }
        }
    }

    // *** Setup: 初始化存储后端，解析配置并根据deviceId决定启用传输模式或调度模式
    Status Setup(const Detail::Dictionary& inConfig) override
    {
        auto config = ParseConfig(inConfig);  // *** 从字典中解析所有配置参数
        auto s = CheckConfig(config);  // *** 校验必要参数（local_hostname）
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Config check failed: {}.", s);
            return s;
        }
        config_ = config;
        transEnable_ = config.deviceId >= 0;  // *** deviceId>=0表示有NPU设备，启用传输模式

        if (transEnable_) {
            s = transMgr_.Setup(config);  // *** 传输模式：初始化HeteroClient和队列
            if (s.Failure()) [[unlikely]] { return s; }
        } else {
            s = SetupRpcClient(config);  // *** 调度模式：仅建立RPC客户端用于查找
            if (s.Failure()) [[unlikely]] { return s; }
        }
        ShowConfig(config);
        return Status::OK();
    }

    std::string Readme() const override { return "YuanrongStore"; }

    // *** Lookup: 全量查找，返回每个block是否存在的布尔向量
    Expected<std::vector<uint8_t>> Lookup(const Detail::BlockId* blocks, size_t num) override
    {
        if (num == 0) { return std::vector<uint8_t>{}; }

        auto res = LookupOnPrefix(blocks, num);  // *** 调用前缀查找获取连续命中位置
        if (!res) [[unlikely]] { return res.Error(); }

        std::vector<uint8_t> results(num, 0);
        const auto index = res.Value();
        for (ssize_t i = 0; i <= index; ++i) { results[i] = 1; }  // *** 0~index范围内的block标记为存在
        return results;
    }

    // *** LookupOnPrefix: 前缀查找，返回连续命中的最后一个block索引
    // *** 流程：先在yuanrong中批量查找，若全部命中则返回num-1；若有缺失则fallback到storeBackend
    // *** 与MooncakeStore的差异：使用HeteroClient::Exist替代mooncake::Client::BatchIsExist
    // *** Exist返回vector<bool>而非optional<bool>，查询失败时需要用GetMetaInfo补充
    Expected<ssize_t> LookupOnPrefix(const Detail::BlockId* blocks, size_t num) override
    {
        if (num == 0) { return static_cast<ssize_t>(-1); }

        std::vector<std::string> keys;
        keys.reserve(num);
        for (size_t i = 0; i < num; ++i) {
            keys.push_back(BlockIdToKey(blocks[i]) + "_0");  // *** 构造查找键
        }

        auto exists = RpcBatchIsExist(keys);  // *** 通过RPC批量查询yuanrong中哪些key存在

        ssize_t firstMiss = -1;
        for (size_t i = 0; i < num; ++i) {
            if (exists[i] != 1) {  // *** 0=不存在，-1=查询失败
                firstMiss = static_cast<ssize_t>(i);
                break;
            }
        }

        if (firstMiss == -1) { return static_cast<ssize_t>(num) - 1; }  // *** 全部命中

        // *** 有后端存储时，将缺失部分交给storeBackend继续查找
        if (config_.storeBackend) {
            auto backendRes =
                config_.storeBackend->LookupOnPrefix(blocks + firstMiss, num - firstMiss);
            if (backendRes) {
                ssize_t backendHit = backendRes.Value();
                if (backendHit >= 0) { return firstMiss + backendHit; }
            }
        }

        return firstMiss - 1;
    }

    // *** Prefetch: 当前未实现预取功能
    void Prefetch(const Detail::BlockId* blocks, size_t num) override
    {
        (void)blocks;
        (void)num;
    }

    // *** Load: 从yuanrong远端加载KV缓存数据到本地设备内存
    Expected<Detail::TaskHandle> Load(Detail::TaskDesc task) override
    {
        if (!transEnable_) { return Status::Error("transfer is not enabled (scheduler mode)"); }
        TransTask transTask;
        transTask.type = TaskType::LOAD;
        transTask.brief = task.brief;
        BuildShards(task, transTask);
        return transMgr_.Submit(std::move(transTask));
    }

    // *** Dump: 将本地设备内存中的KV缓存数据卸载到yuanrong远端
    Expected<Detail::TaskHandle> Dump(Detail::TaskDesc task) override
    {
        if (!transEnable_) { return Status::Error("transfer is not enabled (scheduler mode)"); }
        TransTask transTask;
        transTask.type = TaskType::DUMP;
        transTask.brief = task.brief;
        transTask.prerequisiteHandle = task.prerequisiteHandle;
        BuildShards(task, transTask);
        return transMgr_.Submit(std::move(transTask));
    }

    Expected<bool> Check(Detail::TaskHandle taskId) override { return transMgr_.Check(taskId); }

    Status Wait(Detail::TaskHandle taskId) override { return transMgr_.Wait(taskId); }

    // *** RegisterMemory: 空实现（yuanrong内部通过DeviceBlobList.deviceIdx自动管理设备内存映射）
    // *** 与MooncakeStore的差异：不需要像Mooncake那样手动register_buffer
    Status RegisterMemory(void* base_addr, size_t total_size) override
    {
        // *** yuanrong通过DeviceBlobList.deviceIdx自动管理设备内存映射，无需手动注册
        UC_DEBUG("RegisterMemory skipped (yuanrong manages device memory internally), addr={}, size={}",
                 base_addr, total_size);
        return Status::OK();
    }

private:
    // *** SetupRpcClient: 调度模式下创建HeteroClient，仅用于查找操作
    Status SetupRpcClient(const Config& config)
    {
        rpcClient_ = std::make_shared<datasystem::HeteroClient>();  // *** 创建HeteroClient实例
        if (!rpcClient_) {
            UC_ERROR("RPC HeteroClient::create failed");
            return Status::Error("RPC HeteroClient::create failed");
        }

        datasystem::ConnectOptions opts;
        opts.host = config.localHostname;
        opts.port = config.port;
        opts.deviceId = -1;  // *** 调度模式下deviceId=-1
        opts.enableRemoteH2D = false;  // *** 调度模式不启用远程H2D

        auto rc = rpcClient_->Init(opts);  // *** 初始化连接
        if (rc != 0) {
            UC_ERROR("RPC HeteroClient::Init failed, rc={}", rc);
            rpcClient_.reset();
            return Status::Error("RPC HeteroClient::Init failed");
        }
        UC_DEBUG("RPC HeteroClient setup ok (lookup only)");
        return Status::OK();
    }

    // *** RpcBatchIsExist: 批量查询yuanrong中哪些key存在
    // *** 返回值：1=存在，0=不存在，-1=查询失败
    // *** 与MooncakeStore的差异：使用HeteroClient::Exist替代Client::BatchIsExist
    // *** Exist只返回vector<bool>，查询失败需要用GetMetaInfo补充判断
    std::vector<int> RpcBatchIsExist(const std::vector<std::string>& keys)
    {
        if (keys.empty()) { return std::vector<int>(keys.size(), -1); }

        if (!rpcClient_) { return std::vector<int>(keys.size(), -1); }  // *** 无RPC客户端

        // *** 调用HeteroClient::Exist批量查询
        std::vector<bool> existsResult;
        auto rc = rpcClient_->Exist(keys, existsResult);
        if (rc != 0) {
            // *** Exist查询失败时，尝试用GetMetaInfo补充判断
            UC_WARN("HeteroClient::Exist failed, rc={}, trying GetMetaInfo", rc);
            return RpcBatchIsExistViaMetaInfo(keys);  // *** fallback到GetMetaInfo查询
        }

        // *** 转换vector<bool>为int编码：true→1，false→0
        std::vector<int> out;
        out.reserve(existsResult.size());
        for (auto b : existsResult) { out.push_back(b ? 1 : 0); }
        return out;
    }

    // *** RpcBatchIsExistViaMetaInfo: 当Exist查询失败时，用GetMetaInfo补充判断
    // *** GetMetaInfo返回MetaInfo（含blobSizeList），如果blobSizeList非空则说明key存在
    std::vector<int> RpcBatchIsExistViaMetaInfo(const std::vector<std::string>& keys)
    {
        std::vector<int> out(keys.size(), -1);  // *** 默认全部为-1（查询失败）

        std::vector<datasystem::MetaInfo> metaInfos;
        std::vector<std::string> failKeys;
        auto rc = rpcClient_->GetMetaInfo(keys, false, metaInfos, failKeys);
        if (rc != 0) {
            UC_WARN("HeteroClient::GetMetaInfo also failed, rc={}", rc);
            return out;  // *** GetMetaInfo也失败，全部返回-1
        }

        // *** failKeys中的key不存在，其他key通过blobSizeList判断是否存在
        for (size_t i = 0; i < keys.size(); ++i) {
            bool inFailKeys = false;
            for (auto& fk : failKeys) {
                if (fk == keys[i]) { inFailKeys = true; break; }
            }
            if (inFailKeys) {
                out[i] = 0;  // *** 在failKeys中→不存在
            } else if (i < metaInfos.size() && !metaInfos[i].blobSizeList.empty()) {
                out[i] = 1;  // *** blobSizeList非空→存在
            } else {
                out[i] = 0;  // *** blobSizeList空→不存在
            }
        }
        return out;
    }

    // *** BuildShards: 将TaskDesc中的shard描述转换为TransShard
    // *** 与MooncakeStore完全一致的逻辑：构造key、配对地址和大小
    void BuildShards(const Detail::TaskDesc& desc, TransTask& out)
    {
        out.shards.reserve(desc.size());
        for (const auto& shard : desc) {
            std::string key = BlockIdToKey(shard.owner) + "_" + std::to_string(shard.index);

            if (shard.addrs.size() > config_.tensorSizeList.size()) {
                UC_WARN("BuildShards: key={} has {} addrs but tensorSizeList has only {}, truncating",
                        key, shard.addrs.size(), config_.tensorSizeList.size());
            }

            size_t count = std::min(shard.addrs.size(), config_.tensorSizeList.size());
            std::vector<void*> addrs(shard.addrs.begin(), shard.addrs.begin() + count);
            std::vector<size_t> sizes(config_.tensorSizeList.begin(),
                                      config_.tensorSizeList.begin() + count);

            out.shards.push_back(TransShard{std::move(key), shard.owner, shard.index,
                                            std::move(addrs), std::move(sizes)});
        }
    }

    // *** ParseConfig: 从配置字典中解析所有参数到Config结构体
    // *** 与MooncakeStore的差异：无metadataServer/masterServerAddress/globalSegmentSize/localBufferSize
    // *** 增加port/enableRemoteH2D/writeMode/ttlSecond/cacheType
    Config ParseConfig(const Detail::Dictionary& inConfig)
    {
        Config config;
        inConfig.Get("local_hostname", config.localHostname);

        auto colonPos = config.localHostname.rfind(':');
        if (colonPos != std::string::npos) {
            UC_WARN("local_hostname contains port '{}', using separate port config",
                    config.localHostname);
            // *** 不剥离端口号，因为yuanrong有独立的port参数
        }

        inConfig.GetNumber("port", config.port);  // *** yuanrong端口参数
        inConfig.GetNumber("device_id", config.deviceId);
        inConfig.Get("enable_remote_h2d", config.enableRemoteH2D);  // *** 是否启用远程H2D
        inConfig.GetNumbers("tensor_size_list", config.tensorSizeList);
        inConfig.GetNumber("dump_queue_depth", config.dumpQueueDepth);
        inConfig.GetNumber("load_queue_depth", config.loadQueueDepth);
        inConfig.GetNumber("host_buf_pool_size", config.hostBufPoolSize);
        inConfig.GetNumber("timeout_ms", config.timeoutMs);
        inConfig.GetNumber("stream_number", config.streamNumber);
        inConfig.Get("cpu_affinity_cores", config.cpuAffinityCores);
        inConfig.Get("io_direct", config.ioDirect);
        inConfig.GetNumber("write_mode", config.writeMode);  // *** MSet写入模式
        inConfig.GetNumber("ttl_second", config.ttlSecond);  // *** MSet TTL秒数
        inConfig.GetNumber("cache_type", config.cacheType);  // *** MSet缓存类型
        inConfig.Get("store_backend", config.storeBackend);

        return config;
    }

    // *** CheckConfig: 校验必要配置参数，缺少local_hostname则报错
    // *** 与MooncakeStore的差异：不需要metadataServer/protocol/replicaNum
    Status CheckConfig(const Config& config)
    {
        if (config.localHostname.empty()) {
            return Status::InvalidParam("local_hostname is required");
        }
        return Status::OK();
    }

    // *** ShowConfig: 打印所有配置参数到日志
    void ShowConfig(const Config& config)
    {
        constexpr const char* ns = "YuanrongStore";
        std::string buildType = UCM_BUILD_TYPE;
        if (buildType.empty()) { buildType = "Release"; }
        UC_INFO("{}-{}({}).", ns, UCM_COMMIT_ID, buildType);
        UC_INFO("{}::LocalHostname = {}", ns, config.localHostname);
        UC_INFO("{}::Port = {}", ns, config.port);
        UC_INFO("{}::DeviceId = {}", ns, config.deviceId);
        UC_INFO("{}::EnableRemoteH2D = {}", ns, config.enableRemoteH2D);
        UC_INFO("{}::DumpQueueDepth = {}", ns, config.dumpQueueDepth);
        UC_INFO("{}::LoadQueueDepth = {}", ns, config.loadQueueDepth);
        UC_INFO("{}::HostBufPoolSize = {}", ns, config.hostBufPoolSize);
        UC_INFO("{}::TimeoutMs = {}", ns, config.timeoutMs);
        UC_INFO("{}::StreamNumber = {}", ns, config.streamNumber);
        UC_INFO("{}::CpuAffinityCores = {}", ns, config.cpuAffinityCores);
        UC_INFO("{}::IoDirect = {}", ns, config.ioDirect);
        UC_INFO("{}::WriteMode = {}", ns, config.writeMode);
        UC_INFO("{}::TtlSecond = {}", ns, config.ttlSecond);
        UC_INFO("{}::CacheType = {}", ns, config.cacheType);
        UC_INFO("{}::StoreBackend = {}", ns, config.storeBackend ? "yes" : "none");
        UC_INFO("{}::TransEnable = {}", ns, transEnable_);
    }
};

}  // namespace UC::YuanrongStore

// *** C接口工厂函数：供动态加载创建/销毁YuanrongStore实例
extern "C" UC::StoreV1* MakeYuanrongStore() { return new UC::YuanrongStore::YuanrongStore(); }
extern "C" void DestroyYuanrongStore(UC::StoreV1* p) { delete p; }