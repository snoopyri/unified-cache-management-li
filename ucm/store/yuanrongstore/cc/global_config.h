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
// *** global_config.h: YuanrongStore存储后端的配置结构体定义
// *** Config包含所有运行参数：连接、内存、队列、传输、后端存储等
// *** 与MooncakeStore的主要差异：无replicaNum/withSoftPin（由集群管理），增加enableRemoteH2D等
#ifndef UNIFIEDCACHE_YUANRONG_STORE_CC_GLOBAL_CONFIG_H
#define UNIFIEDCACHE_YUANRONG_STORE_CC_GLOBAL_CONFIG_H

#include <cstdint>
#include <string>
#include <vector>

namespace UC {
class StoreV1;
}

namespace UC::YuanrongStore {

// *** Config: YuanrongStore存储后端的完整配置结构体
struct Config {
    // *** 连接配置（HeteroClient ConnectOptions）
    std::string localHostname{};  // *** 本节点主机名/IP（连接yuanrong-datasystem的host参数）
    uint32_t port{2379};  // *** yuanrong-datasystem服务端口（默认2379）
    int32_t deviceId{-1};  // *** NPU设备ID，-1表示调度模式（无传输能力）
    bool enableRemoteH2D{true};  // *** 是否启用远程H2D（Host到Device直传），对应ConnectOptions.enableRemoteH2D

    // *** Tensor/Shard配置
    std::vector<uint64_t> tensorSizeList{};  // *** 每个tensor的大小列表，用于scatter/gather传输

    // *** 队列配置
    uint32_t loadQueueDepth{524288};  // *** 加载队列深度
    uint32_t dumpQueueDepth{8192};  // *** 卸载队列深度
    uint32_t hostBufPoolSize{1024};  // *** 主机缓冲池大小（缓冲区数量）
    size_t timeoutMs{0};  // *** 任务超时时间（毫秒）

    // *** 传输配置
    size_t streamNumber{4};  // *** Ascend stream数量
    std::vector<ssize_t> cpuAffinityCores{};  // *** CPU亲和核心列表

    bool ioDirect{false};  // *** 是否使用DirectIO（大页内存）

    // *** MSet参数
    int32_t writeMode{0};  // *** 写入模式（0=默认）
    int32_t ttlSecond{0};  // *** TTL秒数（0=永不过期）
    int32_t cacheType{0};  // *** 缓存类型

    // *** 后端存储
    StoreV1* storeBackend{nullptr};  // *** 后端存储指针（如Posix），用于miss shard的持久化
};

}  // namespace UC::YuanrongStore

#endif