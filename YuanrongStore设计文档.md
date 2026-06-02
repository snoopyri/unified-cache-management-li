# YuanrongStore 设计文档

> 基于 yuanrong-datasystem HeteroClient 的 UCM KV 缓存存储后端实现
> 
> 代码仓：`D:\code\unified-cache-management-li` 分支 `yuanrong_store_dev`
> 
> 参考代码仓：
> - `D:\code\unified-cache-management-fh` — MooncakeStore 实现参考（UCM 原版 fork）
> - `D:\code\yuanrong-datasystem` — yuanrong-datasystem SDK 源码（HeteroClient API 定义）
> 
> Commit：`291fca0` — 29 files changed, 2433 insertions(+)

---

## 1. 项目背景

UCM（Unified Cache Management）是华为开源的 LLM KV Cache 多级存储系统，用于在推理场景中将 KV Cache 从 GPU/NPU 内存卸载到 Host DRAM 和 SSD，并在需要时重新加载回设备内存。

当前 UCM 通过 **MooncakeStore**（PR #972）对接 Mooncake TransferEngine，实现跨节点 KV Cache 的 RDMA/DMA 直传。本项目的目标是将 Mooncake 替换为 **yuanrong-datasystem**（华为开源的分布式缓存系统），实现同等功能但使用 yuanrong 的 HeteroClient API。

### 1.1 为什么要替换 Mooncake

| 维度 | Mooncake TransferEngine | yuanrong-datasystem |
|------|------------------------|---------------------|
| 数据传输 | RealClient C++ API，需手动 register_buffer | HeteroClient C++ API，DeviceBlobList 自动管理 |
| 设备支持 | 仅 NPU（Ascend ACL） | NPU + GPU 双平台，设备无关抽象 |
| 卡间通信 | 无直接支持 | P2P HCCL/NCCL 卡间直传 |
| 查询能力 | BatchIsExist 返回 optional\<bool\>（3态） | Exist 返回 bool（2态），GetMetaInfo 补充 |
| 副本控制 | ReplicateConfig（显式 replica_num） | 集群内部管理，无显式副本数 |
| 异步模型 | 同步 batch API + 手动队列 | AsyncMGetH2D/AsyncMSetD2H，自带异步 Future |

### 1.2 设计目标

- **对等替换**：YuanrongStore 实现与 MooncakeStore 完全相同的 StoreV1 接口（Lookup/LookupOnPrefix/Load/Dump/Check/Wait/RegisterMemory）
- **Pipeline 兼容**：支持 `Yuanrong|Posix` 双层管线（分布式缓存 + 磁盘持久化）
- **DMA 直传**：通过 HeteroClient::MGetH2D/MSetD2H 实现 NPU↔远端直传，不走 CPU 中转
- **双写策略**：Dump 同时写入 yuanrong 远端和 PosixStore 磁盘

---

## 2. 架构总览

### 2.1 调用链路

```
vLLM KVConnectorBase_V1
    → UCMDirectConnector (integration/vllm/ucm_connector.py)
        → UcmPipelineStore (pipeline/connector.py, Python→C++ 桥接)
            → PipelineStore (C++ pybind11, 管理多层 Store 的 Stack)
                → YuanrongStore (C++ StoreV1 实现)
                    → datasystem::HeteroClient (yuanrong SDK 传输引擎)
                    → storeBackend (PosixStore C++ StoreV1, 穿透后端)
```

### 2.2 Pipeline 组装

```python
# Yuanrong|Posix 双层管线
pipeline.Stack("Posix", libposixstore.so, posix_config)   # 底层：持久化
pipeline.Stack("Yuanrong", libyuanrongstore.so, config)    # 上层：分布式缓存+DMA
```

### 2.3 组件关系图

```
YuanrongStore
├── TransManager (传输管理器)
│   ├── HeteroClient (yuanrong SDK 客户端)
│   ├── LoadQueue (双线程加载队列)
│   │   ├── dispatcher 线程 → TryYuanrongLoad(MGetH2D) + SubmitMissShards
│   │   └── transfer 线程 → backend Wait + H2D ScatterAsync
│   ├── DumpQueue (双线程卸载队列)
│   │   ├── dispatcher 线程 → Exist + MSetD2H + D2H GatherAsync + backend Dump
│   │   └── dumper 线程 → backend Wait
│   ├── HostBufferPool (池化主机内存)
│   ├── failureSet (失败任务追踪)
│   └── CopyStream (多流并行 D2H/H2D)
├── rpcClient_ (调度模式 HeteroClient，仅 Lookup)
└── storeBackend (PosixStore，穿透后端)
```

---

## 3. 双模式设计

YuanrongStore 根据部署角色分为两种模式：

| 模式 | 触发条件 | 角色 | 能力 | HeteroClient |
|------|---------|------|------|-------------|
| **传输模式** | `deviceId >= 0` | vLLM Worker | Lookup + Load + Dump + RegisterMemory | `transMgr_` 内的 HeteroClient（全功能） |
| **调度模式** | `deviceId < 0` | vLLM Scheduler | 仅 Lookup/LookupOnPrefix | `rpcClient_`（仅查询，deviceId=-1） |

### 3.1 Setup 流程

```
Setup(Dictionary config):
    config = ParseConfig(inConfig)
    CheckConfig(config)  // 校验 local_hostname 非空
    
    if deviceId >= 0:  // 传输模式
        transMgr_.Setup(config)
            → SetupHeteroClient(config)  // 创建 HeteroClient + Init
            → bufPool_.Setup(...)         // 初始化主机缓冲池
            → loadQ_.Setup(...)            // 初始化 LoadQueue
            → dumpQ_.Setup(...)            // 初始化 DumpQueue
    else:  // 调度模式
        SetupRpcClient(config)  // 创建 rpcClient_，deviceId=-1
```

---

## 4. API 映射

### 4.1 StoreV1 接口 → yuanrong HeteroClient API

| UCM StoreV1 接口 | yuanrong HeteroClient API | 说明 |
|------------------|--------------------------|------|
| `LookupOnPrefix(blocks, num)` | `Exist(keys, exists)` + `GetMetaInfo(keys, isDevKey, metaInfos, failKeys)` | 先 Exist 批量查，失败时 fallback 到 GetMetaInfo |
| `Lookup(blocks, num)` | 同上（调用 LookupOnPrefix 后转布尔向量） | — |
| `Load(TaskDesc)` | `MGetH2D(keys, devBlobList, failKeys, subTimeoutMs)` | 批量 DMA 直传，miss shard 交给 backend |
| `Dump(TaskDesc)` | `Exist(keys, exists)` + `MSetD2H(keys, devBlobList, setParam)` | 先查已存在（跳过），再 MSetD2H 写入 |
| `Check(TaskHandle)` | `TaskWrapper::Check` 内部机制 | Latch 完成状态检查 |
| `Wait(TaskHandle)` | `TaskWrapper::Wait` 内部机制 | Latch 阻塞等待 |
| `RegisterMemory(addr, size)` | **空实现** | yuanrong 通过 DeviceBlobList.deviceIdx 自动管理 |
| `Prefetch(blocks, num)` | **未实现** | — |

### 4.2 与 MooncakeStore 的 API 对比

| MooncakeStore | YuanrongStore | 差异说明 |
|---------------|---------------|---------|
| `mooncake::Client::BatchIsExist` → `vector<optional<bool>>` | `HeteroClient::Exist` → `vector<bool>` + `GetMetaInfo` 补充 | Exist 无 -1 状态，需 GetMetaInfo fallback |
| `mooncake::RealClient::batch_get_into_multi_buffers` | `HeteroClient::MGetH2D` | MGetH2D 使用 DeviceBlobList（含 deviceIdx） |
| `mooncake::RealClient::batch_put_from_multi_buffers` + `ReplicateConfig{replicaNum}` | `HeteroClient::MSetD2H` + `SetParam{writeMode, ttlSecond, cacheType}` | 无显式副本数，参数语义不同 |
| `mooncake::RealClient::register_buffer/unregister_buffer` | 空实现 | yuanrong 自动管理设备内存映射 |
| `vector<vector<void*>>` + `vector<vector<size_t>>` | `DeviceBlobList{blobs, deviceIdx, srcOffset}` | 数据封装不同 |
| `mooncake::RealClient::batchIsExist` (Dump 时跳已存在) | `HeteroClient::Exist` | 同 Lookup 的差异 |

---

## 5. 核心流程详解

### 5.1 Lookup / LookupOnPrefix

**目的**：查询哪些 KV Cache block 在分布式缓存中存在，支持前缀连续查找。

```
LookupOnPrefix(blocks[0..num-1]):
    1. 构造 keys = [BlockIdToHex(blocks[i]) + "_0"]
    2. RpcBatchIsExist(keys):
        a. HeteroClient::Exist(keys, existsResult)
        b. 如果 Exist 失败 → fallback 到 GetMetaInfo:
           HeteroClient::GetMetaInfo(keys, false, metaInfos, failKeys)
           failKeys 中的 key → 0(不存在)
           其他 key: blobSizeList 非空 → 1(存在)
           blobSizeList 空 → 0(不存在)
           GetMetaInfo 也失败 → -1(查询失败)
    3. 找到 firstMiss（第一个不存在或查询失败的 index）
    4. 全部命中(firstMiss==-1) → 返回 num-1
    5. 有 miss + 有 storeBackend:
       backendRes = storeBackend->LookupOnPrefix(blocks+firstMiss, num-firstMiss)
       返回 firstMiss + backendHit
    6. 有 miss + 无 backend → 返回 firstMiss-1
```

**穿透式设计**：先查 yuanrong 分布式缓存（速度快），miss 的部分穿透 PosixStore 查磁盘（慢但持久），组合返回最长命中前缀。

### 5.2 Load（从远端加载 KV Cache 到 NPU）

**双线程架构**：dispatcher 调度 + transfer 搬运

```
┌─────────────────────────────────────────────────┐
│               dispatcher 线程                    │
│                                                  │
│  waiting_ 队列 → DispatchOneTask:               │
│                                                  │
│  1. TryYuanrongLoad(task):                       │
│     ┌──────────────────────────────────┐         │
│     │ keys = [shard.key for each shard] │         │
│     │ devBlobList.blobs = 所有 tensor 的│         │
│     │   Blob{shard.addrs[j], sizes[j]} │         │
│     │ devBlobList.deviceIdx = deviceId_ │         │
│     │ devBlobList.srcOffset = 0         │         │
│     │                                    │         │
│     │ HeteroClient::MGetH2D(             │         │
│     │   keys, devBlobList,               │         │
│     │   failKeys, subTimeoutMs)          │         │
│     │                                    │         │
│     │ 全部 hit → waiter->Done() 直接完成 │         │
│     │ 有 miss → 继续 SubmitMissShards   │         │
│     └──────────────────────────────────┘         │
│                                                  │
│  2. SubmitMissShards(task, waiter, failKeys):    │
│     对每个 miss shard:                            │
│     buf = bufPool_->AcquireWithTimeout(3s)       │
│     backendTask = {shard.owner, shard.index,     │
│                    buf.get()}  // 目标为主机缓冲   │
│     backend_->Load(backendTask)                   │
│     running_.Push({taskHandle, backendHandle,    │
│                    shard, hostBuf, waiter})       │
│                                                  │
│     最后一个 miss shard 持有 waiter                │
└─────────────────────────────────────────────────┘
                         │ running_ 队列
                         ▼
┌─────────────────────────────────────────────────┐
│               transfer 线程                      │
│                                                  │
│  running_ 队列 → TransferOneTask:               │
│                                                  │
│  1. backend_->Wait(backendTaskHandle)            │
│     // 等 PosixStore 从磁盘读到 host buffer       │
│                                                  │
│  2. HostToDeviceScatterAsync(stream, hostBuf,    │
│                              device_addrs):      │
│     对每个 tensor:                                │
│     stream->HostToDeviceAsync(                   │
│       host+offset, device[i], tensorSizes[i])    │
│     // NPU H2D DMA 异步拷贝                       │
│                                                  │
│  3. stream.Synchronize()                         │
│     // 等所有 H2D DMA 完成                         │
│                                                  │
│  4. holder_.clear()  // 释放暂存的 shard          │
│     waiter->Done()                               │
└─────────────────────────────────────────────────┘
```

**数据流**：
- **yuanrong hit**：远端 → MGetH2D DMA → NPU（直传，不经过 CPU）
- **yuanrong miss**：磁盘 → PosixStore → host buffer → H2D DMA → NPU

### 5.3 Dump（从 NPU 卸载 KV Cache 到远端）

**双线程架构**：dispatcher 调度 + dumper 等待后端

```
┌──────────────────────────────────────────────────────┐
│               dispatcher 线程                         │
│                                                       │
│  waiting_ 队列 → DispatchOneTask:                    │
│                                                       │
│  DumpOneTask(stream, task):                           │
│                                                       │
│  1. HeteroClient::Exist(keys, existsResult)           │
│     // 找出 yuanrong 中已存在的 key，跳过              │
│     missingKeys = keys 中 !existsResult[i] 的部分     │
│                                                       │
│  2. 如有 backend (双写策略):                           │
│     a. stream.WaitEvent(prerequisiteHandle)            │
│        // 异步等待 NPU 计算事件                        │
│     b. PrepareBackendDump:                            │
│        对每个 shard:                                   │
│        buf = bufPool_->AcquireWithTimeout(3s)         │
│        DeviceToHostGatherAsync(stream, addrs, buf):   │
│          stream->DeviceToHostAsync(                   │
│            device[i], host+offset, tensorSizes[i])    │
│          // NPU D2H DMA 异步拷贝                       │
│        backendTaskDesc += {shard.owner, shard.index,  │
│                            buf.get()}                  │
│                                                       │
│  3. WaitPrerequisite(task):                           │
│     aclrtSynchronizeEvent(prerequisiteHandle)          │
│     // 同步等待 NPU 计算完成（确保数据已写入内存）       │
│                                                       │
│  4. PutToYuanrong(task, missingKeys, buffers, sizes): │
│     ┌────────────────────────────────────────────┐    │
│     │ setParam.writeMode = writeMode_             │    │
│     │ setParam.ttlSecond = ttlSecond_             │    │
│     │ setParam.cacheType = cacheType_             │    │
│     │                                              │    │
│     │ devBlobList.blobs = 所有 missing tensor 的   │    │
│     │   Blob{addrs[j], sizes[j]}                  │    │
│     │ devBlobList.deviceIdx = deviceId_            │    │
│     │ devBlobList.srcOffset = 0                    │    │
│     │                                              │    │
│     │ HeteroClient::MSetD2H(                       │    │
│     │   missingKeys, devBlobList, setParam)        │    │
│     │                                              │    │
│     │ // NPU → 远端 DMA 直传                        │    │
│     └────────────────────────────────────────────┘    │
│                                                       │
│  5. SubmitBackendDump:                                │
│     stream.Synchronize()  // 等 D2H DMA 完成          │
│     backend_->Dump(backendTaskDesc)                    │
│     // host buffer → PosixStore 写磁盘                │
│     dumping_.Push({taskHandle, backendHandle, hostBufs})│
└──────────────────────────────────────────────────────┘
                         │ dumping_ 队列
                         ▼
┌──────────────────────────────────────────────────────┐
│               dumper 线程                             │
│                                                       │
│  dumping_ 队列 → 等待后端卸载完成:                     │
│                                                       │
│  backend_->Wait(backendTaskHandle)                    │
│  // 等 PosixStore 写磁盘完成                           │
│  // hostBufs 在 DumpCtx 析构时自动释放                 │
└──────────────────────────────────────────────────────┘
```

**数据流**（双写策略）：
- **写 yuanrong**：NPU → MSetD2H DMA → 远端（直传）
- **写 PosixStore**：NPU → D2H DMA → host buffer → 写磁盘

### 5.4 RegisterMemory

```
RegisterMemory(base_addr, total_size):
    // yuanrong 通过 DeviceBlobList.deviceIdx 自动管理设备内存映射
    // 无需像 Mooncake 那样手动调用 register_buffer
    return Status::OK()
```

---

## 6. 关键组件

### 6.1 TransManager

| 属性 | 说明 |
|------|------|
| `heteroClient_` | 共享的 HeteroClient 实例，用于 Load/Dump 的数据传输 |
| `failureSet_` | HashSet\<TaskHandle\>，记录失败任务 ID，后续跳过 |
| `bufPool_` | HostBufferPool，池化主机内存 |
| `loadQ_` | LoadQueue，双线程加载 |
| `dumpQ_` | DumpQueue，双线程卸载 |

职责：创建 HeteroClient → 初始化 bufPool → 初始化 LoadQueue/DumpQueue → 根据任务类型分发到对应队列。

### 6.2 HostBufferPool

| 参数 | 说明 |
|------|------|
| `pool_` | 预分配的连续主机内存（Pinned/DirectIO HugePages） |
| `unitSize_` | 单个缓冲区大小 = sum(tensorSizeList) |
| `count_` | 缓冲区数量 = hostBufPoolSize 配置 |
| `index_` | IndexPool，管理缓冲区的分配/释放索引 |
| `cv_` | 条件变量，AcquireWithTimeout 时等待释放 |

内存分配方式：
- `ioDirect=false`：`aclrtMallocHost`（Pinned 内存）
- `ioDirect=true`：HugePages（DirectIO 大页内存）

释放机制：shared_ptr 自定义 deleter → `ReleaseByIndex` → 归还索引 → `cv_.notify_one()`

### 6.3 CopyStream

| 参数 | 说明 |
|------|------|
| `streams_` | vector\<shared_ptr\<Trans::Stream\>\>，Ascend stream 池 |
| `streamIndex_` | 轮询分配索引 |
| `streamNumber_` | stream 数量（配置项，默认 4） |

核心方法：
- `NextStream()`：轮询分配下一个 stream
- `WaitEvent(event)`：所有 stream 等待 aclrtEvent
- `Synchronize()`：同步所有 stream

### 6.4 TransTask / TransShard

```
TransShard:
    key: string        // "hex(BlockId)_shardIndex"
    owner: BlockId     // 16字节唯一标识（用于 backend 穿透）
    index: size_t      // shard 索引
    addrs: vector<void*>  // 设备端 tensor 地址列表
    sizes: vector<size_t> // tensor 大小列表

TransTask:
    id: TaskHandle     // 全局自增唯一 ID
    type: TaskType     // LOAD 或 DUMP
    brief: string      // 任务描述
    shards: vector<TransShard>
    prerequisiteHandle: uintptr_t  // aclrtEvent（Dump 时等 NPU 计算完成）
```

### 6.5 Config（global_config.h）

| 类别 | 参数 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| **连接** | localHostname | string | "" | yuanrong 服务地址 |
| | port | uint32_t | 2379 | yuanrong 端口 |
| | deviceId | int32_t | -1 | NPU 设备 ID（-1=调度模式） |
| | enableRemoteH2D | bool | true | 是否启用远程 H2D |
| **Tensor** | tensorSizeList | vector\<uint64_t\ | {} | 每个 tensor 的大小列表 |
| **队列** | loadQueueDepth | uint32_t | 524288 | 加载队列深度 |
| | dumpQueueDepth | uint32_t | 8192 | 卸载队列深度 |
| | hostBufPoolSize | uint32_t | 1024 | 主机缓冲池数量 |
| | timeoutMs | size_t | 0 | 任务超时（毫秒） |
| **传输** | streamNumber | size_t | 4 | Ascend stream 数量 |
| | cpuAffinityCores | vector\<ssize_t\ | {} | CPU 亲和核心 |
| | ioDirect | bool | false | DirectIO 大页内存 |
| **MSet** | writeMode | int32_t | 0 | 写入模式 |
| | ttlSecond | int32_t | 0 | TTL 秒数 |
| | cacheType | int32_t | 0 | 缓存类型 |
| **后端** | storeBackend | StoreV1* | nullptr | 穿透后端（PosixStore） |

---

## 7. 文件清单

### 7.1 新增文件

```
ucm/store/yuanrongstore/
├── cc/
│   ├── yuanrong_store.cc      # StoreV1 主实现（Setup/Lookup/Load/Dump/RegisterMemory）
│   ├── global_config.h         # Config 结构体定义
│   ├── trans_manager.h/cc      # 传输管理器（HeteroClient + LoadQueue + DumpQueue）
│   ├── trans_task.h             # TransTask/TransShard 数据结构
│   ├── load_queue.h/cc         # 双线程加载队列
│   ├── dump_queue.h/cc         # 双线程卸载队列
│   ├── host_buffer_pool.h      # 池化主机内存管理
│   └── copy_stream.h           # 多流并行 D2H/H2D
├── CMakeLists.txt               # 条件编译（依赖 ACL + datasystem 库）
├── yuanrong_connector.py        # V0 Python 兼容版（CPU 中转）
└── __init__.py                  # 模块初始化

examples/
└── ucm_yuanrong_config.yaml     # 配置示例

ucm/store/test/e2e/
├── yuanrong_store_test.py       # 单层 YuanrongStore 测试
└── yuanrong_on_posix_test.py    # Yuanrong|Posix 双层管线测试
```

### 7.2 修改的现有文件

| 文件 | 修改内容 |
|------|---------|
| `ucm/store/ucmstore_v1.h` | 添加 `RegisterMemory(void*, size_t)` 虚接口 |
| `ucm/store/ucmstore_v1.py` | 添加 `register_memory(base_addr, total_size)` 方法 |
| `ucm/store/pipeline/cpy/pipeline_store.py.cc` | 添加 `RegisterMemory` pybind11 绑定 |
| `ucm/store/pipeline/connector.py` | 添加 `Yuanrong` / `Yuanrong|Posix` pipeline builder + `register_memory` |
| `ucm/store/factory_v1.py` | 注册 YuanrongStore connector |
| `ucm/store/CMakeLists.txt` | 添加 yuanrongstore 子目录 |
| `ucm/store/cache/cc/cache_store.cc` | `RegisterMemory` 空实现 |
| `ucm/store/posix/cc/posix_store.cc` | `RegisterMemory` 空实现 |
| `ucm/store/empty/cc/empty_store.cc` | `RegisterMemory` 空实现 |
| `ucm/store/fake/cc/fake_store.cc` | `RegisterMemory` 空实现 |
| `ucm/store/ds3fs/cc/ds3fs_store.cc` | `RegisterMemory` 空实现 |
| `ucm/store/ds3fs/cc/ds3fs_store.h` | `RegisterMemory` 空实现声明 |

---

## 8. 构建 & 部署

### 8.1 CMakeLists.txt 条件编译

```cmake
find_path(ASCEND_ACL_INCLUDE_DIR NAMES acl/acl.h ...)
find_library(ASCEND_ACL_LIBRARY NAMES ascendcl ...)
find_path(DATASYSTEM_INCLUDE_DIR NAMES datasystem/hetero_client.h ...)
find_library(DATASYSTEM_LIBRARY NAMES datasystem ...)

if(所有依赖找到)
    add_library(yuanrongstore SHARED ./cc/*.cc)
    target_compile_features(yuanrongstore PUBLIC cxx_std_20)
    target_include_directories(yuanrongstore PUBLIC cc/ ${DATASYSTEM_INCLUDE_DIR})
    target_include_directories(yuanrongstore SYSTEM PUBLIC ${ASCEND_ACL_INCLUDE_DIR})
    target_link_libraries(yuanrongstore PUBLIC storeintf infra_logger fmt trans
                          ${DATASYSTEM_LIBRARY} ${ASCEND_ACL_LIBRARY})
else()
    message("yuanrongstore: Skipping build - dependencies not found")
endif()
```

### 8.2 部署前置条件

1. **Ascend ACL**：安装在 `/usr/local/Ascend/ascend-toolkit/latest/`
2. **yuanrong-datasystem**：编译安装到 `/usr/local/`（头文件 `datasystem/hetero_client.h`，库文件 `libdatasystem.so`）
3. **UCM 内部依赖**：storeintf, infra_logger, fmt, trans（已有）

### 8.3 配置示例（ucm_yuanrong_config.yaml）

```yaml
store_type: "Yuanrong|Posix"
local_hostname: "192.168.1.100"
port: 2379
device_id: 0
enable_remote_h2d: true
tensor_size_list: [1024, 1024, ...]  # 每个 tensor 的大小
load_queue_depth: 524288
dump_queue_depth: 8192
host_buf_pool_size: 1024
stream_number: 4
timeout_ms: 60000
write_mode: 0
ttl_second: 0
cache_type: 0
```

---

## 9. 已知限制 & 后续优化

### 9.1 当前限制

| 限制 | 说明 |
|------|------|
| **Exist 无法返回查询失败状态** | yuanrong Exist 返回 `vector<bool>`，没有 -1 状态。用 GetMetaInfo fallback，但增加了额外 RPC 调用 |
| **无显式副本控制** | MSetD2H 的 SetParam 没有 replicaNum，副本由 yuanrong 集群内部管理 |
| **CMake 依赖硬编码路径** | find_path/find_library 使用固定路径（`/usr/local/`），本地开发时需要手动安装或修改路径 |
| **V0 Python 版走 CPU 中转** | yuanrong_connector.py 用 safetensors 序列化 + `copy_()` 中转，性能差 |
| **Prefetch 未实现** | yuanrong 没有对应的 prefetch API |
| **无 DevMSet/DevMGet 支持** | yuanrong 的卡间直传能力（DevMSet/DevMGet/P2P）暂未利用 |

### 9.2 后续优化方向

1. **CMake 变量支持**：添加 `YUANRONG_DATASYSTEM_DIR` Cache 变量，支持外部传入路径
2. **异步接口优化**：用 `AsyncMGetH2D/AsyncMSetD2H` 替代同步 API，简化 LoadQueue/DumpQueue 双线程模型
3. **卡间直传**：利用 yuanrong 的 `DevMSet/DevMGet` 实现同节点多卡 P2P 传输
4. **GetMetaInfo 预查**：在 LookupOnPrefix 中先用 GetMetaInfo 获取 blobSizeList，减少 Exist → GetMetaInfo 的两次 RPC
5. **ioDirect 性能调优**：大页内存模式下减少 CPU 页表开销

---

## 10. 与 MooncakeStore 的快速对照

| 概念 | MooncakeStore | YuanrongStore |
|------|---------------|---------------|
| 传输客户端 | `mooncake::RealClient` | `datasystem::HeteroClient` |
| 查询客户端 | `mooncake::Client` (RPC) | `datasystem::HeteroClient` (deviceId=-1) |
| 批量读取 | `batch_get_into_multi_buffers(keys, buffers, sizes, async)` | `MGetH2D(keys, devBlobList, failKeys, subTimeoutMs)` |
| 批量写入 | `batch_put_from_multi_buffers(keys, buffers, sizes, ReplicateConfig)` | `MSetD2H(keys, devBlobList, SetParam)` |
| 批量查询 | `BatchIsExist(keys)` → `vector<optional<bool>>` | `Exist(keys, exists)` → `vector<bool>` + `GetMetaInfo` fallback |
| 内存注册 | `register_buffer(addr, size)` + `unregister_buffer(addr)` | 空实现（DeviceBlobList.deviceIdx 自动管理） |
| 数据封装 | `vector<vector<void*>>` + `vector<vector<size_t>>` | `DeviceBlobList{blobs, deviceIdx, srcOffset}` |
| 写入参数 | `ReplicateConfig{replicaNum, withSoftPin}` | `SetParam{writeMode, ttlSecond, cacheType}` |
| 队列模型 | 同步 API + 手动 Dispatch+Transfer 双线程 | 同步 MGetH2D/MSetD2H + 手动双线程（后续可改 AsyncMGet/AsyncMSet） |
| Key 格式 | `hex(BlockId)_offset` | `hex(BlockId)_shardIndex` |
| Pipeline | `Mooncake|Posix` | `Yuanrong|Posix` |
| 工厂函数 | `MakeMooncakeStore` / `DestroyMooncakeStore` | `MakeYuanrongStore` / `DestroyYuanrongStore` |