# *** yuanrong_connector.py: YuanrongStore的V0 Python接口兼容版
# *** 基于datasystem Python SDK，走CPU中转（与Mooncake V0版类似）
# *** 这是V0兼容版，V1版通过C++ PipelineStore实现高性能直传
import asyncio
import threading
from concurrent.futures import Future, TimeoutError
from dataclasses import dataclass
from typing import Dict, List

import torch
from safetensors.torch import load as safetensors_load
from safetensors.torch import save as safetensors_save

from ucm.logger import init_logger
from ucm.store.ucmstore import Task, UcmKVStoreBase

TIMEOUT_S_THR: int = 60 * 60
DEFAULT_PORT: int = 2379

logger = init_logger(__name__)


# *** YuanrongStoreConfig: yuanrong-datasystem连接配置
@dataclass
class YuanrongStoreConfig:
    local_hostname: str
    port: int
    device_id: int
    enable_remote_h2d: bool

    @staticmethod
    def load_from_dict(config: Dict = {}) -> "YuanrongStoreConfig":
        """Load the config from dict."""
        return YuanrongStoreConfig(
            local_hostname=config.get("local_hostname"),
            port=config.get("port", DEFAULT_PORT),
            device_id=config.get("device_id", -1),
            enable_remote_h2d=config.get("enable_remote_h2d", True),
        )


# *** YuanrongTask: 任务类，包含task_id标识
@dataclass
class YuanrongTask(Task):
    """A task class for Yuanrong operations with a task identifier."""
    task_id: int = -1


# *** UcmYuanrongStore: 基于datasystem Python SDK的V0兼容版
# *** 通过CPU中转（safetensors序列化）实现lookup/load/dump
class UcmYuanrongStore(UcmKVStoreBase):
    """
    A wrapper class for yuanrong-datasystem that implements the UcmKVStoreBase interface.
    Provides key-value store functionality using yuanrong as the backend (V0 CPU relay version).
    """

    def __init__(self, config: Dict = {}):
        """Initialize the Yuanrong store with configuration."""
        super().__init__(config)
        try:
            from datasystem import HeteroClient  # *** 导入datasystem Python SDK
        except ImportError as e:
            raise ImportError(
                "Please install yuanrong-datasystem Python SDK "
                "to run vLLM with YuanrongConnector."
            ) from e

        try:
            self.client = HeteroClient()  # *** 创建HeteroClient实例

            yuanrong_config = YuanrongStoreConfig.load_from_dict(config)
            logger.info("Yuanrong Configuration loaded from dict successfully.")

            # *** 构造连接选项并初始化
            opts = {
                "host": yuanrong_config.local_hostname,
                "port": yuanrong_config.port,
                "deviceId": yuanrong_config.device_id,
                "enableRemoteH2D": yuanrong_config.enable_remote_h2d,
            }
            self.client.init(opts)  # *** 调用HeteroClient::init建立连接

        except ValueError as e:
            logger.error(f"Configuration loading failed: {e}")
            raise
        except TypeError:
            logger.warning("Lack of configuration, please check the dict params.")
        except Exception as exc:
            logger.error(f"An error occurred while loading the configuration: {exc}")
            raise

        # *** 任务管理变量
        self.task_id: int = 0
        self.tasks: Dict[int, Future] = {}

        # *** 线程和同步变量
        self.loop = asyncio.new_event_loop()
        self.lock = threading.Lock()
        self._shutting_down = threading.Event()

        # *** 启动事件循环线程
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.thread.start()

    def __del__(self):
        """Release resources on garbage collection."""
        try:
            self.shutdown()
        except Exception:
            pass

    def _run_event_loop(self):
        """Run the asyncio event loop in a separate thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    # *** create: 创建KV缓存空间（yuanrong不支持，返回0占位）
    def create(self, block_ids: List[str]) -> List[int]:
        return [0] * len(block_ids)

    # *** lookup: 查询哪些block在yuanrong中存在
    # *** 使用HeteroClient::Exist批量查询，返回布尔向量
    def lookup(self, block_ids: List[str]) -> List[bool]:
        if self._shutting_down.is_set():
            raise RuntimeError("UcmYuanrongStore is shutting down.")

        keys = [f"{block_key}_0" for block_key in block_ids]
        exists_result = self.client.exist(keys)  # *** 调用HeteroClient::exist
        mask = [bool(r) for r in exists_result]
        return mask

    # *** prefetch: 预取操作（yuanrong V0不支持）
    def prefetch(self, block_ids: List[str]) -> None:
        pass

    # *** load: 从yuanrong加载KV缓存到设备
    # *** V0版本走CPU中转：get → safetensors_load → copy_to_device
    def load(
        self, block_ids: List[str], offset: List[int], dst_tensor: List[torch.Tensor]
    ) -> Task:
        if self._shutting_down.is_set():
            raise RuntimeError("UcmYuanrongStore is shutting down.")

        coro = self._load_impl(block_ids, offset, dst_tensor)
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        with self.lock:
            self.task_id += 1
            self.tasks[self.task_id] = future
            return YuanrongTask(task_id=self.task_id)

    async def _load_impl(
        self, block_ids: List[str], offset: List[int], dst_tensor: List[torch.Tensor]
    ) -> int:
        """Internal implementation of loading KV cache from Yuanrong Store (CPU relay)."""
        assert len(block_ids) == len(dst_tensor), \
            "block_ids and dst_tensor have different lengths, please check!"
        for i in range(len(block_ids)):
            try:
                block_hash = f"{block_ids[i]}_{offset[i]}"
                data = self.client.get(block_hash)  # *** 调用HeteroClient::get获取数据
            except TypeError as err:
                logger.error(f"Failed to get value from Yuanrong Store: {err}")
                raise TypeError("Yuanrong Store Get Type Error.") from err

            if data:
                loaded_tensors = safetensors_load(data)  # *** safetensors反序列化
                tensor_cpu = loaded_tensors["tensor"]
                assert dst_tensor[i].shape == tensor_cpu.shape
                assert dst_tensor[i].dtype == tensor_cpu.dtype
                dst_tensor[i].copy_(tensor_cpu)  # *** CPU→Device拷贝
            else:
                return 1
        return 0

    # *** dump: 将设备KV缓存卸载到yuanrong
    # *** V0版本走CPU中转：copy_to_cpu → safetensors_save → put
    def dump(
        self, block_ids: List[str], offset: List[int], src_tensor: List[torch.Tensor]
    ) -> Task:
        if self._shutting_down.is_set():
            raise RuntimeError("UcmYuanrongStore is shutting down.")

        coro = self._dump_impl(block_ids, offset, src_tensor)
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        with self.lock:
            self.task_id += 1
            self.tasks[self.task_id] = future
            return YuanrongTask(task_id=self.task_id)

    async def _dump_impl(
        self, block_ids: List[str], offset: List[int], src_tensor: List[torch.Tensor]
    ) -> int:
        """Internal implementation of dumping KV cache to Yuanrong Store (CPU relay)."""
        assert len(block_ids) == len(src_tensor), \
            "block_ids and src_tensor have different lengths, please check!"
        for i in range(len(block_ids)):
            value_bytes = safetensors_save({"tensor": src_tensor[i]})  # *** safetensors序列化
            try:
                block_hash = f"{block_ids[i]}_{offset[i]}"
                ret = self.client.put(block_hash, value_bytes)  # *** 调用HeteroClient::put写入数据
                if ret != 0:
                    return ret
            except TypeError as err:
                logger.error(f"Failed to put value into Yuanrong Store: {err}")
                raise TypeError("Yuanrong Store Put Type Error.") from err
        return 0

    def fetch_data(
        self, block_ids: List[str], offset: List[int], dst_addr: List[int], size: List[int]
    ) -> Task:
        raise NotImplementedError("Method(fetch_data) not yet implemented in this version")

    def dump_data(
        self, block_ids: List[str], offset: List[int], src_addr: List[int], size: List[int]
    ) -> Task:
        raise NotImplementedError("Method(dump_data) not yet implemented in this version")

    # *** wait: 等待异步任务完成
    def wait(self, task: Task) -> int:
        with self.lock:
            future = self.tasks.pop(task.task_id, None)

        if future is None:
            logger.error(f"Invalid task ID: {task.task_id}")
            return 1

        try:
            ret = future.result(TIMEOUT_S_THR)
            return ret
        except TimeoutError:
            future.cancel()
            logger.error(f"Task {task.task_id} timed out after {TIMEOUT_S_THR}s")
            return 1
        except asyncio.CancelledError:
            logger.error(f"Task {task.task_id} was cancelled")
            return 1
        except Exception as e:
            logger.error(f"Task {task.task_id} failed: {str(e)}")
            return 1

    # *** commit: 提交KV缓存（yuanrong V0不支持）
    def commit(self, block_ids: List[str], is_success: bool = True) -> None:
        pass

    # *** shutdown: 安全关闭所有组件
    def shutdown(self):
        """Safely shutdown all components of the store."""
        if self._shutting_down.is_set():
            return

        self._shutting_down.set()

        with self.lock:
            tasks_to_cancel = list(self.tasks.values())
            self.tasks.clear()

        for future in tasks_to_cancel:
            if not future.done():
                future.cancel()

        self.loop.call_soon_threadsafe(self.loop.stop)

        if self.thread.is_alive():
            self.thread.join(TIMEOUT_S_THR)
            if not self.loop.is_closed():
                self.loop.close()

        # *** 关闭HeteroClient连接
        if hasattr(self, 'client'):
            self.client.shutdown()  # *** 调用HeteroClient::shutdown关闭连接

    # *** check: 检查任务是否完成（V0不支持）
    def check(self, task: Task) -> int:
        pass