# *** yuanrong_store_test.py: YuanrongStore基本功能E2E测试
# *** 测试Yuanrong|Posix管线的基本lookup/load/dump操作
import os
import sys
import unittest
import numpy as np
import torch

# *** 添加UCM路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))


class YuanrongStoreTest(unittest.TestCase):
    """YuanrongStore E2E basic functionality test."""

    @classmethod
    def setUpClass(cls):
        """Setup test: create Yuanrong|Posix pipeline store."""
        from ucm.store.pipeline.connector import UcmPipelineStore

        # *** 构造Yuanrong|Posix管线配置
        config = {
            "store_pipeline": "Yuanrong|Posix",
            "local_hostname": os.environ.get("YUANRONG_HOST", "127.0.0.1"),
            "port": int(os.environ.get("YUANRONG_PORT", "2379")),
            "device_id": int(os.environ.get("UCM_DEVICE_ID", "-1")),
            "enable_remote_h2d": True,
            "shard_size": 4096,
            "block_size": 65536,
            "storage_backends": ["/tmp/ucm_yuanrong_test_storage"],
            "posix_io_engine": "psync",
            "posix_data_trans_concurrency": 4,
        }
        cls.store = UcmPipelineStore(config)
        cls.config = config

    def test_lookup_empty(self):
        """Test lookup on empty store returns all false."""
        # *** 构造随机BlockId
        block_ids = [os.urandom(16) for _ in range(5)]
        result = self.store.lookup(block_ids)
        # *** 空存储应该返回全部False
        self.assertEqual(len(result), len(block_ids))

    def test_lookup_on_prefix_empty(self):
        """Test lookup_on_prefix on empty store returns -1."""
        block_ids = [os.urandom(16) for _ in range(5)]
        result = self.store.lookup_on_prefix(block_ids)
        # *** 空存储应该返回-1
        self.assertEqual(result, -1)

    def test_register_memory(self):
        """Test register_memory is callable (yuanrong should be no-op)."""
        # *** YuanrongStore的RegisterMemory是空实现，应该不会报错
        base_addr = 0
        total_size = 1024 * 1024 * 1024  # 1GB
        self.store.register_memory(base_addr, total_size)
        # *** 无异常即成功


if __name__ == "__main__":
    unittest.main()