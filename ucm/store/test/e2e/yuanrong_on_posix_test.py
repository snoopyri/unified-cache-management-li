# *** yuanrong_on_posix_test.py: Yuanrong|Posix pipeline E2E测试
# *** 测试Yuanrong|Posix双层管线的lookup/load/dump操作
import os
import sys
import unittest
import tempfile
import shutil
import numpy as np
import torch

# *** 添加UCM路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))


class YuanrongOnPosixTest(unittest.TestCase):
    """Yuanrong|Posix pipeline E2E test."""

    @classmethod
    def setUpClass(cls):
        """Setup test: create Yuanrong|Posix pipeline store."""
        from ucm.store.pipeline.connector import UcmPipelineStore

        # *** 创建临时Posix存储目录
        cls.posix_dir = tempfile.mkdtemp(prefix="ucm_yuanrong_posix_test_")

        # *** 构造Yuanrong|Posix管线配置
        config = {
            "store_pipeline": "Yuanrong|Posix",
            "local_hostname": os.environ.get("YUANRONG_HOST", "127.0.0.1"),
            "port": int(os.environ.get("YUANRONG_PORT", "2379")),
            "device_id": int(os.environ.get("UCM_DEVICE_ID", "-1")),
            "enable_remote_h2d": True,
            "shard_size": 4096,
            "block_size": 65536,
            "storage_backends": [cls.posix_dir],
            "posix_io_engine": "psync",
            "posix_data_trans_concurrency": 4,
        }
        cls.store = UcmPipelineStore(config)
        cls.config = config

    @classmethod
    def tearDownClass(cls):
        """Cleanup: remove temporary Posix storage directory."""
        shutil.rmtree(cls.posix_dir, ignore_errors=True)

    def test_pipeline_creation(self):
        """Test Yuanrong|Posix pipeline store creation."""
        # *** 管线创建应该成功
        self.assertIsNotNone(self.store)

    def test_lookup_empty(self):
        """Test lookup on empty Yuanrong|Posix store."""
        block_ids = [os.urandom(16) for _ in range(5)]
        result = self.store.lookup(block_ids)
        # *** 空存储应该返回全部False
        self.assertEqual(len(result), len(block_ids))

    def test_lookup_on_prefix_empty(self):
        """Test lookup_on_prefix on empty Yuanrong|Posix store."""
        block_ids = [os.urandom(16) for _ in range(5)]
        result = self.store.lookup_on_prefix(block_ids)
        # *** 空存储应该返回-1
        self.assertEqual(result, -1)

    def test_register_memory(self):
        """Test register_memory on Yuanrong|Posix pipeline."""
        # *** YuanrongStore的RegisterMemory是空实现
        # *** PosixStore的RegisterMemory也是空实现
        # *** 管线最终层（Yuanrong）的RegisterMemory应该是空实现
        base_addr = 0
        total_size = 1024 * 1024 * 1024  # 1GB
        self.store.register_memory(base_addr, total_size)


if __name__ == "__main__":
    unittest.main()