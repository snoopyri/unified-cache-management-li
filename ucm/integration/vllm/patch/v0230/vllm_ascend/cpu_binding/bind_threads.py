"""UCM-aware CPU role allocation for vLLM-Ascend 0.23.0rc1."""

import os

import psutil

from ucm.integration.vllm.patch.cpu_binding_affinity_patch import (
    assign_cpu_roles,
)
from ucm.integration.vllm.patch.cpu_binding_affinity_patch import (
    bind_threads as bind_ucm_threads,
)
from ucm.integration.vllm.patch.cpu_binding_affinity_patch import (
    print_plan as print_ucm_plan,
)

_UCM_CPU_AFFINITY_CORES_ENV = "UCM_CPU_AFFINITY_CORES"


def _publish_ucm_cores(self) -> None:
    """Make this rank's UCM CPU subset available to the later store setup."""
    current_npu = self.device_info.running_npu_list[self.rank_id]
    ucm_cores = getattr(self, "assign_ucm", {}).get(current_npu, [])
    if ucm_cores:
        os.environ[_UCM_CPU_AFFINITY_CORES_ENV] = ",".join(map(str, ucm_cores))
    else:
        os.environ.pop(_UCM_CPU_AFFINITY_CORES_ENV, None)


def allocate(self) -> None:
    """Reserve UCM cores while retaining v0.23.0rc1 device-specific rules."""
    if self._is_ascend_950():
        # Ascend 950 supplies one topology-aware CPU cluster per NPU. Keep that
        # cluster assignment, but partition the cluster so UCM I/O workers do
        # not contend with the vLLM worker threads when affinity is enabled.
        self.assign_ucm = {}
        for npu, cpu_pool in self.npu_cpu_pool.items():
            assign_cpu_roles(self, npu, cpu_pool, [], [])
        _publish_ucm_cores(self)
        return

    self.assign_ucm = {}
    reserve_irq_cpus = self._reserve_irq_cpus()
    min_cpus_per_npu = self._min_cpus_per_npu()
    for npu, cpu_pool in self.npu_cpu_pool.items():
        if len(cpu_pool) < min_cpus_per_npu:
            raise RuntimeError(
                "The number of CPUs is insufficient. Each NPU requires at "
                f"least {min_cpus_per_npu} CPUs."
            )
        main = cpu_pool[2:-2] if reserve_irq_cpus else cpu_pool[:-2]
        assign_cpu_roles(self, npu, main, [cpu_pool[-2]], [cpu_pool[-1]])
    _publish_ucm_cores(self)


def print_plan(self) -> None:
    """Print the worker/UCM split for every supported device."""
    print_ucm_plan(self)


def bind_threads(self) -> None:
    """Bind UCM tasks and preserve Ascend 950 memory placement."""
    if self._is_ascend_950():
        bind_ucm_threads(self)
        main_pid = str(psutil.Process().pid)
        current_npu = self.device_info.running_npu_list[self.rank_id]
        self.bind_memory(main_pid, current_npu)
        return
    bind_ucm_threads(self)
