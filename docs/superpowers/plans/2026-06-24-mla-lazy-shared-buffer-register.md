# MLA Lazy Shared Buffer Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep MLA shared cache data pages unregistered until runtime transfers begin after vllm-ascend memory migration.

**Architecture:** Propagate an MLA-only lazy-registration flag into CacheStore. Shared TransBuffer setup initializes shared metadata but defers HostRegister; CacheStore activates the buffer once before Load or Dump submission.

**Tech Stack:** C++17, Python 3, POSIX shared memory, GoogleTest, Ascend host registration.

---

### Task 1: Specify lazy activation behavior

**Files:**
- Modify: `ucm/store/test/case/cache/cache_trans_buffer_test.cc`

- [ ] Add a failing test asserting lazy shared setup is inactive, `Activate()` makes it active, and repeated activation succeeds.
- [ ] Add an assertion that default shared setup remains eagerly active.
- [ ] Build/run the cache TransBuffer test and confirm failure is caused by the missing API.

### Task 2: Implement lazy shared registration

**Files:**
- Modify: `ucm/store/cache/cc/global_config.h`
- Modify: `ucm/store/cache/cc/cache_store.cc`
- Modify: `ucm/store/cache/cc/trans_buffer.h`
- Modify: `ucm/store/cache/cc/trans_buffer.cc`
- Modify: `ucm/store/cache/cc/buffer_manager.h`

- [ ] Parse `lazy_shared_buffer_register` into CacheStore configuration.
- [ ] Add thread-safe, idempotent, retryable shared-buffer activation.
- [ ] Avoid unregistering a shared data region that was never activated.
- [ ] Activate before CacheStore Load/Dump queue submission and propagate failure.
- [ ] Run the targeted test and confirm it passes.

### Task 3: Enable the behavior for MLA workers

**Files:**
- Modify: `ucm/integration/vllm/ucm_connector.py`
- Modify: `ucm/integration/vllm/hma_connector.py`

- [ ] Set `lazy_shared_buffer_register=true` only for MLA worker store configurations.
- [ ] Preserve scheduler watcher and non-MLA behavior.
- [ ] Run Python syntax checks and relevant connector tests.

### Task 4: Verify the change

**Files:**
- Verify all modified files.

- [ ] Run the targeted C++ test and available cache-store test suite.
- [ ] Run `git diff --check` and formatter checks.
- [ ] Review the diff for unrelated changes and document environment limitations.
