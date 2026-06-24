# MLA Lazy Shared Buffer Registration Design

## Problem

MLA workers map the same large POSIX shared-memory cache and immediately register
the whole data region as pinned host memory. vllm-ascend later runs
`migratepages` once per worker toward different NUMA nodes, causing expensive
processing of the same shared physical pages during service startup.

## Design

Add an opt-in cache configuration named `lazy_shared_buffer_register`. The vLLM
connector enables it only for MLA worker stores. Shared-buffer setup still
creates/opens the shm file and initializes header and metadata, but defers the
device HostRegister operation.

`TransBuffer::Activate()` performs the deferred registration. It is retryable,
thread-safe, and idempotent. `CacheStore::Load()` and `CacheStore::Dump()` call it
before submitting their first transfer, which occurs after vllm-ascend warmup
and `migratepages`. Destruction unregisters the buffer only after a successful
activation.

Local buffers, shared-buffer watchers, non-MLA models, and configurations that
do not enable the new option retain their existing eager behavior.

## Failure Handling

If activation fails, the transfer returns the activation error without entering
the transfer queue. A later transfer retries activation. Concurrent first
transfers serialize around the registration operation.

## Verification

Cache TransBuffer tests cover lazy shared setup, first activation, repeated
activation, and unchanged eager behavior. Existing cache tests and Python syntax
checks remain part of verification.
