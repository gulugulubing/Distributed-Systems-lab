# Distributed Systems Labs Progress Summary

## Overview
This document tracks the development progress of the distributed systems labs (MIT 6.5840/6.824). The core focus has been on building a fault-tolerant, linearizable Key-Value store on top of a consensus algorithm.

| Lab | Module | Status | Difficulty | Notes                                                                  |
| :--- | :--- | :--- | :--- |:-----------------------------------------------------------------------|
| **Lab 1** | MapReduce | ✅ Completed | Low | Implemented basic distributed data processing.                         |
| **Lab 2** | KV Server | ✅ Completed | Low | Basic client/server architecture for key-value operations.             |
| **Lab 3** | Raft Consensus | ✅ Completed | High | Implemented leader election, log replication, persistence. |
| **Lab 4** | Fault-Tolerant KV | ⚠️ Completed | Medium | Linearizable KV store built on Raft. Rare Heisenbug under extreme load. |
| **Lab 5** | Sharded KV | ⬜ Pending | TBD | Horizontal scaling via sharding.                                       |

---

## Detailed Status Report

### Lab 3: Raft Implementation
* **Status:** Stable.
* **Achievements:** Successfully handled leader election, log replication, log compaction (snapshots), and persistence. The implementation passes the standard test suite hundreds of times without failure, demonstrating strong resilience to partitions and crash-restarts.

### Lab 4: Fault-Tolerant Key/Value Service
* **Status:** Functionally complete, but exhibits a rare performance bottleneck under synthetic stress tests.
* **Current Issue:**
    * **Symptom:** In `TestBasic4B` and `TestSpeed4B` (reliable network), the client occasionally receives `ErrMaybe` (approx. 1 in 500-1000 runs).
    * **Root Cause Analysis:** The issue appears to be **RPC Handler Starvation** / **Goroutine Explosion**. Under intense load, the Leader spawns a new goroutine for every `Start()` call to replicate logs. This floods the scheduler, causing CPU saturation.
    * **Effect:** Valid requests are committed by Raft, but the server is too overloaded to reply to the client in time. The client times out, retries, and receives a `WrongVersion` error (since the original request actually succeeded), which bubbles up as `ErrMaybe`.

### Lab 5: Sharded KV
* **Status:** Not started.
* **Goals:** Implement shard controller and shard movement to support horizontal scaling.