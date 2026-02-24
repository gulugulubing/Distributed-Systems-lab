# Distributed Systems Labs Progress Summary

## Overview
This document tracks the development progress of the distributed systems labs (MIT 6.5840/6.824). The core focus has been on building a fault-tolerant, linearizable Key-Value store on top of a consensus algorithm.

| Lab | Module | Status      | Difficulty | Notes                                                                 |
| :--- | :--- |:------------|:-----------|:----------------------------------------------------------------------|
| **Lab 1** | MapReduce | ✅ Completed | Low        | Implemented basic distributed data processing.                        |
| **Lab 2** | KV Server | ✅ Completed | Low        | Basic client/server architecture for key-value operations.            |
| **Lab 3** | Raft Consensus | ✅ Completed | High       | Implemented leader election, log replication, persistence.            |
| **Lab 4** | Fault-Tolerant KV | ✅ Completed | Medium     | Linearizable KV store built on Raft.                                  |
| **Lab 5** | Sharded KV | ✅ Completed | High       | Horizontal scaling via sharding.                                      |

---

## Detailed Status Report

### Lab 3: Raft Implementation
* **Achievements:** Successfully handled leader election, log replication, log compaction (snapshots), and persistence. The implementation passes the standard test suite hundreds of times without failure, demonstrating strong resilience to partitions and crash-restarts.

### Lab 4: Fault-Tolerant Key/Value Service
* **Achievements:**  Built a fault-tolerant key/value storage service using above Raft library by implementing a replicated-state machine package and a replicated key/value service with using snapshots. 
* **Test Artifact:**
    * **Symptom:** In `TestBasic4B` and `TestSpeed4B` (reliable network), the client occasionally receives `ErrMaybe` (approx. 1 in 500-1000 runs) **only when running with the `-race` flag. The host laptop(M3 Air) experiences unusually high temperatures**.
    * **Observation:** If the `-race` flag is removed, the tests pass thousands of times consistently without failure.
    * **Root Cause Analysis:** The Go race detector's massive overhead, which distorts the test framework's strict timing expectations(making the code run from 2x to 10x slower).

### Lab 5: Sharded KV
* **Achievements:** Completed implementing a highly-available sharded key/value service with many shard groups for scalability, reconfiguration to handle changes in load, and with a fault-tolerant controller.
* **Tips of ChangeCfg in 5B and 5C:** Once a configuration wins the race and is saved to the KV server, it will be executed(Unstoppable Mandate). Configs Increase Exactly 1 by 1 (No Skipping).