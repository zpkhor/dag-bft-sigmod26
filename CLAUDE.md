# Bash commands and python run
`source activate narwhal39 && cargo build --release --features benchmark`: Compile
`source activate narwhal39`: before running python 

# Purpose of the repo:
- To study load balancing algorithm for validator, answering which validator should a client submits its reqeusts to. The mechanism is MigrationNotice sent from worker's Synchronizer as MigrationMessage. Client needs to listen to this
- To study load balancing algorithm for executors, balancing the number of requests per executor, yet minimizing distributed transaction

# BFT system model
- The network is partial synchronous, there is a known bound ∆ on message transmission after some unknown Global Stabilization Time

# Architecture
- Narwhal is a BFT system with a **two-tier process architecture**:
  - **Tier 1 (Primary)**: One per validator (handles consensus)
  - **Tier 2 (Workers)**: Multiple per validator (handle transaction batching and receival of batch from others)
- The client sends into unbounded mpsc to include queueing delay for latency. Backpressure from the worker's bounded tx_batch_maker channel propagates through TCP but only grows the unbounded channel in memory — the client's send() never blocks. The signal for overload is client misses due to the packet drop
- Single client process simulates N geographically distributed clients — one per validator region, and send using unbounded mpsc channel to avoid TCP backpressure causing Coordinated Omission in performance measurement
- Clock drift is not an issue

## Transaction Flow
- Client → Worker (BatchMaker) → QuorumWaiter → Processor → PrimaryConnector → Primary (Proposer) → Header → Votes → Certificate → Consensus → Commit

## Executor (only if NUM_EXECUTORS env var is set)
Commit → Executor

## Certificate Lifecycle
- **Voting**: Validator votes for header only after verifying parent certificates and batches are available
- **Certification**: Certificate formed when 2f+1 validators vote for a header
- **Key Insight**: A validator can commit a certificate it never voted for (certified by others)
  - Creates gap between consensus commitment and local batch availability
  - Execution must verify batch availability independently

## Primary Components
- **Core**: Central coordination component
- **Payload Receiver**: Receives payloads (digests) from workers
- **Proposer**: Proposes blocks, creates header when payload_size >= header_size OR max_header_delay timer. With 1 worker producing ~5 digests/sec (32B each)
- **Consensus**: Runs the consensus protocol, round-robin leader election. Commits entire sub-DAG when leader has f+1 support. All validators' certificates in the sub-DAG get committed together.
- **Garbage Collector**: Receives updates from consensus, broadcasts Cleanup(round) to workers

## Worker Components (3 main flows)
1. **Handle messages from primary**: Receiver → Synchronizer → Simple Sender (to other workers); Execute → Router → executors
2. **Handle client transactions**:
   - Receiver → Batch Maker (assembles txs into batches)
       - Seals when current_batch_size >= batch_size OR max_batch_delay timer. At low rates, batches are always timer-sealed.
       - account_id is at bytes 10..18 of raw transaction (after 1B tx_type + 1B region_id + 8B counter)
   - QuorumWaiter (waits for quorum of acks)
   - Processor (hashes and stores batches)
   - PrimaryConnector (sends batch digests to our primary)
3. **Handle messages from other workers**: Receiver → Processor + Helper (replies to batch requests)

## Execution Flow

1. **Consensus** commits certificate → sends to GarbageCollector
2. **BatchDispatcher** (Primary) assigns monotonic sequence numbers per batch
   - Example: Cert with 3 batches → sequences [N, N+1, N+2]
   - Sends Execute(digest, worker_id, seq) to workers
3. **Router** (Worker) retrieves batch from storage, routes to executor(s)
   - Default (data fusion): Router broadcasts full batch to ALL executors
   - Writeback mode (`WRITEBACK_EXECUTOR=1 fab`): WritebackRouter partitions transactions per-executor by account ownership
   - Routes via TCP network to executor processes
4. **Executor** — two modes selectable via `use_writeback_executor` config:
   - **Data fusion (default, `BatchExecutor`)**: One-way state migration on cross-executor transactions. Dynamic state partition
   - **Writeback (`DistributedTxExecutor`)**: Bidirectional state movement, transfer + writeback. Accounts stay with original owner, static state partition
   - Both: buffer out-of-order batches, execute in sequence, send client replies
5. **ClientReplier** (Executor process) sends signed replies to clients

## BatchExecutor Threading & Migration
- **Single-threaded**: `run()` is a sequential recv loop — no concurrent access to `pending_queues` or `drained_incoming_transfers`. Only `incoming_transfers` is shared with StateHelper via Mutex. Many apparent race conditions are false positives.
- **`process_incoming_transfers` only runs at batch boundaries** (end of `execute_batch`), not event-driven by transfer arrival. MigrateIn txs whose transfers arrive between batches must wait for the next batch.
- **`drain_pending_queues` skips MigrateIn** — only `process_incoming_transfers` handles MigrateIn execution.

# Rust common pitfalls
- Client writes transactions using BytesMut.put_u64() which is BIG-ENDIAN. Reading raw transactions bytes should then use from_be_bytes()

# Python/Rust key ordering pitfall
- Rust `BTreeMap<PublicKey, ...>` sorts by **decoded raw bytes** of the 32-byte public key (`PublicKey([u8;32])` derives `Ord` = lexicographic byte comparison).
- Python `json.dump(..., sort_keys=True)` sorts JSON keys as **base64 strings** (ASCII lexicographic). This is a DIFFERENT ordering
- `committee.sorted_authority_names()` correctly matches Rust's BTreeMap order (sorts by `base64.b64decode(n)`). Always use this when order must match Rust.

# Logging file names prefix
- primary-i: primary of validator i
- worker-i-j: worker j on validator i

# Comment Writing Guidelines
- Do NOT comment the obvious - comments should not simply repeat what the code does.

# Shell scripts (*.sh) Editing Conventions
- When editing, verify variable names match exactly what's already used in the `benchmark/fabfile.py` Grep for existing usage before introducing variable references.
- Print abs path over relative path, so it is clickable in VSCode

# Batch run scripts and parsing label for validator level load balancing
- label starts starts and ends with `n<num_nodes>_` and ends with `_r<rate>`
- There are 4 general scenarios: balanced, imbalanced rate, ≤ f low bandwidth validator, f+1 bandwidth validator. The later three labels are `rate_imb`, `bw_f`, `bw_f1`. Balanced has no scenario label
- There few routing-mode labels: default, rr, bl. The later two are round-robin and baseline, the default is the load balancing algorithm this repo study and has no scenario label
- These together form a full label  `n<num_nodes>_(scenario)(routing-mode)_r<rate>`
- The scenarios and routing-mode can be configured using env var in the `benchmark/fabfile.py`
- The performance metrics are parsed, calculated, formatted and ouput by `benchmark/benchmark/logs.py`

# Dev note
- Don't run tests at all, I will handle testing manually.
- When editing existing code, don't "improve" adjacent code, comments, or formatting. If you notice unrelated dead code, mention it don't delete it.
- Don't remove any TODO when editing files
- `benchmark/fabfile.py` is the entrypoint and contains environ vars that flow through `benchmark/benchmark/cloudlab_bench.py` and `benchmark/benchmark/config.py` and `config/src/lib.rs` to control the behavior of program, when making any edit always check if these need to be updated
- After editing `benchmark/fabfile.py`, check if the *.sh needs to be updated
- When editing python file, please DO NOT use None as default arg (always use positional args over keyword args)
- I prefer hardfail (assert, panic) rather than warning or silence continue or skipping


**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

# Performance
"f+1 Commit latency (workers)" is per batch logging from primary, "PER-VALIDATOR COMMIT METRICS" is by per sample tx reply (worker needs to fetch the batch and reply). If the former is greater than later a lot, then it means the store is bottleneck, IN_MEMORY_STORE is used to bypass this bottleneck