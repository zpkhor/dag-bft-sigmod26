# Bash commands
`source activate narwhal39 && cargo build --release --features benchmark`: Compile

`narwhal-cloudlab-smallbank/` is another fork repo, it shares same base branch with this repo, it has cloudlab deploy code

# Architecture
- Narwhal is a BFT system with a **two-tier process architecture**:
  - **Tier 1 (Primary)**: One per validator (handles consensus)
  - **Tier 2 (Workers)**: Multiple per validator (handle transaction batching and receival of batch from others)
- The number of client and validator are same. In docker mode, the client runs in its own dedicated container with 4 pinned CPUs adjacent (directly after) its validator's CPU range, with 0ms tc latency to the validator.
- The client sends into unbounded mpsc to include queueing delay for latency. Backpressure from the worker's bounded tx_batch_maker channel propagates through TCP but only grows the unbounded channel in memory — the client's send() never blocks. The signal for overload is client misses due to the packet drop of limited interface queue length
- Local bench runs all processes on one machine, clock drift is not an issue

## Transaction Flow
- Client → Worker (BatchMaker) → QuorumWaiter → Processor → PrimaryConnector → Primary (Proposer) → Header → Votes → Certificate → Consensus → Commit

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
- **Garbage Collector**: Receives consensus round updates, broadcasts Cleanup(round) to workers

## Worker Components (3 main flows)
1. **Handle messages from primary**: Receiver → Synchronizer → Simple Sender (to other workers)
2. **Handle client transactions**:
   - Receiver → Batch Maker (assembles txs into batches)
       - Seals when current_batch_size >= batch_size OR max_batch_delay timer. At low rates, batches are always timer-sealed.
       - account_id is first 8 bytes of transaction
   - QuorumWaiter (waits for quorum of acks)
   - Processor (hashes and stores batches)
   - PrimaryConnector (sends batch digests to our primary)
3. **Handle messages from other workers**: Receiver → Processor + Helper (replies to batch requests)


# Rust common pitfalls
- Client writes transactions using BytesMut.put_u64() which is BIG-ENDIAN. Reading raw transactions bytes should then use from_be_bytes()

# Logging file names prefix
- primary-i: primary of validator i
- worker-i-j: worker j on validator i
- client-i-0: client of validator i (j is always 0; one client per validator)

# Comment Writing Guidelines
- Do NOT comment the obvious - comments should not simply repeat what the code does.

# Dev note
- Don't run tests at all, I will handle testing manually.
- This is a experimental branch to study performance impact due to validator load imbalance, no need to worry about breaking things and maintaining backwards compatibility. We are mainly using docker in benchmark/fabfile.py
- When editing existing code, don't "improve" adjacent code, comments, or formatting. If you notice unrelated dead code, mention it don't delete it.
- Don't remove any TODO when editing files
- `benchmark/fabfile.py` is the entrypoint and contains environ vars that flow through `benchmark/benchmark/docker_bench.py` and `benchmark/benchmark/config.py` and `config/src/lib.rs` to control the behavior of program, when making any edit always check if these need to be updated
- When editing python file, please DO NOT use None as default arg (always use positional args over keyword args)
- I prefer hardfail (assert, panic) rather than warning or silence continue or skipping

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.