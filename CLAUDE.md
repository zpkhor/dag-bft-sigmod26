# Bash commands
`source activate narwhal39 && cargo build --release --features benchmark`: Compile

# Architecture
- Narwhal is a BFT system with a **two-tier process architecture**:
  - **Tier 1 (Primary)**: One per validator (handles consensus)
  - **Tier 2 (Workers)**: Multiple per validator (handle transaction batching and receival of batch from others)
- There is one client per validator that connects to all of that validator's workers (or all validators' workers in round-robin mode). In docker mode, the client runs in its own dedicated container with 4 pinned CPUs adjacent (directly after) its validator's CPU range, with 0ms tc latency to the validator.
- The client sends at a fixed rate. When overloaded, backpressure from TCP / tx_batch_maker mpsc channel blocks the client's send() call. The signal for overload is client misses, not latency increase
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
- **Proposer**: Proposes blocks, creates header when payload_size >= header_size OR max_header_delay timer. With 1 worker producing ~5 digests/sec (32B each), headers mostly timer-sealed too.
- **Consensus**: Runs the consensus protocol, round-robin leader election. Commits entire sub-DAG when leader has f+1 support. All validators' certificates in the sub-DAG get committed together.
- **Header Waiter**: Waits for headers from other primaries
- **Certificate Waiter**: Waits for certificates
- **Garbage Collector**: Receives consensus round updates, broadcasts Cleanup(round) to workers
- **Receiver**: Handles incoming messages from other primaries
- **Helper**: Assists with batch requests from other primaries

## Worker Components (3 main flows)
1. **Handle messages from primary**: Receiver → Synchronizer → Simple Sender (to other workers)
2. **Handle client transactions**:
   - Receiver → Batch Maker (assembles txs into batches)
       - Seals when current_batch_size >= batch_size OR max_batch_delay timer. At low rates, batches are always timer-sealed.
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
- When editing existing code, don't "improve" adjacent code, comments, or formatting. If you notice unrelated dead code, mention it - don't delete it.
- Don't remove any TODO when editing files
- `benchmark/fabfile.py` is the entrypoint and contains environ var to control the behavior of program, when making any edit always check if it needs to be updated

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.