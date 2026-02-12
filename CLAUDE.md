# Bash commands
`source activate narwhal39 && cargo build --release --features benchmark`: Compile

# Architecture
- Narwhal is a BFT system with a **three-tier process architecture**:
  - **Tier 1 (Primary)**: One per validator (handles consensus)
  - **Tier 2 (Workers)**: Multiple per validator (handle transaction batching and routing)
- There is one client per worker, each authority has workers to receive requests
- Local bench runs all processes on one machine, clock drift is not an issue

## Certificate Lifecycle
- **Voting**: Validator votes for header only after verifying parent certificates and batches are available
- **Certification**: Certificate formed when 2f+1 validators vote for a header
- **Key Insight**: A validator can commit a certificate it never voted for (certified by others)
  - Creates gap between consensus commitment and local batch availability
  - Execution must verify batch availability independently

## Primary Components
- **Core**: Central coordination component
- **Payload Receiver**: Receives payloads (digests) from workers
- **Consensus**: Runs the consensus protocol
- **Proposer**: Proposes blocks
- **Header Waiter**: Waits for headers from other primaries
- **Certificate Waiter**: Waits for certificates
- **Garbage Collector**: Receives consensus round updates, broadcasts Cleanup(round) to workers
- **Receiver**: Handles incoming messages from other primaries
- **Helper**: Assists with batch requests from other primaries

## Worker Components (3 main flows)
1. **Handle messages from primary**: Receiver → Synchronizer → Simple Sender (to other workers)
2. **Handle client transactions**:
   - Receiver → Batch Maker (assembles txs into batches)
   - QuorumWaiter (waits for quorum of acks)
   - Processor (hashes and stores batches)
   - PrimaryConnector (sends batch digests to our primary)
3. **Handle messages from other workers**: Receiver → Processor + Helper (replies to batch requests)


# Rust common pitfalls
- Client writes transactions using BytesMut.put_u64() which is BIG-ENDIAN. Reading raw transactions bytes should then use from_be_bytes()

# Logging file names prefix
- primary-i: primary of validator i
- worker-i-j: worker j on validator i
- client-i-j: client of validator i for worker j

# Comment Writing Guidelines
- Do NOT comment the obvious - comments should not simply repeat what the code does.

# Dev note
- Don't run tests at all, I will handle testing manually.
- This is a experimental branch, no need to worry about breaking things and maintaining backwards compatibility