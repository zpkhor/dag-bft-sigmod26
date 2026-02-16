// Copyright(C) Facebook, Inc. and its affiliates.
use crate::executor::WorkerToExecutorMessage;
use crate::transaction::{Transaction};
use crate::worker::WorkerMessage;
use crate::workload::WorkloadType;
use bytes::Bytes;
use config::{Committee, ExecutionMode, ExecutorId, Partition, WorkerId};
use crypto::{Digest, PublicKey};
use log::{debug, info, warn};
use network::SimpleSender;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Message sent from Primary to Router
#[derive(Debug)]
pub struct ExecuteCommand {
    pub digest: Digest,
    pub sequence: u64,
}

/// Helper function to send execution message to executor process via network.
/// Used by both DirectRouter and Router in isolated execution mode.
async fn send_to_executor(
    network: &mut SimpleSender,
    addr: SocketAddr,
    digest: Digest,
    sequence: u64,
    txs: Vec<Vec<u8>>,
) {
    let msg = if txs.is_empty() {
        WorkerToExecutorMessage::SequenceSync(sequence)
    } else {
        WorkerToExecutorMessage::Execute(digest, sequence, txs)
    };

    let serialized = bincode::serialize(&msg)
        .expect("Failed to serialize WorkerToExecutorMessage");

    network.send(addr, Bytes::from(serialized)).await;
}

/// Direct router: executes all transactions locally without partition-based forwarding.
/// Used when load balancing is disabled and routing overhead is unnecessary.
pub struct DirectRouter {
    /// The id of this worker (for logging).
    worker_id: WorkerId,
    /// The persistent storage to retrieve batches.
    store: Store,
    /// Receives execute commands from the primary.
    rx_execute: Receiver<ExecuteCommand>,
    /// Channel to local BatchExecutor (used in InProcess mode).
    tx_batch_executor: Option<Sender<(Digest, u64, Vec<Vec<u8>>)>>,
    /// Execution mode: in_process or isolated.
    execution_mode: ExecutionMode,
    /// Executor address for this worker (for isolated mode).
    executor_address: Option<SocketAddr>,
    /// Network sender for executor communication (isolated mode).
    executor_network: SimpleSender,
}

impl DirectRouter {
    pub fn spawn(
        worker_id: WorkerId,
        store: Store,
        rx_execute: Receiver<ExecuteCommand>,
        tx_batch_executor: Option<Sender<(Digest, u64, Vec<Vec<u8>>)>>,
        execution_mode: ExecutionMode,
        executor_address: Option<SocketAddr>,
    ) {
        tokio::spawn(async move {
            Self {
                worker_id,
                store,
                rx_execute,
                tx_batch_executor,
                execution_mode,
                executor_address,
                executor_network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.rx_execute.recv().await {
            if let Err(e) = self.route_execution(cmd).await {
                warn!("DirectRouter: Failed to route execution: {}", e);
            }
        }
    }

    async fn route_execution(&mut self, cmd: ExecuteCommand) -> Result<(), Box<dyn std::error::Error>> {
        let ExecuteCommand { digest, sequence } = cmd;

        // Retrieve the batch from storage (with blocking wait if not available)
        let serialized = match self.store.read(digest.to_vec()).await? {
            Some(data) => data,
            None => {
                info!(
                    "DirectRouter: Batch {:?} (seq={}) not yet in storage, waiting...",
                    digest, sequence
                );
                self.store.notify_read(digest.to_vec()).await?;
                self.store
                    .read(digest.to_vec())
                    .await?
                    .ok_or_else(|| format!("Batch {:?} not found after sync", digest))?
            }
        };

        // Deserialize batch
        let message: WorkerMessage = bincode::deserialize(&serialized)?;
        let batch = match message {
            WorkerMessage::Batch(batch) => batch,
            _ => return Err("Expected WorkerMessage::Batch".into()),
        };

        // Route to executor based on execution mode
        match self.execution_mode {
            ExecutionMode::InProcess => {
                // Current behavior: send to local BatchExecutor via channel
                self.tx_batch_executor
                    .as_ref()
                    .expect("tx_batch_executor should be Some in InProcess mode")
                    .send((digest.clone(), sequence, batch.clone()))
                    .await
                    .expect("Failed to send to local executor");
            }
            ExecutionMode::Isolated => {
                // New behavior: send to executor process via network
                let addr = self
                    .executor_address
                    .expect("executor_address should be Some in Isolated mode");

                send_to_executor(&mut self.executor_network, addr, digest.clone(), sequence, batch.clone()).await;

                debug!(
                    "DirectRouter (worker {}): Sent {} txs to executor at {} (seq={})",
                    self.worker_id,
                    batch.len(),
                    addr,
                    sequence
                );
            }
        }

        Ok(())
    }
}

/// The `Router` receives Execute commands from the Primary, filters transactions
/// by partition assignment, and routes them to the appropriate worker's BatchExecutor.
///
/// # Routing Strategy
/// - Extracts account_id from each transaction
/// - Looks up worker assignment in partition cache
/// - Partitions transactions into buckets by target worker
/// - Sends to ALL workers (even if transaction list is empty)
///
/// # O(W²) Complexity
/// This Router sends W messages per batch (one to each worker), regardless of
/// whether the worker actually has transactions to execute. This is necessary for
/// strict sequence ordering across all workers within a validator:
/// - Each worker must advance its sequence counter in lockstep
/// - Empty messages use `SequenceSync(seq)` optimization (only sequence number)
/// - Total: W workers × W messages = O(W²) messages per batch
///
/// Trade-off: Guarantees sequential consistency at the cost of messaging overhead.
/// For validators with many workers (W >> 10), consider DirectRouting instead.
///
/// # Partition Sources
/// - With LoadBalancer: Dynamic partitions from hypergraph algorithm
/// - Without LoadBalancer: Static partitions from initial range/hash sharding
pub struct Router {
    /// The id of this worker.
    worker_id: WorkerId,
    /// The persistent storage to retrieve batches.
    store: Store,
    /// Current partition for states for execution, init by worker and is static during runtime
    /// TODO: can be dynamic by populating the LookUpTable if we decide to migrate the state in the future, current routing overhead is low
    states_partition_cache: Arc<RwLock<Option<Partition>>>,
    /// Network sender to forward transactions to peer workers.
    network: SimpleSender,
    /// Receives execute commands from the primary.
    rx_execute: Receiver<ExecuteCommand>,
    /// Workload type configured at worker initialization (static per deployment).
    workload_type: WorkloadType,
    /// Network addresses of all workers for this authority (including local).
    worker_addresses: HashMap<WorkerId, std::net::SocketAddr>,
    /// Network addresses of all executors for this authority (including local).
    executor_addresses: HashMap<ExecutorId, std::net::SocketAddr>,
    /// Merged channel to BatchExecutor (used for all workers, local and remote, InProcess mode).
    tx_batch_executor: Option<Sender<(Digest, u64, Vec<Vec<u8>>)>>,
    /// Execution mode: in_process or isolated.
    execution_mode: ExecutionMode,
    /// Network sender for executor communication (isolated mode).
    executor_network: SimpleSender,
}

impl Router {
    pub fn spawn(
        name: PublicKey,
        worker_id: WorkerId,
        committee: Committee,
        store: Store,
        states_partition_cache: Arc<RwLock<Option<Partition>>>,
        rx_execute: Receiver<ExecuteCommand>,
        workload_type: WorkloadType,
        tx_batch_executor: Option<Sender<(Digest, u64, Vec<Vec<u8>>)>>,
        execution_mode: ExecutionMode,
    ) {
        // Uses worker_to_worker addresses since ForwardExecute messages are sent between workers.
        let worker_addresses: HashMap<WorkerId, std::net::SocketAddr> = committee
            .authorities
            .get(&name)
            .map(|authority| {
                authority
                    .workers
                    .iter()
                    .map(|(wid, addr)| (*wid, addr.worker_to_worker))
                    .collect()
            })
            .unwrap_or_else(HashMap::new);

        // Get all executor addresses for this authority (for isolated mode remote routing)
        let executor_addresses: HashMap<ExecutorId, std::net::SocketAddr> = committee
            .authorities
            .get(&name)
            .map(|authority| {
                authority
                    .executors
                    .iter()
                    .map(|(wid, addr)| (*wid, addr.worker_to_executor))
                    .collect()
            })
            .unwrap_or_else(HashMap::new);

        tokio::spawn(async move {
            Self {
                worker_id,
                store,
                states_partition_cache,
                network: SimpleSender::new(),
                rx_execute,
                workload_type,
                worker_addresses,
                executor_addresses,
                tx_batch_executor,
                execution_mode,
                executor_network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.rx_execute.recv().await {
            if let Err(e) = self.route_execution(cmd).await {
                warn!("Failed to route execution: {}", e);
            }
        }
    }

    async fn route_execution(&mut self, cmd: ExecuteCommand) -> Result<(), Box<dyn std::error::Error>> {
        let ExecuteCommand { digest, sequence } = cmd;

        // Retrieve the batch from storage.
        // If the batch is not yet available (still being synced from other validators),
        // wait for it to arrive before proceeding with execution.
        let serialized = match self.store.read(digest.to_vec()).await? {
            Some(data) => data,
            None => {
                // Batch not in storage yet - it's being synced from other validators.
                // The certificate is proof that at least f+1 honest nodes have it,
                // so the background Synchronizer will fetch it.
                info!(
                    "Batch {:?} (seq={}) not yet in storage, waiting for sync to complete...",
                    digest, sequence
                );

                // Wait for the batch to arrive in storage.
                // This blocks processing of subsequent Execute commands until this batch is ready,
                // which maintains strict sequential execution order and prevents buffer explosion.
                self.store.notify_read(digest.to_vec()).await?;

                info!(
                    "Batch {:?} (seq={}) sync completed, proceeding with execution",
                    digest, sequence
                );

                // Retry read after notification
                self.store
                    .read(digest.to_vec())
                    .await?
                    .ok_or_else(|| format!("Batch {:?} not found in storage after sync notification", digest))?
            }
        };

        // Deserialize the WorkerMessage and extract the batch.
        let message: WorkerMessage = bincode::deserialize(&serialized)?;
        let batch = match message {
            WorkerMessage::Batch(batch) => batch,
            _ => return Err(format!("Expected WorkerMessage::Batch, got other variant").into()),
        };

        let partition = self.states_partition_cache.read().unwrap().clone();

        // Pre-populate HashMap with all worker IDs for this authority.
        // O(W²) behavior: ALL workers must receive messages (even empty) for synchronization.
        // This ensures strict sequence ordering across all workers within the validator.
        let mut txs_by_executors: HashMap<ExecutorId, Vec<Vec<u8>>> = HashMap::new();
        for executor_id in self.executor_addresses.keys() {
            txs_by_executors.insert(*executor_id, Vec::new());
        }

        for tx_bytes in batch.iter() {
            let transaction = Transaction::new(tx_bytes);

            match self.workload_type {
                WorkloadType::Default => {
                    // Default workload: execute locally (no partitioning).
                    txs_by_executors
                        .get_mut(&self.worker_id) // Only support in-process execution for Default workload, executor lives in the same worker process
                        .unwrap()
                        .push(tx_bytes.clone());
                }
                WorkloadType::SmallBank => {
                    // SmallBank: extract src and dest account IDs
                    let (src_opt, dest_opt) = transaction.extract_account_ids();

                    match (src_opt, dest_opt) {
                        (Some(src), Some(dest)) => {
                            let partition_assignment = partition.as_ref()
                                .expect("Partition required for SmallBank workload");

                            let src_owner = partition_assignment.get(&src).copied()
                                .expect("Source account must have owner in partition");
                            let dest_owner = partition_assignment.get(&dest).copied()
                                .expect("Dest account must have owner in partition");

                            // Phase 2: Check if distributed SendPayment (different executors)
                            if src != dest && src_owner != dest_owner {
                                // Distributed: multicast to BOTH executors
                                txs_by_executors.get_mut(&src_owner).unwrap().push(tx_bytes.clone());
                                txs_by_executors.get_mut(&dest_owner).unwrap().push(tx_bytes.clone());
                                // debug!(
                                //     "Routing distributed SendPayment: src={} (worker {}) -> dest={} (worker {})",
                                //     src, src_owner, dest, dest_owner
                                // );
                            } else {
                                // Local: single-account tx or same executor (existing Phase 1 logic)
                                txs_by_executors.get_mut(&src_owner).unwrap().push(tx_bytes.clone());
                            }
                        }
                        _ => {
                            // Malformed transaction, warn and ignore.
                            warn!("Malformed SmallBank transaction, ignoring: {:?}", tx_bytes);
                        }
                    }
                }
            }
        }

        // Send to ALL workers (hybrid approach: direct channel for local, network for remote).
        // Optimization: For empty batches, send only sequence number to save bandwidth.
        match self.execution_mode {
            ExecutionMode::InProcess => {
                // InProcess mode: local via channel, remote via network to workers
                for (target_worker_id, txs) in txs_by_executors.iter() {
                    if *target_worker_id == self.worker_id {
                        // LOCAL: Direct channel send (no serialization)
                        self.tx_batch_executor
                            .as_ref()
                            .expect("tx_batch_executor should be Some in InProcess mode")
                            .send((digest.clone(), sequence, txs.clone()))
                            .await
                            .expect("Failed to send to local executor");

                        debug!(
                            "Sent to local executor (InProcess) with {} txs for batch {:?} (seq={})",
                            txs.len(),
                            digest,
                            sequence
                        );
                    } else {
                        // REMOTE: Send to remote workers
                        if let Some(&addr) = self.worker_addresses.get(target_worker_id) {
                            let message = if txs.is_empty() {
                                // Optimization: Send only sequence number for empty batches
                                WorkerMessage::SequenceSync(sequence)
                            } else {
                                WorkerMessage::ForwardExecute(digest.clone(), sequence, txs.clone())
                            };
                            let serialized = bincode::serialize(&message)
                                .expect("Failed to serialize worker message");

                            self.network.send(addr, Bytes::from(serialized)).await;

                            if !txs.is_empty() {
                                debug!(
                                    "Sent ForwardExecute to worker {} with {} txs for batch {:?} (seq={})",
                                    target_worker_id,
                                    txs.len(),
                                    digest,
                                    sequence
                                );
                            }
                        } else {
                            warn!("Worker {} not found in cached addresses", target_worker_id);
                        }
                    }
                }
            }
            ExecutionMode::Isolated => {
                // Isolated mode: all executors via network (no special case for local)
                for (target_executor_id, txs) in txs_by_executors.iter() {
                    if let Some(&addr) = self.executor_addresses.get(target_executor_id) {
                        send_to_executor(&mut self.executor_network, addr, digest.clone(), sequence, txs.clone()).await;

                        if !txs.is_empty() {
                            debug!(
                                "Sent to executor {} at {} with {} txs for batch {:?} (seq={})",
                                target_executor_id,
                                addr,
                                txs.len(),
                                digest,
                                sequence
                            );
                        }
                    } else {
                        warn!("Executor {} not found in cached addresses", target_executor_id);
                    }
                }
            }
        }

        // Log routing summary.
        let local_txs_len = txs_by_executors.get(&self.worker_id).map(|v| v.len()).unwrap_or(0);
        let remote_count: usize = txs_by_executors
            .iter()
            .filter(|(wid, _)| **wid != self.worker_id)
            .map(|(_, v)| v.len())
            .sum();
        let remote_workers = txs_by_executors
            .keys()
            .filter(|wid| **wid != self.worker_id)
            .count();

        info!(
            "Routed batch {:?} (seq={}): {} local, {} remote txs to {} workers",
            digest,
            sequence,
            local_txs_len,
            remote_count,
            remote_workers
        );

        Ok(())
    }
}
