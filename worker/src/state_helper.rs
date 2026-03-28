use crate::transaction::TxID;
use crate::workload::AccountState;
use config::{Committee, ExecutorId};
use crypto::PublicKey;
use log::{info, warn};
use network::SimpleSender;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;

/// State transfer payload sent between executors for account migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransfer {
    pub tx_id: TxID,
    pub src_account_id: u64,
    pub src_account_state: AccountState,
}

/// Request from BatchExecutor to send state to another executor.
pub struct StateTransferRequest {
    pub state_transfer: StateTransfer,
    pub dest_executor_id: ExecutorId,
}

/// Messages exchanged between executors for state migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutorToExecutorMessage {
    StateTransfer {
        tx_id: TxID,
        src_account_id: u64,
        src_account_state: AccountState,
    },
}

/// Primary feedback message from executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutorPrimaryMessage {
    ExecutionFeedback(ExecutorId, u64),
}

pub struct StateHelper;

impl StateHelper {
    /// Spawn the state helper that handles outgoing state transfers, incoming state transfers,
    /// and feedback to primary.
    ///
    /// Returns the shared incoming_transfers buffer that BatchExecutor polls.
    pub fn spawn(
        executor_id: ExecutorId,
        name: PublicKey,
        committee: Committee,
        rx_send_state: Receiver<StateTransferRequest>,
        rx_executor_message: Receiver<ExecutorToExecutorMessage>,
        rx_feedback: Receiver<(ExecutorId, u64)>,
    ) -> Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>> {
        let incoming_transfers: Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let incoming_clone = incoming_transfers.clone();

        // Spawn outgoing state transfer handler
        let name_clone = name;
        let committee_clone = committee.clone();
        tokio::spawn(async move {
            Self::handle_outgoing(executor_id, name_clone, committee_clone, rx_send_state).await;
        });

        // Spawn incoming state transfer handler
        tokio::spawn(async move {
            Self::handle_incoming(rx_executor_message, incoming_clone).await;
        });

        // Spawn feedback forwarder
        let committee_clone = committee;
        tokio::spawn(async move {
            Self::handle_feedback(name, committee_clone, rx_feedback).await;
        });

        incoming_transfers
    }

    async fn handle_outgoing(
        _executor_id: ExecutorId,
        name: PublicKey,
        committee: Committee,
        mut rx_send_state: Receiver<StateTransferRequest>,
    ) {
        let mut network = SimpleSender::new();
        while let Some(request) = rx_send_state.recv().await {
            let dest_addr = match committee.executor(&name, &request.dest_executor_id) {
                Ok(addrs) => addrs.executor_to_executor,
                Err(e) => {
                    warn!("Failed to find executor {}: {}", request.dest_executor_id, e);
                    continue;
                }
            };
            let msg = ExecutorToExecutorMessage::StateTransfer {
                tx_id: request.state_transfer.tx_id,
                src_account_id: request.state_transfer.src_account_id,
                src_account_state: request.state_transfer.src_account_state,
            };
            let serialized = bincode::serialize(&msg).expect("Failed to serialize state transfer");
            network.send(dest_addr, bytes::Bytes::from(serialized)).await;
        }
    }

    async fn handle_incoming(
        mut rx_executor_message: Receiver<ExecutorToExecutorMessage>,
        incoming_transfers: Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>>,
    ) {
        while let Some(msg) = rx_executor_message.recv().await {
            match msg {
                ExecutorToExecutorMessage::StateTransfer {
                    tx_id,
                    src_account_id,
                    src_account_state,
                } => {
                    let transfer = StateTransfer {
                        tx_id,
                        src_account_id,
                        src_account_state,
                    };
                    let mut map = incoming_transfers.lock().unwrap();
                    map.entry(tx_id).or_default().push(transfer);
                }
            }
        }
    }

    async fn handle_feedback(
        name: PublicKey,
        committee: Committee,
        mut rx_feedback: Receiver<(ExecutorId, u64)>,
    ) {
        let mut network = SimpleSender::new();
        let primary_addr = committee
            .primary(&name)
            .expect("Own key not in committee")
            .executor_to_primary;

        while let Some((executor_id, last_executed)) = rx_feedback.recv().await {
            let msg = ExecutorPrimaryMessage::ExecutionFeedback(executor_id, last_executed);
            let serialized = bincode::serialize(&msg).expect("Failed to serialize feedback");
            network
                .send(primary_addr, bytes::Bytes::from(serialized))
                .await;
            info!(
                "Sent execution feedback: executor={}, last_executed={}",
                executor_id, last_executed
            );
        }
    }
}
