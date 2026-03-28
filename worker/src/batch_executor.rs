use crate::state_helper::{StateTransfer, StateTransferRequest};
use crate::transaction::{Transaction, TransactionHeader, TxID};
use crate::workload::{AccountStore, AccountState, SmallBankTransaction, SmallBankTxType};
use config::{Committee, Partition};
use crypto::{Digest, PublicKey};
use log::{info, warn};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};

const FEEDBACK_INTERVAL: u64 = 100;

/// Represents a client reply request with named fields.
pub struct ClientReplyRequest {
    pub batch_digest: Digest,
    pub client_addr: SocketAddr,
    pub tx_type: u8,
    pub tx_id: TxID,
    pub success: bool,
    pub account_id: u64,
    pub account_state: Option<AccountState>,
}

struct BufferedBatch {
    digest: Digest,
    transactions: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
enum TransactionLocality {
    Local,
    MigrateOut {
        owned_accounts_to_migrate: Vec<u64>,
        dest_executor_id: u32,
    },
    MigrateIn {
        accounts: Vec<(u64, u32)>,
    },
    Others {
        executor_id: u32,
    },
}

#[derive(Clone)]
struct PendingTransaction {
    tx_bytes: Vec<u8>,
    tx_header: TransactionHeader,
    sb_tx: SmallBankTransaction,
    batch_digest: Digest,
    required_accounts: Vec<u64>,
    locality: TransactionLocality,
}

pub struct BatchExecutor {
    rx_batch_executor: Receiver<(Digest, u64, Vec<Vec<u8>>)>,
    tx_client_reply: Sender<ClientReplyRequest>,
    committee: Committee,
    account_store: AccountStore,
    next_sequence: u64,
    buffer: BTreeMap<u64, BufferedBatch>,
    executor_id: u32,
    states_schedule_partition: Partition,
    pending_queues: HashMap<u64, VecDeque<PendingTransaction>>,
    name: PublicKey,
    tx_send_state: Sender<StateTransferRequest>,
    incoming_transfers: Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>>,
    tx_feedback: Sender<(u32, u64)>,
    last_feedback_sent: u64,
    executed_tx_count: u64,
    num_executors: u32,
    use_new_scheduler: bool,
}

impl BatchExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        rx_batch_executor: Receiver<(Digest, u64, Vec<Vec<u8>>)>,
        tx_client_reply: Sender<ClientReplyRequest>,
        committee: Committee,
        num_accounts: u64,
        num_executors: u32,
        min_balance: i64,
        max_balance: i64,
        executor_id: u32,
        initial_partition: Partition,
        name: PublicKey,
        tx_send_state: Sender<StateTransferRequest>,
        incoming_transfers: Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>>,
        tx_feedback: Sender<(u32, u64)>,
        use_new_scheduler: bool,
    ) {
        // Initialize account store from partition
        let mut account_store = AccountStore::new();
        for (&account_id, &owner_id) in initial_partition.iter() {
            if owner_id == executor_id {
                account_store.insert(
                    account_id,
                    crate::workload::create_account_with_seed(account_id, min_balance, max_balance),
                );
            }
        }
        info!(
            "BatchExecutor {}: loaded {} accounts (num_accounts={}, num_executors={}, new_scheduler={})",
            executor_id, account_store.len(), num_accounts, num_executors, use_new_scheduler
        );

        tokio::spawn(async move {
            Self {
                rx_batch_executor,
                tx_client_reply,
                committee,
                account_store,
                next_sequence: 0,
                buffer: BTreeMap::new(),
                executor_id,
                states_schedule_partition: initial_partition,
                pending_queues: HashMap::new(),
                name,
                tx_send_state,
                incoming_transfers,
                tx_feedback,
                last_feedback_sent: 0,
                executed_tx_count: 0,
                num_executors,
                use_new_scheduler,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        loop {
            let (digest, sequence, transactions) = match self.rx_batch_executor.recv().await {
                Some(msg) => msg,
                None => return,
            };

            if sequence == self.next_sequence {
                self.execute_batch(digest, transactions).await;
                self.next_sequence += 1;

                // Drain buffered batches
                while let Some(buffered) = self.buffer.remove(&self.next_sequence) {
                    self.execute_batch(buffered.digest, buffered.transactions).await;
                    self.next_sequence += 1;
                }
            } else if sequence > self.next_sequence {
                self.buffer.insert(
                    sequence,
                    BufferedBatch {
                        digest,
                        transactions,
                    },
                );
                assert!(
                    self.buffer.len() <= 5000,
                    "Executor {} buffer overflow: {} batches (next_seq={}, got={})",
                    self.executor_id, self.buffer.len(), self.next_sequence, sequence
                );
            }
            // sequence < next_sequence: duplicate, ignore

            // Send feedback periodically
            if self.next_sequence > self.last_feedback_sent + FEEDBACK_INTERVAL {
                let _ = self
                    .tx_feedback
                    .send((self.executor_id, self.next_sequence - 1))
                    .await;
                self.last_feedback_sent = self.next_sequence - 1;
            }
        }
    }

    async fn execute_batch(&mut self, digest: Digest, transactions: Vec<Vec<u8>>) {
        for tx_bytes in &transactions {
            let tx = Transaction::new(tx_bytes);
            let header = match tx.parse_header() {
                Some(h) => h,
                None => {
                    warn!("Executor {}: failed to parse tx header, skipping", self.executor_id);
                    continue;
                }
            };
            let sb_tx = match tx.parse_smallbank_payload() {
                Some(sb) => sb,
                None => {
                    warn!("Executor {}: failed to parse SmallBank payload, skipping", self.executor_id);
                    continue;
                }
            };

            if header.is_sample() {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Executing sample tx counter {} from client {} in batch {:?}",
                    header.id.tx_counter, header.id.client_id, digest
                );
            }

            let src = sb_tx.account_id;
            let _dest = sb_tx.dest_account_id;

            // Determine locality: which executor owns this account?
            let target_executor = *self
                .states_schedule_partition
                .get(&src)
                .unwrap_or_else(|| panic!("Account {} not in partition", src));

            if target_executor == self.executor_id {
                // Local execution
                let success = self.execute_local_tx(&sb_tx);
                self.executed_tx_count += 1;

                if header.is_sample() {
                    // NOTE: This log entry is used to compute performance.
                    info!(
                        "Completed executing sample tx from client {} counter {}",
                        header.id.client_id, header.id.tx_counter
                    );
                }

                // Send reply for sample txs only
                if header.is_sample() {
                    let client_addr = match self.committee.client_reply_address(header.id.client_id) {
                        Some(addr) => *addr,
                        None => {
                            warn!(
                                "Client ID {} not found in committee, skipping reply",
                                header.id.client_id
                            );
                            continue;
                        }
                    };
                    let _ = self
                        .tx_client_reply
                        .send(ClientReplyRequest {
                            batch_digest: digest.clone(),
                            client_addr,
                            tx_type: header.tx_type,
                            tx_id: header.id,
                            success,
                            account_id: src,
                            account_state: self.account_store.get(&src).cloned(),
                        })
                        .await;
                }
            }
            // Others: skip (another executor handles it)
        }
    }

    fn execute_local_tx(&mut self, sb_tx: &SmallBankTransaction) -> bool {
        let account = match self.account_store.get_mut(&sb_tx.account_id) {
            Some(a) => a,
            None => {
                warn!(
                    "Executor {}: account {} not found in local store",
                    self.executor_id, sb_tx.account_id
                );
                return false;
            }
        };

        match sb_tx.tx_type {
            SmallBankTxType::Balance => {
                let _ = account.execute_balance();
                true
            }
            SmallBankTxType::DepositChecking => {
                account.execute_deposit_checking(sb_tx.amount)
            }
            SmallBankTxType::TransactSavings => {
                account.execute_transact_savings(sb_tx.amount)
            }
            SmallBankTxType::WriteCheck => {
                account.execute_write_check(sb_tx.amount)
            }
            SmallBankTxType::SendPayment => {
                // TODO: implement cross-account SendPayment with state migration
                let success = account.execute_send_payment_debit(sb_tx.amount);
                if success && sb_tx.dest_account_id != sb_tx.account_id {
                    if let Some(dest) = self.account_store.get_mut(&sb_tx.dest_account_id) {
                        dest.execute_send_payment_credit(sb_tx.amount);
                    }
                    // If dest is on another executor, we'd need state transfer (future work)
                }
                success
            }
        }
    }
}
