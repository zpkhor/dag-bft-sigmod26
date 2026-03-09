// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::Digest;
use crypto::PublicKey;
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
use std::collections::BTreeMap;
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = (Vec<Transaction>, BTreeMap<u64, u64>);

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<(u64, Transaction)>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch as (account_id, payload) pairs.
    current_batch: Vec<(u64, Transaction)>,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<(u64, Transaction)>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Vec::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some((account_id, tx)) = self.rx_transaction.recv() => {
                    self.current_batch_size += (tx.len() + 8) as usize; // 8 bytes for the account_id
                    self.current_batch.push((account_id, tx));
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Drain accumulator into (account_id, payload) pairs.
        self.current_batch_size = 0;
        let pairs: Vec<(u64, Transaction)> = self.current_batch.drain(..).collect();

        // Build per-account tx counts (deterministic order via BTreeMap).
        let mut account_counts: BTreeMap<u64, u64> = BTreeMap::new();
        for (account_id, _) in &pairs {
            *account_counts.entry(*account_id).or_insert(0) += 1;
        }

        // Extract payloads only for the batch.
        let batch_txs: Vec<Transaction> = pairs.iter().map(|(_, tx)| tx.clone()).collect();

        #[cfg(feature = "benchmark")]
        // Look for sample txs (type byte 0) and gather their counter and account_id.
        let sample_ids: Vec<_> = pairs
            .iter()
            .filter(|(_, tx)| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|(account_id, tx)| {
                let counter: [u8; 8] = tx[1..9].try_into().ok()?;
                Some((counter, *account_id))
            })
            .collect();

        let message = WorkerMessage::Batch((batch_txs, account_counts));
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        let digest = Digest(
            Sha512::digest(&serialized).as_ref()[..32]
                .try_into()
                .unwrap(),
        );

        #[cfg(feature = "benchmark")]
        {
            for (id, account_id) in sample_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {} account {}",
                    digest,
                    u64::from_be_bytes(id),
                    account_id,
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                digest,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}
