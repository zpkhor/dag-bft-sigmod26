// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
#[cfg(feature = "benchmark")]
use log::info;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto as _;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                let digest = Digest(Sha512::digest(&batch).as_ref()[..32].try_into().unwrap());
                store.write(digest.to_vec(), batch).await;
                let message = WorkerPrimaryMessage::OthersBatch(digest, id);
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }

    /// Spawn a processor for our own batches, using the digest pre-computed in batch_maker.
    pub fn spawn_own(
        id: WorkerId,
        mut store: Store,
        mut rx_batch: Receiver<(SerializedBatchMessage, Digest)>,
        tx_digest: Sender<SerializedBatchDigestMessage>,
    ) {
        tokio::spawn(async move {
            while let Some((batch, digest)) = rx_batch.recv().await {
                let account_counts =
                    match bincode::deserialize::<crate::worker::WorkerMessage>(&batch) {
                        Ok(crate::worker::WorkerMessage::Batch((_, counts))) => counts,
                        _ => std::collections::BTreeMap::new(),
                    };

                store.write(digest.to_vec(), batch).await;

                #[cfg(feature = "benchmark")]
                info!("Processed batch {:?}", digest);

                let message = WorkerPrimaryMessage::OurBatch(digest, id, account_counts);
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}
