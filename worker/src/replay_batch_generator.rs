use crate::worker::{SerializedBatchDigestMessage, WorkerMessage};
use config::replay;
use config::{Parameters, WorkerId};
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use log::info;
use primary::WorkerPrimaryMessage;
use rand::Rng;
use rand::SeedableRng;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::Sender;

pub struct ReplayBatchGenerator;

impl ReplayBatchGenerator {
    pub fn spawn(
        id: WorkerId,
        mut store: Store,
        parameters: Parameters,
        tx_primary: Sender<SerializedBatchDigestMessage>,
    ) {
        tokio::spawn(async move {
            let entries = replay::parse_replay_csv(&parameters.replay_csv)
                .expect("Failed to parse replay CSV");
            let assignments = replay::assign_batches_round_robin(
                &entries,
                parameters.num_workers,
                parameters.replay_tx_size,
            );

            // Filter to batches assigned to this worker.
            let my_assignments: Vec<_> = assignments
                .iter()
                .filter(|a| a.worker_id == id)
                .collect();

            info!(
                "ReplayBatchGenerator worker {}: generating {} batches (of {} total)",
                id,
                my_assignments.len(),
                assignments.len(),
            );

            let tx_size = parameters.replay_tx_size;
            let num_accounts = parameters.num_accounts;
            assert!(num_accounts > 0, "num_accounts must be > 0 for replay mode");

            for (count, assignment) in my_assignments.iter().enumerate() {
                let mut rng = rand::rngs::StdRng::seed_from_u64(assignment.batch_index);
                let mut txs: Vec<Vec<u8>> = Vec::with_capacity(assignment.num_tx);

                for _ in 0..assignment.num_tx {
                    let mut tx = vec![0u8; tx_size];

                    // Standard header: [tx_type:1][client_id:1][tx_counter:8]
                    tx[0] = 1; // tx_type = 1 (regular, non-sample)
                    tx[1] = 0; // client_id = 0
                    // tx_counter = 0 (bytes 2..10, already zeroed)

                    // SmallBank payload: [src_account:8][dest_account:8][sb_tx_type:1][amount:8]
                    let src_account: u64 = rng.gen_range(0, num_accounts);
                    let sb_tx_type: u8 = rng.gen_range(0, 5);
                    let dest_account: u64 = if sb_tx_type == 4 {
                        // SendPayment: different dest
                        let mut d = rng.gen_range(0, num_accounts);
                        while d == src_account && num_accounts > 1 {
                            d = rng.gen_range(0, num_accounts);
                        }
                        d
                    } else {
                        src_account
                    };

                    let amount: f64 = match sb_tx_type {
                        1 => 1.3,   // DepositChecking
                        2 => 20.20, // TransactSavings
                        3 => 5.0,   // WriteCheck
                        4 => 5.0,   // SendPayment
                        _ => 0.0,   // Balance
                    };

                    tx[10..18].copy_from_slice(&src_account.to_be_bytes());
                    tx[18..26].copy_from_slice(&dest_account.to_be_bytes());
                    tx[26] = sb_tx_type;
                    tx[27..35].copy_from_slice(&amount.to_be_bytes());

                    txs.push(tx);
                }

                // Serialize as WorkerMessage::Batch (same format as BatchMaker/Processor)
                let serialized = bincode::serialize(&WorkerMessage::Batch(txs))
                    .expect("Failed to serialize batch");

                // Compute digest identically to Processor
                let digest = Digest(
                    Sha512::digest(&serialized).as_ref()[..32]
                        .try_into()
                        .unwrap(),
                );

                // Store the batch
                store.write(digest.to_vec(), serialized).await;

                // Notify primary
                let message = WorkerPrimaryMessage::ReplayBatchReady(
                    assignment.batch_index,
                    digest,
                    id,
                );
                let serialized_msg = bincode::serialize(&message)
                    .expect("Failed to serialize ReplayBatchReady");
                tx_primary
                    .send(serialized_msg)
                    .await
                    .expect("Failed to send ReplayBatchReady to primary");

                if (count + 1) % 500 == 0 {
                    info!(
                        "ReplayBatchGenerator worker {}: generated {}/{} batches",
                        id,
                        count + 1,
                        my_assignments.len(),
                    );
                }
            }

            info!(
                "ReplayBatchGenerator worker {}: finished generating all {} batches",
                id,
                my_assignments.len(),
            );
        });
    }
}
