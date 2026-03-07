// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, WorkerId};
use consensus::Consensus;
use crypto::PublicKey;
use env_logger::Env;
use network::SimpleSender;
use primary::{Certificate, Primary, PrimaryWorkerMessage};
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use worker::Worker;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => KeyPair::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let name = keypair.name;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // Make the data store.
    let store = Store::new(store_path).context("Failed to create a store")?;

    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Check whether to run a primary, a worker, or an entire authority.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        ("primary", _) => {
            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            Primary::spawn(
                keypair,
                committee.clone(),
                parameters.clone(),
                store,
                tx_new_certificates,
                rx_feedback,
            );
            let analyze_committee = committee.clone();
            Consensus::spawn(
                committee,
                parameters.gc_depth,
                rx_new_certificates,
                tx_feedback,
                tx_output,
            );
            analyze(rx_output, analyze_committee, name).await;
        }

        // Spawn a single worker.
        ("worker", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            Worker::spawn(name, id, committee, parameters, store);
        }
        _ => unreachable!(),
    }

    // For workers, keep the process alive (primary path awaits analyze() forever).
    std::future::pending::<()>().await;
    unreachable!();
}

/// Receives an ordered list of certificates and dispatches committed batch digests to our workers.
async fn analyze(mut rx_output: Receiver<Certificate>, committee: Committee, name: PublicKey) {
    let mut network = SimpleSender::new();

    // Build a map from worker_id -> our worker's primary_to_worker address.
    let our_workers: HashMap<WorkerId, _> = committee
        .authorities
        .get(&name)
        .expect("Our key is not in the committee")
        .workers
        .iter()
        .map(|(id, addr)| (*id, addr.primary_to_worker))
        .collect();

    while let Some(certificate) = rx_output.recv().await {
        // Group committed batch digests by worker_id.
        let mut per_worker: HashMap<WorkerId, Vec<_>> = HashMap::new();
        for (digest, worker_id) in &certificate.header.payload {
            per_worker
                .entry(*worker_id)
                .or_default()
                .push(digest.clone());
        }

        // Send CommittedBatches to each of our workers.
        for (worker_id, digests) in per_worker {
            if let Some(&address) = our_workers.get(&worker_id) {
                let message = PrimaryWorkerMessage::CommittedBatches(digests);
                let bytes = bincode::serialize(&message)
                    .expect("Failed to serialize CommittedBatches");
                network.send(address, Bytes::from(bytes)).await;
            }
        }
    }
}
