// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::Bytes;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use crypto::PublicKey;
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{info, warn};
use primary::MigrationMessage;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        // .args_from_usage("<ADDR>... 'Network addresses of this validator\\'s workers'") not supporting fab local anymore
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .args_from_usage("--account-start=[INT] 'The first account_id for this client'")
        .args_from_usage("--num-accounts=[INT] 'Number of accounts for this client (default 1000000)'")
        .args_from_usage("--client-id=[INT] 'Unique client identifier (validator_index * num_workers + worker_index)'")
        .args_from_usage("--validator-workers=<PAIR>... 'PUBKEY_BASE64:addr1+addr2 per validator'")
        .args_from_usage("--reply-addr=[ADDR] 'Address to listen for migration notices'")
        .args_from_usage("--own-validator=<KEY> 'Base64 public key of this client\\'s home validator'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;
    let account_start = matches
        .value_of("account-start")
        .unwrap_or("0")
        .parse::<u64>()
        .context("account-start must be a non-negative integer")?;
    let num_accounts = matches
        .value_of("num-accounts")
        .unwrap_or("1000000")
        .parse::<u64>()
        .context("num-accounts must be a non-negative integer")?;
    let client_id = matches
        .value_of("client-id")
        .unwrap_or("0")
        .parse::<u64>()
        .context("client-id must be a non-negative integer")?;

    // Parse --validator-workers: each value is "PUBKEY_BASE64:addr1+addr2"
    // Note: '+' separator because clap treats ',' as a value delimiter.
    let mut all_validators: HashMap<PublicKey, Vec<SocketAddr>> = HashMap::new();
    if let Some(pairs) = matches.values_of("validator-workers") {
        for pair in pairs {
            let (pk_str, addrs_str) = pair.split_once(':')
                .context(format!("Invalid --validator-workers format '{}', expected PUBKEY:addr1+addr2", pair))?;
            let pk = PublicKey::decode_base64(pk_str)
                .context(format!("Invalid public key in --validator-workers: {}", pk_str))?;
            let addrs: Vec<SocketAddr> = addrs_str.split('+')
                .map(|a| a.parse::<SocketAddr>())
                .collect::<Result<Vec<_>, _>>()
                .context(format!("Invalid address in --validator-workers: {}", addrs_str))?;
            all_validators.insert(pk, addrs);
        }
    }

    let reply_addr: Option<SocketAddr> = matches
        .value_of("reply-addr")
        .map(|a| a.parse().context("Invalid --reply-addr"))
        .transpose()?;

    let own_validator: PublicKey = PublicKey::decode_base64(
        matches.value_of("own-validator").unwrap()
    ).context("Invalid --own-validator public key")?;

    assert!(
        all_validators.contains_key(&own_validator),
        "--own-validator key must appear in --validator-workers"
    );

    let num_validators = all_validators.len();
    let num_workers = all_validators[&own_validator].len();
    assert!(num_workers > 0, "Validators must have at least one worker");
    for (pk, addrs) in &all_validators {
        assert!(
            addrs.len() == num_workers,
            "All validators must have the same number of workers, but {} has {} (expected {})",
            pk, addrs.len(), num_workers
        );
    }

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    info!("Account range: {} to {} ({} accounts)", account_start, account_start + num_accounts - 1, num_accounts);

    info!("{} validators, {} workers each, own={}", num_validators, num_workers, own_validator);

    let f_plus_one = (num_validators - 1) / 3 + 1;

    let client = Client {
        all_validators,
        size,
        rate,
        nodes,
        account_start,
        num_accounts,
        client_id,
        reply_addr,
        own_validator,
        f_plus_one,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    all_validators: HashMap<PublicKey, Vec<SocketAddr>>,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
    account_start: u64,
    num_accounts: u64,
    client_id: u64,
    reply_addr: Option<SocketAddr>,
    own_validator: PublicKey,
    f_plus_one: usize,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        if self.size < 17 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 17 bytes (8 prefix + 1 type + 8 counter)",
            ));
        }
        if self.num_accounts == 0 {
            return Err(anyhow::Error::msg(
                "num-accounts must be greater than 0",
            ));
        }

        // Connect to all validators' workers.
        let mut validator_senders: HashMap<PublicKey, Vec<mpsc::UnboundedSender<Bytes>>> = HashMap::new();
        for (pk, addrs) in &self.all_validators {
            let mut senders = Vec::new();
            for addr in addrs {
                let stream = TcpStream::connect(addr)
                    .await
                    .context(format!("failed to connect to validator worker {}", addr))?;
                let transport = Framed::new(stream, LengthDelimitedCodec::new());
                let (chan_tx, mut chan_rx) = mpsc::unbounded_channel::<Bytes>();
                tokio::spawn(async move {
                    let mut transport = transport;
                    while let Some(bytes) = chan_rx.recv().await {
                        if let Err(e) = transport.send(bytes).await {
                            warn!("Failed to send transaction to validator worker: {}", e);
                            break;
                        }
                    }
                });
                senders.push(chan_tx);
            }
            validator_senders.insert(*pk, senders);
        }

        // Routing table: account_id -> target validator pk.
        // Accounts not in the table go to own validator (default).
        let routing_table: Arc<RwLock<HashMap<u64, PublicKey>>> = Arc::new(RwLock::new(HashMap::new()));

        // Spawn migration notice listener if configured.
        if let Some(reply_addr) = self.reply_addr {
            let rt = routing_table.clone();
            let f_plus_one = self.f_plus_one;
            tokio::spawn(async move {
                if let Err(e) = migration_listener(reply_addr, rt, f_plus_one).await {
                    warn!("Migration listener error: {}", e);
                }
            });
        }

        self.send_with_routing(validator_senders, routing_table).await
    }

    async fn send_with_routing(
        &self,
        validator_senders: HashMap<PublicKey, Vec<mpsc::UnboundedSender<Bytes>>>,
        routing_table: Arc<RwLock<HashMap<u64, PublicKey>>>,
    ) -> Result<()> {
        const PRECISION: u64 = 20;
        const BURST_DURATION: u64 = 1000 / PRECISION;

        let own_validator = self.own_validator;
        let own_senders = &validator_senders[&own_validator];
        let num_workers = own_senders.len();
        let burst = self.rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0u64;
        let mut rng = rand::thread_rng();
        let mut r = rng.gen();
        let mut worker_rr: HashMap<u64, usize> = HashMap::new();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                let account_id = self.account_start + rng.gen_range(0, self.num_accounts);

                // Check routing table for this account
                let target_pk = routing_table.read().unwrap().get(&account_id).copied();
                let senders = match target_pk {
                    Some(pk) if pk != own_validator => {
                        validator_senders.get(&pk).unwrap_or(own_senders)
                    }
                    _ => own_senders,
                };

                let w_idx = {
                    let entry = worker_rr.entry(account_id).or_insert((account_id as usize) % num_workers);
                    let w = *entry;
                    *entry = (w + 1) % num_workers;
                    w
                };

                tx.put_u64(account_id); // bytes 0-7: account_id prefix
                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {} account {} client {}", counter, account_id, self.client_id);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(counter); // This counter identifies the tx.
                } else {
                    r += 1;
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                };

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();
                if let Err(e) = senders[w_idx].send(bytes) {
                    warn!("Failed to queue transaction: {}", e);
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}

/// Listen for migration notices from validators, track f+1 matching, update routing table.
async fn migration_listener(
    addr: SocketAddr,
    routing_table: Arc<RwLock<HashMap<u64, PublicKey>>>,
    f_plus_one: usize,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await
        .context(format!("Failed to bind migration listener on {}", addr))?;
    info!("Migration listener started on {}", addr);

    // Track votes per (account_id, new_target): set of sender validators
    let votes: Arc<RwLock<HashMap<(u64, PublicKey), HashSet<PublicKey>>>> =
        Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (stream, peer) = listener.accept().await
            .context("Failed to accept migration connection")?;
        let votes = votes.clone();
        let rt = routing_table.clone();

        tokio::spawn(async move {
            let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
            while let Some(result) = framed.next().await {
                match result {
                    Ok(data) => {
                        match bincode::deserialize::<MigrationMessage>(&data) {
                            Ok(msg) => {
                                info!(
                                    "Received {} migration notices from validator {} via {}",
                                    msg.notices.len(), msg.sender, peer
                                );
                                let mut confirmed = Vec::new();
                                {
                                    let mut votes = votes.write().unwrap();
                                    for notice in &msg.notices {
                                        let key = (notice.account_id, notice.new_target);
                                        let entry = votes.entry(key).or_default();
                                        entry.insert(msg.sender);
                                        if entry.len() >= f_plus_one {
                                            confirmed.push((notice.account_id, notice.new_target));
                                        }
                                    }
                                    // Clean up confirmed entries atomically to avoid TOCTOU with vote accumulation
                                    for &(account_id, new_target) in &confirmed {
                                        votes.remove(&(account_id, new_target));
                                    }
                                }
                                if !confirmed.is_empty() {
                                    let mut rt = rt.write().unwrap();
                                    for (account_id, new_target) in &confirmed {
                                        rt.insert(*account_id, *new_target);
                                    }
                                    info!(
                                        "Migration confirmed: {} accounts rerouted (total routed: {})",
                                        confirmed.len(), rt.len()
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Failed to deserialize migration message from {}: {}", peer, e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Migration listener read error from {}: {}", peer, e);
                        break;
                    }
                }
            }
        });
    }
}
