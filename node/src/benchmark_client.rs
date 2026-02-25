// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{info, warn};
use rand::Rng;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR>... 'Network addresses of this validator\\'s workers'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .args_from_usage("--open-loop 'Use open-loop mode (no TCP backpressure)'")
        .args_from_usage("--account-start=[INT] 'The first account_id for this client'")
        .args_from_usage("--num-accounts=[INT] 'Number of accounts for this client (0 = disabled)'")
        .args_from_usage("--client-id=[INT] 'Unique client identifier (validator_index * num_workers + worker_index)'")
        .args_from_usage("--reply-port=[INT] 'Port to listen for commit replies'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let targets = matches
        .values_of("ADDR")
        .unwrap()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;
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
    let open_loop = matches.is_present("open-loop");
    let account_start = matches
        .value_of("account-start")
        .unwrap_or("0")
        .parse::<u64>()
        .context("account-start must be a non-negative integer")?;
    let num_accounts = matches
        .value_of("num-accounts")
        .unwrap_or("0")
        .parse::<u64>()
        .context("num-accounts must be a non-negative integer")?;
    let client_id = matches
        .value_of("client-id")
        .unwrap_or("0")
        .parse::<u64>()
        .context("client-id must be a non-negative integer")?;
    let reply_port = matches
        .value_of("reply-port")
        .map(|v| v.parse::<u16>())
        .transpose()
        .context("reply-port must be a valid port number")?;

    info!("Node addresses: {:?}", targets);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    info!("Client mode: {}", if open_loop { "open-loop (no TCP backpressure)" } else { "closed-loop (with TCP backpressure)" });

    // Spawn reply listener if reply_port is set.
    let seen_replies: Arc<Mutex<HashMap<u64, (u64, Vec<u8>)>>> =
        Arc::new(Mutex::new(HashMap::new()));
    if let Some(port) = reply_port {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        let seen = Arc::clone(&seen_replies);
        tokio::spawn(async move {
            listen_for_replies(addr, seen).await;
        });
    }

    let client = Client {
        targets,
        size,
        rate,
        nodes,
        open_loop,
        account_start,
        num_accounts,
        client_id,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    targets: Vec<SocketAddr>,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
    open_loop: bool,
    account_start: u64,
    num_accounts: u64,
    client_id: u64,
}

impl Client {
    pub async fn send(&self) -> Result<()> {

        if self.size < 25 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 25 bytes",
            ));
        }

        // Connect to all worker targets.
        let mut transports = Vec::new();
        for target in &self.targets {
            let stream = TcpStream::connect(target)
                .await
                .context(format!("failed to connect to {}", target))?;
            transports.push(Framed::new(stream, LengthDelimitedCodec::new()));
        }

        if self.open_loop {
            self.send_open_loop(transports).await
        } else {
            self.send_closed_loop(transports).await
        }
    }

    async fn send_open_loop(&self, transports: Vec<Framed<TcpStream, LengthDelimitedCodec>>) -> Result<()> {
        const PRECISION: u64 = 20;
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // Spawn one drain task per transport, collect the senders.
        let mut senders = Vec::new();
        for transport in transports {
            let (chan_tx, mut chan_rx) = mpsc::unbounded_channel::<Bytes>();
            tokio::spawn(async move {
                let mut transport = transport;
                while let Some(bytes) = chan_rx.recv().await {
                    if let Err(e) = transport.send(bytes).await {
                        warn!("Failed to send transaction: {}", e);
                        break;
                    }
                }
            });
            senders.push(chan_tx);
        }

        let num_workers = senders.len();
        let burst = self.rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0u64;
        let mut r = rand::thread_rng().gen();
        let mut account_rr: HashMap<u64, usize> = HashMap::new();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                let account_id = if self.num_accounts > 0 {
                    self.account_start + (counter % self.num_accounts) // TODO: use random account_id
                } else {
                    0
                };
                let worker = {
                    let entry = account_rr.entry(account_id).or_insert(0usize);
                    let w = *entry;
                    *entry = (w + 1) % num_workers;
                    w
                };
                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {} account {}", counter, account_id);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(counter); // This counter identifies the tx.
                } else {
                    r += 1;
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                };
                tx.put_u64(account_id);
                tx.put_u64(self.client_id);

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();
                if let Err(e) = senders[worker].send(bytes) {
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

    async fn send_closed_loop(&self, mut transports: Vec<Framed<TcpStream, LengthDelimitedCodec>>) -> Result<()> {
        const PRECISION: u64 = 20;
        const BURST_DURATION: u64 = 1000 / PRECISION;

        let num_workers = transports.len();
        let burst = self.rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0u64;
        let mut r = rand::thread_rng().gen();
        let mut account_rr: HashMap<u64, usize> = HashMap::new();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        info!("Start sending transactions");

        loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                let account_id = if self.num_accounts > 0 {
                    self.account_start + (counter % self.num_accounts)
                } else {
                    0
                };
                let worker = {
                    let entry = account_rr.entry(account_id).or_insert(0usize);
                    let w = *entry;
                    *entry = (w + 1) % num_workers;
                    w
                };
                if x == counter % burst {
                    info!("Sending sample transaction {} account {}", counter, account_id);

                    tx.put_u8(0u8);
                    tx.put_u64(counter);
                } else {
                    r += 1;
                    tx.put_u8(1u8);
                    tx.put_u64(r);
                };
                tx.put_u64(account_id);
                tx.put_u64(self.client_id);

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();
                if let Err(e) = transports[worker].send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
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

/// Listen for commit replies from workers and log them.
async fn listen_for_replies(addr: SocketAddr, seen_replies: Arc<Mutex<HashMap<u64, (u64, Vec<u8>)>>>) {
    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind reply listener");
    info!("Listening for commit replies on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                info!("Reply connection from {}", peer);
                let seen = Arc::clone(&seen_replies);
                tokio::spawn(async move {
                    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
                    while let Some(Ok(frame)) = transport.next().await {
                        if let Ok((tx_type, counter, account_id, digest, name, worker_id)) = parse_reply(&frame) {
                            if tx_type != 0 {
                                continue;
                            }
                            {
                                let mut seen_map = seen.lock().unwrap();
                                if let Some((prev_acct, prev_digest)) = seen_map.get(&counter) {
                                    if *prev_acct != account_id || *prev_digest != digest {
                                        warn!(
                                            "Reply mismatch for tx {}: account {}/{}, digest {:?}/{:?}",
                                            counter, prev_acct, account_id, prev_digest, digest
                                        );
                                    }
                                } else {
                                    seen_map.insert(counter, (account_id, digest));
                                }
                            }
                            // NOTE: This log entry is used to compute performance.
                            info!(
                                "Received reply for tx {} account {} from validator {:?} worker {}",
                                counter, account_id, name, worker_id,
                            );
                        }
                    }
                });
            }
            Err(e) => {
                warn!("Failed to accept reply connection: {}", e);
            }
        }
    }
}

fn parse_reply(data: &[u8]) -> Result<(u8, u64, u64, Vec<u8>, crypto::PublicKey, config::WorkerId)> {
    // Deserialize CommitReply using bincode (matches worker's serialization).
    #[derive(serde::Deserialize)]
    struct CommitReply {
        counter: u64,
        account_id: u64,
        #[allow(dead_code)]
        client_id: u64,
        tx_type: u8,
        digest: crypto::Digest,
        name: crypto::PublicKey,
        worker_id: config::WorkerId,
    }
    let reply: CommitReply =
        bincode::deserialize(data).context("Failed to deserialize commit reply")?;
    Ok((reply.tx_type, reply.counter, reply.account_id, reply.digest.to_vec(), reply.name, reply.worker_id))
}
