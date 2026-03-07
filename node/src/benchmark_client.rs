// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::Bytes;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpStream;
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
        .args_from_usage("--account-start=[INT] 'The first account_id for this client'")
        .args_from_usage("--num-accounts=[INT] 'Number of accounts for this client (default 1000000)'")
        .args_from_usage("--client-id=[INT] 'Unique client identifier (validator_index * num_workers + worker_index)'")
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

    info!("Node addresses: {:?}", targets);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    info!("Account range: {} to {} ({} accounts)", account_start, account_start + num_accounts - 1, num_accounts);

    let client = Client {
        targets,
        size,
        rate,
        nodes,
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
    account_start: u64,
    num_accounts: u64,
    client_id: u64,
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

        // Connect to all worker targets.
        let mut transports = Vec::new();
        for target in &self.targets {
            let stream = TcpStream::connect(target)
                .await
                .context(format!("failed to connect to {}", target))?;
            transports.push(Framed::new(stream, LengthDelimitedCodec::new()));
        }

        self.send_without_tcp_backpressure(transports).await
    }

    async fn send_without_tcp_backpressure(&self, transports: Vec<Framed<TcpStream, LengthDelimitedCodec>>) -> Result<()> {
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
                let w_idx = {
                    let entry = worker_rr.entry(account_id).or_insert(0usize);
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
