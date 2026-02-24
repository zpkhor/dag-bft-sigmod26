// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
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
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .args_from_usage("--open-loop 'Use open-loop mode (no TCP backpressure)'")
        .args_from_usage("--account-start=[INT] 'The first account_id for this client'")
        .args_from_usage("--num-accounts=[INT] 'Number of accounts for this client (0 = disabled)'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
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

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    info!("Client mode: {}", if open_loop { "open-loop (no TCP backpressure)" } else { "closed-loop (with TCP backpressure)" });

    let client = Client {
        target,
        size,
        rate,
        nodes,
        open_loop,
        account_start,
        num_accounts,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
    open_loop: bool,
    account_start: u64,
    num_accounts: u64,
}

impl Client {
    pub async fn send(&self) -> Result<()> {

        if self.size < 17 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 17 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        let transport = Framed::new(stream, LengthDelimitedCodec::new());

        if self.open_loop {
            self.send_open_loop(transport).await
        } else {
            self.send_closed_loop(transport).await
        }
    }

    async fn send_open_loop(&self, transport: Framed<TcpStream, LengthDelimitedCodec>) -> Result<()> {
        const PRECISION: u64 = 20;
        const BURST_DURATION: u64 = 1000 / PRECISION;

        let (chan_tx, mut chan_rx) = mpsc::unbounded_channel::<Bytes>();

        // Background task owns the TCP transport and drains the channel.
        tokio::spawn(async move {
            let mut transport = transport;
            while let Some(bytes) = chan_rx.recv().await {
                if let Err(e) = transport.send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break;
                }
            }
        });

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
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

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();
                if let Err(e) = chan_tx.send(bytes) {
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

    async fn send_closed_loop(&self, mut transport: Framed<TcpStream, LengthDelimitedCodec>) -> Result<()> {
        const PRECISION: u64 = 20;
        const BURST_DURATION: u64 = 1000 / PRECISION;

        let burst = self.rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
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

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();
                if let Err(e) = transport.send(bytes).await {
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
