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
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
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
        .args_from_usage("--account-ranges=<PAIR>... 'PUBKEY_BASE64:start:count per validator'")
        .args_from_usage("--rate-weights=[WEIGHTS] 'Comma-separated rate weights per region (default: equal)'")
        .args_from_usage("--validator-workers=<PAIR>... 'PUBKEY_BASE64:addr1+addr2 per validator'")
        .args_from_usage("--reply-addr=[ADDR] 'Address to listen for migration notices'")
        .args_from_usage("--own-validator=<KEY> 'Base64 public key of this client\\'s home validator'")
        .args_from_usage("--round-robin 'Enable round-robin routing across all validators'")
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

    // Parse --account-ranges: each value is "PUBKEY_BASE64:start:count"
    let mut account_ranges: HashMap<PublicKey, (u64, u64)> = HashMap::new();
    if let Some(pairs) = matches.values_of("account-ranges") {
        for pair in pairs {
            let parts: Vec<&str> = pair.splitn(3, ':').collect();
            assert!(
                parts.len() == 3,
                "Invalid --account-ranges format '{}', expected PUBKEY:start:count", pair
            );
            let pk = PublicKey::decode_base64(parts[0])
                .context(format!("Invalid public key in --account-ranges: {}", parts[0]))?;
            let start = parts[1].parse::<u64>()
                .context(format!("Invalid start in --account-ranges: {}", parts[1]))?;
            let count = parts[2].parse::<u64>()
                .context(format!("Invalid count in --account-ranges: {}", parts[2]))?;
            account_ranges.insert(pk, (start, count));
        }
    }

    // Parse --rate-weights: comma-separated floats
    let rate_weights: Vec<f64> = matches
        .value_of("rate-weights")
        .map(|s| {
            s.split(',')
                .map(|w| w.parse::<f64>().expect("rate-weights must be numbers"))
                .collect()
        })
        .unwrap_or_default();

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

    let round_robin = matches.is_present("round-robin");

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

    // Validate account_ranges covers all validators
    for pk in all_validators.keys() {
        assert!(
            account_ranges.contains_key(pk),
            "--account-ranges must include entry for validator {}",
            pk
        );
    }

    // Default rate_weights to equal if empty
    let rate_weights = if rate_weights.is_empty() {
        vec![1.0f64; num_validators]
    } else {
        assert!(
            rate_weights.len() == num_validators,
            "rate-weights length ({}) must match number of validators ({})",
            rate_weights.len(), num_validators
        );
        rate_weights
    };

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    let sorted_validators: Vec<PublicKey> = {
        let mut keys: Vec<PublicKey> = all_validators.keys().copied().collect();
        keys.sort();
        keys
    };

    for (i, pk) in sorted_validators.iter().enumerate() {
        let (start, count) = account_ranges[pk];
        info!(
            "Region {}: accounts {}..{} ({} accounts), rate_weight={}",
            i, start, start + count - 1, count, rate_weights[i]
        );
    }

    info!("{} validators, {} workers each, own={}", num_validators, num_workers, own_validator);

    let f_plus_one = (num_validators - 1) / 3 + 1;

    if round_robin {
        info!("Round-robin routing enabled across {} validators", num_validators);
    }

    let client = Client {
        all_validators,
        sorted_validators,
        size,
        rate,
        nodes,
        account_ranges,
        rate_weights,
        reply_addr,
        f_plus_one,
        round_robin,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    all_validators: HashMap<PublicKey, Vec<SocketAddr>>,
    sorted_validators: Vec<PublicKey>,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
    account_ranges: HashMap<PublicKey, (u64, u64)>,
    rate_weights: Vec<f64>,
    reply_addr: Option<SocketAddr>,
    f_plus_one: usize,
    round_robin: bool,
}

impl Client {
    async fn connect_all_validators(&self, region_id: usize) -> Result<HashMap<PublicKey, Vec<mpsc::UnboundedSender<Bytes>>>> {
        let num_validators = self.sorted_validators.len();
        let mut validator_senders: HashMap<PublicKey, Vec<mpsc::UnboundedSender<Bytes>>> = HashMap::new();
        for (v_idx, pk) in self.sorted_validators.iter().enumerate() {
            let addrs = &self.all_validators[pk];
            let mut senders = Vec::new();
            for addr in addrs {
                let stream = TcpStream::connect(addr)
                    .await
                    .context(format!("failed to connect to validator worker {}", addr))?;

                // Set SO_MARK for per-region per-validator TC classification
                let fd = stream.as_raw_fd();
                let mark: u32 = (region_id * num_validators + v_idx + 1) as u32;
                unsafe {
                    let ret = libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_MARK,
                        &mark as *const _ as *const libc::c_void,
                        std::mem::size_of::<u32>() as libc::socklen_t,
                    );
                    assert!(
                        ret == 0,
                        "setsockopt SO_MARK failed: {}",
                        std::io::Error::last_os_error()
                    );
                }

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
        Ok(validator_senders)
    }

    pub async fn send(&self) -> Result<()> {
        if self.size < 17 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 17 bytes (8 prefix + 1 type + 8 counter)",
            ));
        }

        // Routing table: account_id -> target validator pk (shared across all senders).
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

        let num_regions = self.sorted_validators.len();
        let total_weight: f64 = self.rate_weights.iter().sum();

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");
        info!("Spawning {} sender tasks (1 per region)", num_regions);

        const PRECISION: u64 = 20;
        const BURST_DURATION: u64 = 1000 / PRECISION;

        let mut base_region_rate_sum: u64 = 0;
        let mut handles = Vec::new();
        for (region_id, &region_pk) in self.sorted_validators.iter().enumerate() {
            let (acct_start, acct_count) = self.account_ranges[&region_pk];
            assert!(acct_count > 0, "account count for region {} must be > 0", region_id);

            // Last region gets remainder to ensure exact sum = self.rate
            let region_rate = if region_id == num_regions - 1 {
                self.rate - base_region_rate_sum
            } else {
                (self.rate as f64 * self.rate_weights[region_id] / total_weight) as u64
            };
            base_region_rate_sum += region_rate;

            let validator_senders = self.connect_all_validators(region_id).await?;
            let rt = routing_table.clone();
            let round_robin = self.round_robin;
            let size = self.size;
            let stagger_ms = BURST_DURATION * region_id as u64 / num_regions as u64;

            handles.push(tokio::spawn(async move {
                sleep(Duration::from_millis(stagger_ms)).await;
                send_shard(
                    validator_senders,
                    rt,
                    region_pk,
                    round_robin,
                    acct_start,
                    acct_count,
                    region_rate,
                    size,
                ).await
            }));
        }

        for h in handles {
            h.await??;
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

async fn send_shard(
    validator_senders: HashMap<PublicKey, Vec<mpsc::UnboundedSender<Bytes>>>,
    routing_table: Arc<RwLock<HashMap<u64, PublicKey>>>,
    own_validator: PublicKey,
    round_robin: bool,
    account_start: u64,
    num_accounts: u64,
    rate: u64,
    size: usize,
) -> Result<()> {
    const PRECISION: u64 = 20;
    const BURST_DURATION: u64 = 1000 / PRECISION;

    let own_senders = &validator_senders[&own_validator];
    let num_workers = own_senders.len();
    let mut tx = BytesMut::with_capacity(size);
    let mut counter = 0u64;
    let mut rng = StdRng::from_entropy();
    let mut r: u64 = rng.gen();
    let mut worker_rr: HashMap<u64, usize> = HashMap::new();
    let interval = interval(Duration::from_millis(BURST_DURATION));
    tokio::pin!(interval);
    // Cumulative send count to avoid truncation loss from rate/PRECISION.
    // On tick `counter`, send exactly `rate*(counter+1)/PRECISION - rate*counter/PRECISION` txs.
    let mut total_sent = 0u64;

    // For round-robin mode: sorted validator keys and per-account validator counter
    let sorted_validators: Vec<PublicKey> = {
        let mut keys: Vec<PublicKey> = validator_senders.keys().copied().collect();
        keys.sort();
        keys
    };
    let num_validators = sorted_validators.len();
    let mut validator_rr: HashMap<u64, usize> = HashMap::new();

    'main: loop {
        interval.as_mut().tick().await;
        let now = Instant::now();

        let next_total = rate * (counter + 1) / PRECISION;
        let burst = next_total - total_sent;

        for x in 0..burst {
            let account_id = account_start + rng.gen_range(0, num_accounts);

            let senders = if round_robin {
                let v_idx = validator_rr.entry(account_id)
                    .or_insert((account_id as usize) % num_validators);
                let target_pk = sorted_validators[*v_idx];
                *v_idx = (*v_idx + 1) % num_validators;
                &validator_senders[&target_pk]
            } else {
                // Check routing table for this account
                let target_pk = routing_table.read().unwrap().get(&account_id).copied();
                match target_pk {
                    Some(pk) if pk != own_validator => {
                        validator_senders.get(&pk).unwrap_or(own_senders)
                    }
                    _ => own_senders,
                }
            };

            let w_idx = {
                let entry = worker_rr.entry(account_id).or_insert((account_id as usize) % num_workers);
                let w = *entry;
                *entry = (w + 1) % num_workers;
                w
            };

            tx.put_u64(account_id); // bytes 0-7: account_id prefix
            if burst > 0 && x == counter % burst {
                // NOTE: This log entry is used to compute performance.
                info!("Sending sample transaction {} account {}", counter, account_id);

                tx.put_u8(0u8); // Sample txs start with 0.
                tx.put_u64(counter); // This counter identifies the tx.
            } else {
                r += 1;
                tx.put_u8(1u8); // Standard txs start with 1.
                tx.put_u64(r); // Ensures all clients send different txs.
            };

            tx.resize(size, 0u8);
            let bytes = tx.split().freeze();
            if let Err(e) = senders[w_idx].send(bytes) {
                warn!("Failed to queue transaction: {}", e);
                break 'main;
            }
        }
        total_sent = next_total;
        if now.elapsed().as_millis() > BURST_DURATION as u128 {
            // NOTE: This log entry is used to compute performance.
            warn!("Transaction rate too high for this client");
        }
        counter += 1;
    }
    Ok(())
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
