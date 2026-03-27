// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, ConsensusOutput, MigrationNotice, QuorumMetrics, Round};
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// How many rounds of account history and TPS data to retain for tracking.
const TRACKING_WINDOW: Round = 30;
/// How often (in rounds) to evaluate rerouting decisions.
/// Must be >= TRACKING_WINDOW so post-migration data is reflected before re-evaluating.
const REROUTE_INTERVAL: Round = 60;
const _: () = assert!(REROUTE_INTERVAL >= TRACKING_WINDOW);

struct ValidatorThroughputTracker {
    /// Per-round, per-validator (created_at_ms, tx_count).
    rounds: HashMap<Round, HashMap<PublicKey, (u64, u64)>>,
    /// Per-round, per-validator raw QuorumMetrics (one per batch).
    quorum_stats: HashMap<Round, HashMap<PublicKey, Vec<QuorumMetrics>>>,
    window_rounds: Round,
}

impl ValidatorThroughputTracker {
    fn new(window_rounds: Round) -> Self {
        Self {
            rounds: HashMap::new(),
            quorum_stats: HashMap::new(),
            window_rounds,
        }
    }

    fn record(&mut self, certificate: &Certificate) {
        let origin = certificate.origin();
        let tx_count: u64 = certificate.header.account_counts.values().sum();
        let created_at = certificate.header.created_at;
        let round = certificate.round();

        self.rounds.entry(round).or_default().insert(origin, (created_at, tx_count));

        let qm_list: Vec<QuorumMetrics> = certificate.header.quorum_metrics.values().cloned().collect();
        if !qm_list.is_empty() {
            self.quorum_stats.entry(round).or_default().insert(origin, qm_list);
        }
    }

    /// Evict entries below min_round.
    fn evict(&mut self, min_round: Round) {
        self.rounds.retain(|&r, _| r >= min_round);
        self.quorum_stats.retain(|&r, _| r >= min_round);
    }

    /// Average queue_delay_ms per validator over [stable_round - window, stable_round].
    fn avg_queue_delay(&self, stable_round: Round) -> HashMap<PublicKey, f64> {
        let metrics = self.get_quorum_metrics(stable_round);
        metrics.into_iter()
            .filter(|(_, ms)| !ms.is_empty())
            .map(|(pk, ms)| {
                let sum: u64 = ms.iter().map(|m| m.queue_delay_ms).sum();
                (pk, sum as f64 / ms.len() as f64)
            })
            .collect()
    }

    /// Collect raw QuorumMetrics per validator over [stable_round - window, stable_round].
    fn get_quorum_metrics(&self, stable_round: Round) -> HashMap<PublicKey, Vec<QuorumMetrics>> {
        let min_round = stable_round.saturating_sub(self.window_rounds);
        let mut result: HashMap<PublicKey, Vec<QuorumMetrics>> = HashMap::new();
        for (&round, validators) in &self.quorum_stats {
            if round < min_round || round > stable_round {
                continue;
            }
            for (pk, metrics) in validators {
                result.entry(*pk).or_default().extend(metrics.iter().cloned());
            }
        }
        result
    }

    fn median_ts(validators: &HashMap<PublicKey, (u64, u64)>) -> u64 {
        let mut timestamps: Vec<u64> = validators.values().map(|(ts, _)| *ts).collect();
        assert!(!timestamps.is_empty(), "median_ts called on empty round");
        timestamps.sort_unstable();
        let n = timestamps.len();
        if n % 2 == 1 {
            timestamps[n / 2]
        } else {
            (timestamps[n / 2 - 1] + timestamps[n / 2]) / 2
        }
    }

    /// Compute per-validator TPS using certificates in [stable_round - window, stable_round].
    /// Duration is median(ts @ stable_round) - median(ts @ min_round).
    fn get_tps(&self, stable_round: Round) -> Option<HashMap<PublicKey, f64>> {
        let min_round = stable_round.saturating_sub(self.window_rounds);
        let mut totals: HashMap<PublicKey, u64> = HashMap::new();
        for (&round, validators) in &self.rounds {
            if round < min_round || round > stable_round {
                continue;
            }
            for (pk, &(_ts, tx_count)) in validators {
                *totals.entry(*pk).or_insert(0) += tx_count;
            }
        }
        if totals.is_empty() {
            return None;
        }

        // TODO: formally prove that first/last rounds always have certificates
        let start_ts = Self::median_ts(
            self.rounds.get(&min_round).expect("no certificates in first round of window")
        );
        let end_ts = Self::median_ts(
            self.rounds.get(&stable_round).expect("no certificates in last round of window")
        );

        let duration_ms = end_ts.saturating_sub(start_ts);
        if duration_ms == 0 {
            return None;
        }
        let duration_secs = duration_ms as f64 / 1000.0;
        Some(
            totals
                .iter()
                .map(|(pk, count)| (*pk, *count as f64 / duration_secs))
                .collect(),
        )
    }

    fn log(&self, round: Round, stable_round: Round, capacities: &HashMap<PublicKey, u64>) {
        if let Some(tps) = self.get_tps(stable_round) {
            let mut entries: Vec<_> = tps.iter().collect();
            entries.sort_by_key(|(pk, _)| **pk);
            for (pk, rate) in entries {
                let cap = capacities.get(pk).copied().unwrap_or(0);
                info!(
                    "certified_tps (round={}) validator {}: {:.1} tx/s (capacity: {} tx/s, spare: {:.1})",
                    round, pk, rate, cap, cap as f64 - rate
                );
            }
        }
    }
}

/// Compute rerouting decisions: which donors should shed client load to which receivers.
///
/// Donors are identified by: queue_delay >> median (congested).
/// Quorum-limited safety valve: bail if >f validators have high quorum_latency.
/// Shed target per donor: max(donor_tps * 10%, donor_tps - capacity * 75%).
/// Per-account load derived from actual certificate account_counts (heaviest first).
/// Receivers selected per-account by lowest client-to-receiver latency.
fn compute_rerouting(
    tps: &HashMap<PublicKey, f64>,
    capacities: &HashMap<PublicKey, u64>,
    quorum_metrics: &HashMap<PublicKey, Vec<QuorumMetrics>>,
    account_latency: &HashMap<u64, BTreeMap<PublicKey, u64>>,
    per_validator_account_counts: &HashMap<PublicKey, HashMap<u64, u64>>,
    sorted_keys: &[PublicKey],
    current_assignments: &mut Vec<HashSet<u64>>,
    f: usize,
) -> Vec<MigrationNotice> {
    let n = sorted_keys.len();
    if n < 2 {
        return vec![];
    }

    // Step 1: Compute avg_queue_delay and avg_quorum_latency from raw metrics
    let avg_queue_delay: HashMap<PublicKey, f64> = quorum_metrics.iter()
        .filter(|(_, ms)| !ms.is_empty())
        .map(|(pk, ms)| {
            let sum: u64 = ms.iter().map(|m| m.queue_delay_ms).sum();
            (*pk, sum as f64 / ms.len() as f64)
        })
        .collect();
    let avg_quorum_latency: HashMap<PublicKey, f64> = quorum_metrics.iter()
        .filter(|(_, ms)| !ms.is_empty())
        .map(|(pk, ms)| {
            let sum: u64 = ms.iter().map(|m| m.quorum_latency_ms).sum();
            (*pk, sum as f64 / ms.len() as f64)
        })
        .collect();

    if avg_queue_delay.is_empty() {
        info!("reroute: avg_queue_delay is empty, no quorum metrics available");
        return vec![];
    }

    for pk in sorted_keys {
        info!(
            "reroute_detail: validator {} avg_qd={:.1}ms avg_ql={:.1}ms qm_samples={}",
            pk,
            avg_queue_delay.get(pk).copied().unwrap_or(0.0),
            avg_quorum_latency.get(pk).copied().unwrap_or(0.0),
            quorum_metrics.get(pk).map(|v| v.len()).unwrap_or(0),
        );
    }

    // Step 2: Compute medians
    let median_delay = {
        let mut delays: Vec<f64> = avg_queue_delay.values().copied().collect();
        delays.sort_by(|a, b| a.partial_cmp(b).unwrap());
        delays[delays.len() / 2]
    };
    let median_latency = {
        let mut latencies: Vec<f64> = avg_quorum_latency.values().copied().collect();
        if latencies.is_empty() {
            0.0
        } else {
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
            latencies[latencies.len() / 2]
        }
    };

    info!(
        "reroute: median_delay={:.1}ms median_latency={:.1}ms donor_threshold={:.1}ms ql_threshold={:.1}ms f={}",
        median_delay, median_latency,
        f64::max(30.0, 2.0 * median_delay),
        f64::max(200.0, 2.0 * median_latency),
        f
    );

    // Step 3: Quorum-limited check — if >f validators have high quorum_latency,
    // the system is network-bottlenecked and rerouting cannot help.
    let high_latency_count = avg_quorum_latency.values()
        .filter(|&&lat| lat > f64::max(200.0, 2.0 * median_latency))
        .count();
    if high_latency_count > f {
        info!(
            "reroute: {} validators with high quorum_latency > f={}, quorum-limited — skipping",
            high_latency_count, f
        );
        return vec![];
    }

    // Step 4: Identify donors — validators with queue_delay significantly above median
    let donor_set: HashSet<PublicKey> = sorted_keys.iter()
        .filter(|pk| {
            avg_queue_delay.get(pk).copied().unwrap_or(0.0) > f64::max(30.0, 2.0 * median_delay)
        })
        .copied()
        .collect();
    let mut donors: Vec<PublicKey> = donor_set.iter().copied().collect();
    donors.sort_by(|a, b| {
        avg_queue_delay[b].partial_cmp(&avg_queue_delay[a]).unwrap().then_with(|| a.cmp(b))
    });

    // Step 5: Spare capacity and receivers
    let mut spare_capacity: HashMap<PublicKey, f64> = sorted_keys.iter().map(|&pk| {
        let t = tps.get(&pk).copied().unwrap_or(0.0);
        let cap = capacities.get(&pk).copied().unwrap_or(0) as f64;
        (pk, cap - t)
    }).collect();
    let receivers: Vec<PublicKey> = sorted_keys.iter()
        .filter(|pk| !donor_set.contains(pk) && spare_capacity[pk] > 0.0)
        .copied()
        .collect();

    info!(
        "reroute: donors={} receivers={} (donor_pks: {:?})",
        donors.len(), receivers.len(),
        donors.iter().map(|pk| format!("{}", pk)).collect::<Vec<_>>()
    );

    if donors.is_empty() || receivers.is_empty() {
        info!("reroute: no donors or no receivers, skipping");
        return vec![];
    }

    // Step 6: Per-account migration
    let pk_to_idx: HashMap<PublicKey, usize> = sorted_keys.iter()
        .enumerate()
        .map(|(i, pk)| (*pk, i))
        .collect();

    let mut migrations: Vec<MigrationNotice> = Vec::new();

    for donor_pk in &donors {
        let donor_idx = pk_to_idx[donor_pk];
        assert!(donor_idx < current_assignments.len());
        let donor_tps = tps.get(donor_pk).copied().unwrap_or(0.0);
        if donor_tps <= 0.0 {
            continue;
        }

        let donor_accounts: Vec<u64> = current_assignments[donor_idx].iter().copied().collect();
        if donor_accounts.is_empty() {
            info!("reroute: donor {} has no assigned accounts, skipping", donor_pk);
            continue;
        }

        // Compute per-account load using actual certificate counts
        let empty_counts: HashMap<u64, u64> = HashMap::new();
        let donor_counts = per_validator_account_counts.get(donor_pk).unwrap_or(&empty_counts);
        let total_assigned_tx: u64 = donor_accounts.iter()
            .map(|acct| donor_counts.get(acct).copied().unwrap_or(0))
            .sum();

        info!(
            "reroute: donor {} tps={:.1} accounts={} donor_counts_keys={} total_assigned_tx={}",
            donor_pk, donor_tps, donor_accounts.len(), donor_counts.len(), total_assigned_tx
        );

        let acct_load: HashMap<u64, f64> = donor_accounts.iter().map(|&acct| {
            let load = if total_assigned_tx > 0 {
                donor_tps * donor_counts.get(&acct).copied().unwrap_or(0) as f64 / total_assigned_tx as f64
            } else {
                0.0
            };
            (acct, load)
        }).collect();

        let donor_cap = capacities.get(donor_pk).copied().unwrap_or(0) as f64;
        let shed_target = f64::max(donor_tps * 0.1, donor_tps - donor_cap * 0.75);
        info!(
            "reroute: donor {} cap={:.0} shed_target={:.1} (10%={:.1} excess={:.1})",
            donor_pk, donor_cap, shed_target, donor_tps * 0.1, donor_tps - donor_cap * 0.75
        );
        if shed_target <= 0.0 {
            info!("reroute: donor {} shed_target <= 0, skipping", donor_pk);
            continue;
        }

        // Sort accounts by load descending (heaviest first)
        let mut sorted_accounts = donor_accounts;
        sorted_accounts.sort_by(|a, b| {
            acct_load[b].partial_cmp(&acct_load[a]).unwrap().then_with(|| a.cmp(b))
        });

        // Log top-5 heaviest accounts
        for (i, acct) in sorted_accounts.iter().take(5).enumerate() {
            let has_latency = account_latency.contains_key(acct);
            info!(
                "reroute: donor {} top-{} account {} load={:.1} tx/s has_latency={}",
                donor_pk, i + 1, acct, acct_load[acct], has_latency
            );
        }

        // Log receiver spare capacities
        for r in &receivers {
            info!("reroute: receiver {} spare={:.1}", r, spare_capacity[r]);
        }

        // Migrate one-at-a-time
        let mut shed_so_far = 0.0;
        let mut migrated_count: u64 = 0;
        let mut skip_count: u64 = 0;
        for &acct in &sorted_accounts {
            if shed_so_far >= shed_target {
                break;
            }

            let load = acct_load[&acct];

            // Find best receiver: lowest latency with enough spare capacity
            let latencies = account_latency.get(&acct);
            let eligible_receivers: Vec<_> = receivers.iter()
                .filter(|r| spare_capacity[r] >= load)
                .collect();

            let best_receiver = eligible_receivers.iter()
                .min_by(|a, b| {
                    let lat_a = latencies.map(|l| l.get(a).copied().unwrap_or(u64::MAX)).unwrap_or(u64::MAX);
                    let lat_b = latencies.map(|l| l.get(b).copied().unwrap_or(u64::MAX)).unwrap_or(u64::MAX);
                    lat_a.cmp(&lat_b)
                        .then_with(|| spare_capacity[b].partial_cmp(&spare_capacity[a]).unwrap())
                })
                .map(|r| **r);

            if best_receiver.is_none() && migrated_count == 0 && skip_count == 0 {
                info!(
                    "reroute: first account {} load={:.1} has no eligible receiver (eligible={} latencies={})",
                    acct, load, eligible_receivers.len(), latencies.is_some()
                );
            }

            if let Some(recv_pk) = best_receiver {
                migrations.push(MigrationNotice {
                    account_id: acct,
                    new_target: recv_pk,
                });
                current_assignments[donor_idx].remove(&acct);
                current_assignments[pk_to_idx[&recv_pk]].insert(acct);
                shed_so_far += load;
                *spare_capacity.get_mut(&recv_pk).unwrap() -= load;
                migrated_count += 1;
            } else {
                skip_count += 1;
            }
        }

        info!(
            "reroute: donor {} migrated={} skipped={} total={} shed={:.1}/{:.1} tx/s",
            donor_pk, migrated_count, skip_count, sorted_accounts.len(), shed_so_far, shed_target
        );
    }

    migrations
}

struct AccountDistributionTracker {
    /// Per-round, per-validator account_counts extracted from certificates.
    /// Mirrors the DAG structure but stores only account_counts.
    dag: HashMap<Round, HashMap<PublicKey, (BTreeMap<u64, u64>, u64)>>,
    /// Largest round seen across all received certificates.
    max_round_seen: Round,
}

impl AccountDistributionTracker {
    fn new() -> Self {
        Self {
            dag: HashMap::new(),
            max_round_seen: 0,
        }
    }

    fn record(&mut self, certificate: &Certificate) {
        let round = certificate.round();
        self.max_round_seen = max(self.max_round_seen, round);
        let origin = certificate.origin();
        let counts = &certificate.header.account_counts;

        // Insert into dag.
        self.dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(origin, (counts.clone(), certificate.header.created_at));
    }

    /// Aggregate per-account tx counts per validator over [stable_round - window, stable_round].
    fn per_validator_account_counts(&self, stable_round: Round, window: Round) -> HashMap<PublicKey, HashMap<u64, u64>> {
        let min_round = stable_round.saturating_sub(window);
        let mut result: HashMap<PublicKey, HashMap<u64, u64>> = HashMap::new();
        for (&round, validators) in &self.dag {
            if round < min_round || round > stable_round {
                continue;
            }
            for (pk, (counts, _ts)) in validators {
                let entry = result.entry(*pk).or_default();
                for (&acct, &cnt) in counts {
                    *entry.entry(acct).or_insert(0) += cnt;
                }
            }
        }
        result
    }

    /// Evict entries below min_round.
    fn evict(&mut self, min_round: Round) {
        self.dag.retain(|&r, _| r >= min_round);
    }
}

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed round.
    last_committed_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        // TODO: This cleanup is dangerous: we need to ensure consensus can receive idempotent replies
        // from the primary. Here we risk cleaning up a certificate and receiving it again later.
        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,
    /// This validator's public key.
    name: PublicKey,
    /// Per-validator estimated max throughput (requests/sec). Initialized from the committee
    /// (derived from worker bandwidth and tx size) and later updated via control plane consensus.
    validator_capacities: HashMap<PublicKey, u64>,
    /// Latency in ms between each (client_i, validator_j) pair, keyed by public key.
    /// Empty if committee did not provide latency data.
    client_validator_latency: BTreeMap<PublicKey, BTreeMap<PublicKey, u64>>,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_new_certificates: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_feedback: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates and migration notices to the application layer.
    tx_output: Sender<ConsensusOutput>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
    /// When true, disables account_counts tracking and rerouting computation.
    baseline_mode: bool,
}

impl Consensus {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        gc_depth: Round,
        rx_new_certificates: Receiver<Certificate>,
        tx_feedback: Sender<Certificate>,
        tx_output: Sender<ConsensusOutput>,
        baseline_mode: bool,
    ) {
        let validator_capacities: HashMap<PublicKey, u64> = committee
            .authorities
            .iter()
            .map(|(pk, auth)| (*pk, auth.capacity_by_bw))
            .collect();
        let client_validator_latency = committee.latency_matrix.clone();
        info!("Client-Validator latency matrix: {:?}", client_validator_latency);
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                gc_depth,
                rx_new_certificates,
                tx_feedback,
                tx_output,
                genesis: Certificate::genesis(&committee),
                validator_capacities,
                client_validator_latency,
                baseline_mode,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());
        let mut account_tracker = AccountDistributionTracker::new();
        let mut throughput_tracker = ValidatorThroughputTracker::new(TRACKING_WINDOW);

        let mut cap_entries: Vec<(PublicKey, u64)> = self.validator_capacities.iter().map(|(pk, c)| (*pk, *c)).collect();
        cap_entries.sort_by_key(|(pk, _)| *pk);
        for (pk, cap) in &cap_entries {
            info!("Validator capacity: {} -> {} req/s", pk, cap);
        }

        // Routing state: current_assignments[i] = set of account IDs assigned to validator i.
        // Initialized from account_ranges, updated by compute_rerouting each evaluation round.
        let mut current_assignments: Vec<HashSet<u64>> = self.committee.account_ranges
            .iter()
            .map(|(_, &(start, count))| (start..start + count).collect())
            .collect();

        // Build per-account latency map: account_id -> Arc<BTreeMap<validator, latency_ms>>.
        // Each account's home region determines its latency to each validator.
        // Arc avoids cloning the same BTreeMap for every account in a region.
        let account_latency: HashMap<u64, BTreeMap<PublicKey, u64>> = {
            let mut m = HashMap::new();
            for (&pk, &(start, count)) in &self.committee.account_ranges {
                let row = self.client_validator_latency.get(&pk)
                    .expect("latency_matrix missing entry for validator");
                for acct in start..(start + count) {
                    m.insert(acct, row.clone());
                }
            }
            m
        };

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_new_certificates.recv().await {
            info!("Processing {:?}", certificate);
            let round = certificate.round();

            // Add the new certificate to the local storage.
            if !self.baseline_mode {
                account_tracker.record(&certificate);
                throughput_tracker.record(&certificate);
            }
            if !self.baseline_mode && certificate.origin() == self.name && round % REROUTE_INTERVAL == 0 {
                let stable_round = round.saturating_sub(2);
                throughput_tracker.log(round, stable_round, &self.validator_capacities);

                // Compute and log rerouting decisions
                if let Some(tps) = throughput_tracker.get_tps(stable_round) {
                    let avg_qd = throughput_tracker.avg_queue_delay(stable_round);
                    let qm = throughput_tracker.get_quorum_metrics(stable_round);
                    let per_validator_accts = account_tracker.per_validator_account_counts(stable_round, TRACKING_WINDOW);
                    let sorted_keys: Vec<PublicKey> = self.committee.authorities.keys().cloned().collect();
                    let f = (self.committee.size() - 1) / 3;

                    for (i, pk) in sorted_keys.iter().enumerate() {
                        let qd = avg_qd.get(pk).copied().unwrap_or(0.0);
                        let qm_count = qm.get(pk).map(|v| v.len()).unwrap_or(0);
                        let acct_count = per_validator_accts.get(pk).map(|m| m.len()).unwrap_or(0);
                        let assigned = current_assignments[i].len();
                        info!(
                            "reroute_signal (round={}) validator {}: queue_delay={:.0}ms qm_samples={} acct_counts_keys={} assigned={}",
                            round, pk, qd, qm_count, acct_count, assigned
                        );
                    }

                    let migrations = compute_rerouting(
                        &tps,
                        &self.validator_capacities,
                        &qm,
                        &account_latency,
                        &per_validator_accts,
                        &sorted_keys,
                        &mut current_assignments,
                        f,
                    );
                    if migrations.is_empty() {
                        info!("reroute (round={}) no migration needed", round);
                    } else {
                        info!(
                            "reroute (round={}) {} account migrations",
                            round, migrations.len()
                        );
                        for m in migrations.iter().take(10) {
                            info!(
                                "reroute (round={}) account {} -> validator {}",
                                round, m.account_id, m.new_target
                            );
                        }
                        if migrations.len() > 10 {
                            info!("reroute (round={}) ... and {} more", round, migrations.len() - 10);
                        }
                        // Send migration notices to application layer
                        if let Err(e) = self.tx_output.send(
                            ConsensusOutput::Migrations(migrations)
                        ).await {
                            warn!("Failed to send migration notices: {}", e);
                        }
                    }
                }

                let evict_round = stable_round.saturating_sub(TRACKING_WINDOW);
                throughput_tracker.evict(evict_round);
                account_tracker.evict(evict_round);
            }

            state
                .dag
                .entry(round)
                .or_insert_with(HashMap::new)
                .insert(certificate.origin(), (certificate.digest(), certificate));

            // Try to order the dag to commit. Start from the highest round for which we have at least
            // 2f+1 certificates. This is because we need them to reveal the common coin.
            let r = round - 1;

            // We only elect leaders for even round numbers.
            if r % 2 != 0 || r < 4 {
                continue;
            }

            // Get the certificate's digest of the leader of round r-2. If we already ordered this leader,
            // there is nothing to do.
            let leader_round = r - 2;
            if leader_round <= state.last_committed_round {
                continue;
            }
            let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // debug!("Elected leader counts {:?} for round {}", leader.header.account_counts, leader_round);

            // Check if the leader has f+1 support from its children (ie. round r-1).
            let stake: Stake = state
                .dag
                .get(&(r - 1))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(_, x)| x.header.parents.contains(&leader_digest))
                .map(|(_, x)| self.committee.stake(&x.origin()))
                .sum();

            // If it is the case, we can commit the leader. But first, we need to recursively go back to
            // the last committed leader, and commit all preceding leaders in the right order. Committing
            // a leader block means committing all its dependencies.
            if stake < self.committee.validity_threshold() {
                debug!("Leader {:?} does not have enough support", leader);
                continue;
            }

            // Get an ordered list of past leaders that are linked to the current leader.
            debug!("Leader {:?} has enough support", leader);
            let mut sequence = Vec::new();
            for leader in self.order_leaders(leader, &state).iter().rev() {
                // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                for x in self.order_dag(leader, &state) {
                    // Update and clean up internal state.
                    state.update(&x, self.gc_depth);

                    // Add the certificate to the sequence.
                    sequence.push(x);
                }
            }

            // Log the latest committed round of every authority (for debug).
            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // Output the sequence in the right order.
            for certificate in sequence {
                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }

                self.tx_feedback
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(ConsensusOutput::Certificate(certificate)).await {
                    warn!("Failed to output certificate: {}", e);
                }
            }
        }
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        // Elect the leader.
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        // Return its certificate and the certificate's digest.
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// Order the past leaders that we didn't already commit.
    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for r in (state.last_committed_round + 2..=leader.round() - 2)
            .rev()
            .step_by(2)
        {
            // Get the certificate proposed by the previous leader.
            let (_, prev_leader) = match self.leader(r, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state.dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }
        to_commit
    }

    /// Checks if there is a path between two leaders.
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = dag
                .get(&(r))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}
