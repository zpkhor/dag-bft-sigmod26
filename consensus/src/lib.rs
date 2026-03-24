// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, ConsensusOutput, MigrationNotice, Round};
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
const REROUTE_INTERVAL: Round = 50;
const _: () = assert!(REROUTE_INTERVAL >= TRACKING_WINDOW);

struct CertifiedTpsTracker {
    /// Per-round, per-validator (created_at_ms, tx_count).
    rounds: HashMap<Round, HashMap<PublicKey, (u64, u64)>>,
    /// Per-round, per-validator (sum_queue_delay_ms, batch_count).
    quorum_stats: HashMap<Round, HashMap<PublicKey, (u64, u64)>>,
    window_rounds: Round,
}

impl CertifiedTpsTracker {
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

        let (sum_qd, batch_count) = certificate.header.quorum_metrics.values()
            .fold((0u64, 0u64), |(sum, cnt), qm| (sum + qm.queue_delay_ms, cnt + 1));
        if batch_count > 0 {
            self.quorum_stats.entry(round).or_default()
                .insert(origin, (sum_qd, batch_count));
        }
    }

    /// Evict entries below min_round.
    fn evict(&mut self, min_round: Round) {
        self.rounds.retain(|&r, _| r >= min_round);
        self.quorum_stats.retain(|&r, _| r >= min_round);
    }

    /// Average queue_delay_ms per validator over [stable_round - window, stable_round].
    fn avg_queue_delay(&self, stable_round: Round) -> HashMap<PublicKey, f64> {
        let min_round = stable_round.saturating_sub(self.window_rounds);
        let mut totals: HashMap<PublicKey, (u64, u64)> = HashMap::new();
        for (&round, validators) in &self.quorum_stats {
            if round < min_round || round > stable_round {
                continue;
            }
            for (pk, &(sum_qd, count)) in validators {
                let entry = totals.entry(*pk).or_insert((0, 0));
                entry.0 += sum_qd;
                entry.1 += count;
            }
        }
        totals.into_iter()
            .filter(|(_, (_, count))| *count > 0)
            .map(|(pk, (sum, count))| (pk, sum as f64 / count as f64))
            .collect()
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

    fn spare_capacity(&self, stable_round: Round, capacities: &HashMap<PublicKey, u64>) -> Option<HashMap<PublicKey, f64>> {
        let tps = self.get_tps(stable_round)?;
        Some(
            capacities
                .iter()
                .map(|(pk, cap)| {
                    let actual = tps.get(pk).copied().unwrap_or(0.0);
                    (*pk, *cap as f64 - actual)
                })
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
/// Donors are identified by: spare capacity < threshold OR queue_delay >> median.
/// Excess per donor is estimated as: estimated_input_rate - proportional_target.
/// Receivers are selected per-account, preferring lowest client-to-receiver latency.
/// Per-account enumeration takes ranges from current_assignments (mutable — updated in place).
fn compute_rerouting(
    tps: &HashMap<PublicKey, f64>,
    capacities: &HashMap<PublicKey, u64>,
    avg_queue_delay: &HashMap<PublicKey, f64>,
    account_latency: &HashMap<u64, BTreeMap<PublicKey, u64>>,
    sorted_keys: &[PublicKey],
    current_assignments: &mut Vec<Vec<(u64, u64)>>,
    f: usize,
) -> Vec<MigrationNotice> {
    let n = sorted_keys.len();
    if n < 2 {
        return vec![];
    }

    const SPARE_THRESHOLD: f64 = 50.0;
    const QUEUE_DELAY_FACTOR: f64 = 3.0;
    const HYSTERESIS_FRACTION: f64 = 0.10;

    // Step 1: Compute spare capacity and congestion signal
    let max_tps = tps.values().cloned().fold(0.0f64, f64::max);
    let total_capacity: f64 = capacities.values().map(|&c| c as f64).sum();
    if total_capacity <= 0.0 || max_tps <= 0.0 {
        return vec![];
    }

    let spare: HashMap<PublicKey, f64> = sorted_keys.iter().map(|&pk| {
        let t = tps.get(&pk).copied().unwrap_or(0.0);
        let cap = capacities.get(&pk).copied().unwrap_or(0) as f64;
        (pk, cap - t)
    }).collect();

    let median_qd = {
        let mut qds: Vec<f64> = sorted_keys.iter()
            .filter_map(|pk| avg_queue_delay.get(pk).copied())
            .collect();
        if qds.is_empty() {
            return vec![];
        }
        qds.sort_by(|a, b| a.partial_cmp(b).unwrap());
        qds[qds.len() / 2]
    };

    // Step 2: Classify donors and receivers
    // Donor: at capacity (spare < threshold) OR congested (queue_delay >> median)
    // Receiver: has spare AND not congested
    let mut donors: Vec<PublicKey> = Vec::new();
    let mut receiver_spare: HashMap<PublicKey, f64> = HashMap::new();

    for &pk in sorted_keys {
        let s = spare[&pk];
        let qd = avg_queue_delay.get(&pk).copied().unwrap_or(0.0);
        let congested = median_qd > 0.0 && qd > QUEUE_DELAY_FACTOR * median_qd;

        if s < SPARE_THRESHOLD || congested {
            donors.push(pk);
        } else {
            receiver_spare.insert(pk, s);
        }
    }

    if donors.is_empty() || receiver_spare.is_empty() {
        return vec![];
    }

    // Regime detection
    if donors.len() > f {
        info!(
            "reroute: {} congested validators > f={}, quorum-limited",
            donors.len(), f
        );
    }

    // Step 3: Estimate excess per donor with hysteresis
    // Each client sends ~ max_tps. Target load proportional to capacity share.
    let mut donor_excess: Vec<(PublicKey, f64)> = donors.iter().filter_map(|&d| {
        let cap = capacities.get(&d).copied().unwrap_or(0) as f64;
        let target_load = max_tps * cap / total_capacity;
        let excess = (max_tps - target_load).max(0.0);
        let threshold = HYSTERESIS_FRACTION * cap;
        if excess < threshold {
            info!("reroute: validator {} excess {:.0} below hysteresis {:.0}, skipping", d, excess, threshold);
            None
        } else {
            Some((d, excess))
        }
    }).collect();
    if donor_excess.is_empty() {
        return vec![];
    }
    donor_excess.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then_with(|| a.0.cmp(&b.0)));

    // Step 4: Per-account latency-aware receiver selection
    let pk_to_idx: HashMap<PublicKey, usize> = sorted_keys.iter()
        .enumerate()
        .map(|(i, pk)| (*pk, i))
        .collect();

    let mut remaining_spare = receiver_spare.clone();
    let mut per_account_migrations: Vec<MigrationNotice> = Vec::new();

    for &(donor_pk, excess) in &donor_excess {
        let donor_idx = pk_to_idx[&donor_pk];
        assert!(donor_idx < current_assignments.len());
        let donor_tps = tps.get(&donor_pk).copied().unwrap_or(0.0);
        if donor_tps <= 0.0 {
            continue;
        }

        let total_assigned: u64 = current_assignments[donor_idx].iter().map(|(_, c)| c).sum();
        if total_assigned == 0 {
            continue;
        }

        // remaining_spare is in tx/s; tps_per_account converts account count to tx/s
        let tps_per_account = donor_tps / total_assigned as f64;
        let fraction = (excess / donor_tps).min(1.0);
        let n_to_migrate = ((fraction * total_assigned as f64).ceil() as u64).min(total_assigned);

        let taken = take_ranges(&mut current_assignments[donor_idx], n_to_migrate);
        let accounts: Vec<u64> = taken.iter()
            .flat_map(|&(start, count)| start..(start + count))
            .collect();

        let mut migrated_count: u64 = 0;
        for &acct in &accounts {
            let latencies = account_latency.get(&acct);
            let best_receiver = remaining_spare.iter()
                .filter(|(_, spare)| **spare >= tps_per_account)
                .min_by_key(|(pk, _)| {
                    latencies.map(|l| l.get(pk).copied().unwrap_or(u64::MAX)).unwrap_or(u64::MAX)
                })
                .map(|(pk, _)| *pk);

            match best_receiver {
                Some(recv_pk) => {
                    per_account_migrations.push(MigrationNotice {
                        account_id: acct,
                        new_target: recv_pk,
                    });
                    *remaining_spare.get_mut(&recv_pk).unwrap() -= tps_per_account;
                    current_assignments[pk_to_idx[&recv_pk]].push((acct, 1));
                    migrated_count += 1;
                }
                None => {
                    let remaining = &accounts[migrated_count as usize..];
                    for chunk in remaining.chunk_by(|a, b| *b == *a + 1) {
                        current_assignments[donor_idx].push((chunk[0], chunk.len() as u64));
                    }
                    info!(
                        "reroute: no receiver capacity, {} accounts unplaceable for donor {}",
                        remaining.len(), donor_pk
                    );
                    break;
                }
            }
        }

        info!(
            "reroute: migrated {}/{} accounts ({:.1}%) from donor {}",
            migrated_count, total_assigned, fraction * 100.0, donor_pk
        );
    }

    // Coalesce fragmented (acct, 1) entries in current_assignments into contiguous ranges
    for ranges in current_assignments.iter_mut() {
        if ranges.len() <= 1 {
            continue;
        }
        ranges.sort_by_key(|&(start, _)| start);
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for &(start, count) in ranges.iter() {
            if let Some(last) = merged.last_mut() {
                if last.0 + last.1 == start {
                    last.1 += count;
                    continue;
                }
            }
            merged.push((start, count));
        }
        *ranges = merged;
    }

    per_account_migrations
}

/// Take up to `n` accounts from the front of a list of ranges, splitting if needed.
fn take_ranges(ranges: &mut Vec<(u64, u64)>, n: u64) -> Vec<(u64, u64)> {
    let mut taken = Vec::new();
    let mut remaining = n;
    while remaining > 0 && !ranges.is_empty() {
        let (start, count) = ranges[0];
        if count <= remaining {
            taken.push(ranges.remove(0));
            remaining -= count;
        } else {
            taken.push((start, remaining));
            ranges[0] = (start + remaining, count - remaining);
            remaining = 0;
        }
    }
    taken
}

struct AccountCountsHistory {
    /// Per-round, per-validator account_counts extracted from certificates.
    /// Mirrors the DAG structure but stores only account_counts.
    dag: HashMap<Round, HashMap<PublicKey, (BTreeMap<u64, u64>, u64)>>,
    /// Aggregated account_counts per validator over the rounds currently in `dag`.
    past_account_counts: HashMap<PublicKey, BTreeMap<u64, u64>>,
    /// Largest round seen across all received certificates.
    max_round_seen: Round,
}

impl AccountCountsHistory {
    fn new() -> Self {
        Self {
            dag: HashMap::new(),
            past_account_counts: HashMap::new(),
            max_round_seen: 0,
        }
    }

    fn log(&self) {
        let mut entries: Vec<(PublicKey, &BTreeMap<u64, u64>)> =
            self.past_account_counts.iter().map(|(pk, counts)| (*pk, counts)).collect();
        entries.sort_by_key(|(pk, _)| *pk);

        for (pk, counts) in &entries {
            let total: u64 = counts.values().sum();
            debug!("past_account_counts validator {}: total={} {:?}", pk, total, counts);
        }
    }

    fn log_stable(&self, committee_size: usize) {
        // at least 2f+1 validators share the same view on the past up to this round, so we can consider it "stable"
        let safe_round = self.max_round_seen.saturating_sub(2);

        let mut stable: HashMap<PublicKey, (BTreeMap<u64, u64>, u64, u64)> = HashMap::new();
        for (round, validators) in &self.dag {
            if *round > safe_round {
                continue;
            }
            assert_eq!(validators.len(), committee_size, "round {} has {} validators", round, validators.len());
            for (pk, (counts, ts)) in validators {
                let entry = stable.entry(*pk).or_insert_with(|| (BTreeMap::new(), u64::MAX, 0u64));
                for (acc, cnt) in counts {
                    *entry.0.entry(*acc).or_insert(0) += cnt;
                }
                entry.1 = entry.1.min(*ts);
                entry.2 = entry.2.max(*ts);
            }
        }

        // stable_round_count is uniform across all validators (asserted above)
        let stable_round_count = self.dag.keys().filter(|&&r| r <= safe_round).count() as u64;

        let mut all_min: Vec<u64> = stable.values().map(|(_, min, _)| *min).collect();
        let mut all_max: Vec<u64> = stable.values().map(|(_, _, max)| *max).collect();
        all_min.sort();
        all_max.sort();
        let median_min = all_min[all_min.len() / 2];
        let median_max = all_max[all_max.len() / 2];
        let median_duration_secs = (median_max - median_min) as f64 / 1000.0;

        let mut entries: Vec<(PublicKey, (BTreeMap<u64, u64>, u64, u64))> = stable.into_iter().collect(); // TODO: no need to sort, useless
        entries.sort_by_key(|(pk, _)| *pk);

        for (pk, (counts, min_ts, max_ts)) in &entries {
            let total: u64 = counts.values().sum();
            let tps = if max_ts > min_ts {
                total as f64 / ((*max_ts - *min_ts) as f64 / 1000.0)
            } else {
                0.0
            };
            let tpr = if stable_round_count > 0 { total / stable_round_count } else { 0 };
            let tpx = if median_duration_secs > 0.0 { total as f64 / median_duration_secs } else { 0.0 };
            // debug!("stable_account_counts (safe_round={}) validator {}: total={} {:?}", safe_round, pk, total, counts);
            info!("stable_account_counts (safe_round={}) validator {}: total={} tx/s={:.1} tx/r={} tx/x={:.1}", safe_round, pk, total, tps, tpr, tpx);
        }
    }

    /// Aggregated per-validator account counts over [stable_round - window, stable_round].
    fn stable_counts(&self, stable_round: Round, window: Round) -> HashMap<PublicKey, BTreeMap<u64, u64>> {
        let min_round = stable_round.saturating_sub(window);
        let mut result: HashMap<PublicKey, BTreeMap<u64, u64>> = HashMap::new();
        for (&round, validators) in &self.dag {
            if round < min_round || round > stable_round {
                continue;
            }
            for (pk, (counts, _ts)) in validators {
                let entry = result.entry(*pk).or_default();
                for (acc, cnt) in counts {
                    *entry.entry(*acc).or_insert(0) += cnt;
                }
            }
        }
        result
    }

    fn update(&mut self, certificate: &Certificate) {
        let round = certificate.round();
        self.max_round_seen = max(self.max_round_seen, round);
        let origin = certificate.origin();
        let counts = &certificate.header.account_counts;

        // Insert into dag.
        self.dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(origin, (counts.clone(), certificate.header.created_at));

        // Incrementally add this certificate's counts.
        let past = self.past_account_counts.entry(origin).or_insert_with(BTreeMap::new);
        for (acc, cnt) in counts {
            *past.entry(*acc).or_insert(0) += cnt;
        }

        // Evict the round that just fell outside the window, subtracting its counts
        // from past_account_counts for every validator that had a certificate there.
        if round >= TRACKING_WINDOW {
            if let Some(evicted) = self.dag.remove(&(round - TRACKING_WINDOW)) {
                for (validator, (old_counts, _)) in &evicted {
                    if let Some(past) = self.past_account_counts.get_mut(validator) {
                        for (acc, cnt) in old_counts {
                            if let Some(entry) = past.get_mut(acc) {
                                *entry = entry.saturating_sub(*cnt);
                            }
                        }
                        past.retain(|_, v| *v > 0);
                    }
                }
            }
        }
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
        let mut account_history = AccountCountsHistory::new();
        let mut certified_tps_tracker = CertifiedTpsTracker::new(TRACKING_WINDOW);

        let mut cap_entries: Vec<(PublicKey, u64)> = self.validator_capacities.iter().map(|(pk, c)| (*pk, *c)).collect();
        cap_entries.sort_by_key(|(pk, _)| *pk);
        for (pk, cap) in &cap_entries {
            info!("Validator capacity: {} -> {} req/s", pk, cap);
        }

        // Routing state: current_assignments[i] = list of (start, count) ranges assigned to validator i.
        // Initialized from account_ranges, updated by compute_rerouting each evaluation round.
        let mut current_assignments: Vec<Vec<(u64, u64)>> = self.committee.account_ranges
            .iter()
            .map(|(_, &(start, count))| vec![(start, count)])
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
                account_history.update(&certificate);
                certified_tps_tracker.record(&certificate);
            }
            if !self.baseline_mode && certificate.origin() == self.name && round % REROUTE_INTERVAL == 0 {
                // account_history.log();
                account_history.log_stable(self.committee.size());
                let stable_round = round.saturating_sub(2);
                certified_tps_tracker.log(round, stable_round, &self.validator_capacities);

                // Compute and log rerouting decisions
                if let Some(tps) = certified_tps_tracker.get_tps(stable_round) {
                    let avg_qd = certified_tps_tracker.avg_queue_delay(stable_round);
                    let sorted_keys: Vec<PublicKey> = self.committee.authorities.keys().cloned().collect();
                    let f = (self.committee.size() - 1) / 3;

                    for &pk in &sorted_keys {
                        let qd = avg_qd.get(&pk).copied().unwrap_or(0.0);
                        info!("reroute_signal (round={}) validator {}: queue_delay={:.0}ms", round, pk, qd);
                    }

                    let migrations = compute_rerouting(
                        &tps,
                        &self.validator_capacities,
                        &avg_qd,
                        &account_latency,
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

                certified_tps_tracker.evict(stable_round.saturating_sub(TRACKING_WINDOW));
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
// TODO: develop our own algorithm if needed (after reviewing this algo)