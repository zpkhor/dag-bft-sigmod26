// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

const WINDOW_SIZE: Round = 10;

struct AccountCountsHistory {
    /// Per-round, per-validator account_counts extracted from certificates.
    /// Mirrors the DAG structure but stores only account_counts.
    dag: HashMap<Round, HashMap<PublicKey, BTreeMap<u64, u64>>>,
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

    fn log_stable(&self) {
        // at least 2f+1 validators share the same view on the past up to this round, so we can consider it "stable"
        let safe_round = self.max_round_seen.saturating_sub(2);

        let mut stable: HashMap<PublicKey, BTreeMap<u64, u64>> = HashMap::new();
        for (round, validators) in &self.dag {
            if *round > safe_round {
                continue;
            }
            for (pk, counts) in validators {
                let entry = stable.entry(*pk).or_insert_with(BTreeMap::new);
                for (acc, cnt) in counts {
                    *entry.entry(*acc).or_insert(0) += cnt;
                }
            }
        }

        let mut entries: Vec<(PublicKey, BTreeMap<u64, u64>)> = stable.into_iter().collect();
        entries.sort_by_key(|(pk, _)| *pk);

        for (pk, counts) in &entries {
            let total: u64 = counts.values().sum();
            debug!("stable_account_counts (safe_round={}) validator {}: total={} {:?}", safe_round, pk, total, counts);
        }
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
            .insert(origin, counts.clone());

        // Incrementally add this certificate's counts.
        let past = self.past_account_counts.entry(origin).or_insert_with(BTreeMap::new);
        for (acc, cnt) in counts {
            *past.entry(*acc).or_insert(0) += cnt;
        }

        // Evict the round that just fell outside the window, subtracting its counts
        // from past_account_counts for every validator that had a certificate there.
        if round >= WINDOW_SIZE {
            if let Some(evicted) = self.dag.remove(&(round - WINDOW_SIZE)) {
                for (validator, old_counts) in &evicted {
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

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_new_certificates: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_feedback: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        gc_depth: Round,
        rx_new_certificates: Receiver<Certificate>,
        tx_feedback: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        let validator_capacities: HashMap<PublicKey, u64> = committee
            .authorities
            .iter()
            .map(|(pk, auth)| (*pk, auth.capacity_by_bw))
            .collect();
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
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());
        let mut account_history = AccountCountsHistory::new();

        let mut cap_entries: Vec<(PublicKey, u64)> = self.validator_capacities.iter().map(|(pk, c)| (*pk, *c)).collect();
        cap_entries.sort_by_key(|(pk, _)| *pk);
        for (pk, cap) in &cap_entries {
            info!("Validator capacity: {} -> {} req/s", pk, cap);
        }

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_new_certificates.recv().await {
            debug!("Processing {:?}", certificate);
            let round = certificate.round();

            // Add the new certificate to the local storage.
            account_history.update(&certificate);
            if certificate.origin() == self.name && round % 20 == 0 {
                debug!("Account counts history at round {}:", round);
                account_history.log();
                account_history.log_stable();
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

                if let Err(e) = self.tx_output.send(certificate).await {
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
