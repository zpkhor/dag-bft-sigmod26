# Copyright(C) Facebook, Inc. and its affiliates.
from collections import defaultdict
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import basename, join
from re import findall, search
from statistics import mean, quantiles

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0,
                 workers_by_validator=None, clients_by_validator=None,
                 duration=None, warmup=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        self.bench_duration = float(duration) if duration is not None else None
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples, reply_samples_list, reply_mismatches \
            = zip(*results)
        self.misses = sum(misses)
        self.reply_samples = list(reply_samples_list)
        self.reply_mismatches = sum(reply_mismatches)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])

        # Warmup trimming: discard commits/proposals in the warmup window.
        self.warmup = warmup
        if warmup and self.start:
            cutoff = min(self.start) + warmup
            self.commits = {d: t for d, t in self.commits.items() if t >= cutoff}
            self.proposals = {d: t for d, t in self.proposals.items() if d in self.commits}
        self.effective_start = cutoff if (warmup and self.start) else min(self.start)

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Parse per-validator data if available.
        self.sizes_by_validator = {}
        self.received_samples_by_validator = {}
        self.sent_samples_by_validator = {}
        self.misses_by_validator = {}
        if workers_by_validator:
            for v, logs in workers_by_validator.items():
                v_sizes = {}
                v_received_list = []
                for log in logs:
                    s, r, _ = self._parse_workers(log)
                    v_sizes.update(s)
                    v_received_list.append(r)
                self.sizes_by_validator[v] = v_sizes
                self.received_samples_by_validator[v] = v_received_list
        if clients_by_validator:
            for v, logs in clients_by_validator.items():
                v_sent_list = []
                v_misses = 0
                for log in logs:
                    _, _, _, misses, samples, _, _ = self._parse_clients(log)
                    v_sent_list.append(samples)
                    v_misses += misses
                self.sent_samples_by_validator[v] = v_sent_list
                self.misses_by_validator[v] = v_misses

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _merge_results_fplus1(self, input):
        # Use the (f+1)th earliest timestamp (f=1, so 2nd earliest).
        # A commit is only meaningful once f+1 validators have committed it.
        collected = {}
        for x in input:
            for k, v in x:
                collected.setdefault(k, []).append(v)
        merged = {}
        for k, timestamps in collected.items():
            timestamps.sort()
            # Pick the 2nd earliest (index 1) if available, else the earliest
            merged[k] = timestamps[min(1, len(timestamps) - 1)]
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+) account (\d+) client (\d+)', log)
        samples = {(int(c), int(s), int(a)): self._to_posix(t) for t, s, a, c in tmp}

        tmp = findall(r'\[(.*Z) .* Received reply for tx (\d+) account (\d+) client (\d+)', log)
        reply_samples = {}
        for t, tx_str, acct_str, cli_str in tmp:
            key = (int(cli_str), int(tx_str), int(acct_str))
            reply_samples.setdefault(key, []).append(self._to_posix(t))

        reply_mismatches = len(findall(r'Reply mismatch for tx', log))

        return size, rate, start, misses, samples, reply_samples, reply_mismatches

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+) account (\d+) client (\d+)', log)
        samples = {(int(c), int(s), int(a)): d for d, s, a, c in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _calculate_latency_metrics(self, latency_list):
        """Calculate mean and p95 latency from a list of latencies."""
        if not latency_list:
            return {'mean': 0, 'p95': 0}

        result = {'mean': mean(latency_list)}

        # Calculate p95 if we have enough samples
        if len(latency_list) >= 20:
            # quantiles(data, n=20) gives 19 cut points
            # Index 18 is the 95th percentile (19/20 = 0.95)
            q = quantiles(latency_list, n=20)
            result['p95'] = q[18]
        else:
            # For small samples, use max as p95 approximation
            result['p95'] = max(latency_list)

        return result

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return self._calculate_latency_metrics(latency)

    def _committed_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = self.effective_start, max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _committed_latency(self):
        global_sent = {}
        for sent in self.sent_samples:
            global_sent.update(sent)

        global_received = {}
        for received in self.received_samples:
            global_received.update(received)

        latency = []
        for key, batch_id in global_received.items():
            if batch_id in self.commits:
                assert key in global_sent  # We receive txs that we sent.
                start = global_sent[key]
                if start < self.effective_start:
                    continue
                latency.append(self.commits[batch_id] - start)
        return self._calculate_latency_metrics(latency)

    def _e2e_committed_latency(self):
        if not isinstance(self.faults, int):
            return {'mean': 0, 'p95': 0}
        threshold = self.faults + 1

        global_sent = {}
        for sent in self.sent_samples:
            global_sent.update(sent)

        global_replies = {}
        for replies in self.reply_samples:
            for key, timestamps in replies.items():
                global_replies.setdefault(key, []).extend(timestamps)

        latency = []
        for key, timestamps in global_replies.items():
            assert key in global_sent  # We receive replies for txs that we sent.
            start = global_sent[key]
            if start < self.effective_start:
                continue
            if len(timestamps) < threshold:
                continue
            end_time = sorted(timestamps)[threshold - 1]  # (f+1)-th earliest
            latency.append(end_time - start)
        return self._calculate_latency_metrics(latency)

    def _validator_load_distribution(self):
        result = {}
        for v, v_sizes in sorted(self.sizes_by_validator.items()):
            committed_bytes = sum(
                s for d, s in v_sizes.items() if d in self.commits
            )
            tx_count = committed_bytes // self.size[0]
            result[v] = tx_count
        total = sum(result.values())
        percentages = {
            v: (count / total * 100 if total else 0)
            for v, count in result.items()
        }
        return result, percentages

    def _per_validator_committed_tps(self):
        if not self.commits:
            return {}
        start, end = self.effective_start, max(self.commits.values())
        duration = end - start
        if duration == 0:
            return {}
        result = {}
        for v, v_sizes in sorted(self.sizes_by_validator.items()):
            committed_bytes = sum(
                s for d, s in v_sizes.items() if d in self.commits
            )
            result[v] = (committed_bytes / duration) / self.size[0]
        return result

    def _per_validator_committed_latency(self):
        global_sent = {}
        for sent in self.sent_samples:
            global_sent.update(sent)

        result = {}
        for v in sorted(self.sizes_by_validator.keys()):
            v_received_list = self.received_samples_by_validator.get(v, [])
            latencies = []
            for received in v_received_list:
                for key, batch_id in received.items():
                    if batch_id in self.commits:
                        assert key in global_sent  # We receive txs that we sent.
                        start = global_sent[key]
                        if start < self.effective_start:
                            continue
                        latencies.append(self.commits[batch_id] - start)
            if latencies:
                result[v] = self._calculate_latency_metrics(latencies)
        return result

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_metrics = self._consensus_latency()
        consensus_latency = consensus_metrics['mean'] * 1_000
        consensus_p95 = consensus_metrics['p95'] * 1_000
        consensus_tps, consensus_bps, consensus_duration = self._consensus_throughput()
        committed_tps, committed_bps, commit_duration = self._committed_throughput()
        committed_metrics = self._committed_latency()
        committed_latency = committed_metrics['mean'] * 1_000
        e2e_p95 = committed_metrics['p95'] * 1_000
        e2e_reply_metrics = self._e2e_committed_latency()
        e2e_reply_latency = e2e_reply_metrics['mean'] * 1_000
        e2e_reply_p95 = e2e_reply_metrics['p95'] * 1_000
        
        warnings = []
        assert isinstance(self.bench_duration, float), 'Bench duration is not set'
        effective_bench_duration = self.bench_duration - self.warmup
        if (consensus_duration / effective_bench_duration) < 0.9:
            warnings.append('Consensus stalled the system')
        if (commit_duration / effective_bench_duration) < 0.9:
            warnings.append('Commit stalled the system')
        assert consensus_latency <= committed_latency, f"Consensus latency {consensus_latency} ms should be less than or equal to committed latency {committed_latency} ms"

        warnings_str = ''
        if warnings:
            warnings_str = (
            '\n WARNINGS:\n'
            + ''.join(f'  - {msg}\n' for msg in warnings)
            )

        output = (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Benchmark duration: {self.bench_duration:,} s\n'
            f' Consensus duration: {round(consensus_duration, 2):,} s\n'
            f' Commit duration: {round(commit_duration, 2):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency (mean): {round(consensus_latency):,} ms\n'
            f' Consensus latency (p95): {round(consensus_p95):,} ms\n'
            '\n'
            f' Committed TPS: {round(committed_tps):,} tx/s\n'
            f' Committed BPS: {round(committed_bps):,} B/s\n'
            f' Committed latency (mean): {round(committed_latency):,} ms\n'
            f' Committed latency (p95): {round(e2e_p95):,} ms\n'
            f' E2E latency f+1 replies (mean): {round(e2e_reply_latency):,} ms\n'
            f' E2E latency f+1 replies (p95): {round(e2e_reply_p95):,} ms\n'
        )
        if self.reply_mismatches > 0:
            output += f' Reply mismatches: {self.reply_mismatches:,}\n'

        if self.sizes_by_validator:
            tx_counts, percentages = self._validator_load_distribution()
            per_v_tps = self._per_validator_committed_tps()
            per_v_latency = self._per_validator_committed_latency()

            output += (
                '\n'
                ' + VALIDATOR LOAD DISTRIBUTION:\n'
            )
            for v in sorted(tx_counts.keys()):
                output += (
                    f' Validator {v}: {tx_counts[v]:,} tx'
                    f' ({percentages[v]:.1f}%)\n'
                )

            output += (
                '\n'
                ' + PER-VALIDATOR COMMIT METRICS:\n'
                ' Validator    TPS (tx/s)    Latency (ms)    Misses\n'
            )
            for v in sorted(per_v_tps.keys()):
                lat_metrics = per_v_latency.get(v)
                lat_str = f'{round(lat_metrics["mean"] * 1_000):,}' if lat_metrics else 'N/A'
                misses = self.misses_by_validator.get(v, 0)
                output += (
                    f' {v:<12} {round(per_v_tps[v]):<13,} {lat_str:<15} {misses}\n'
                )
            total_tps = sum(per_v_tps.values())
            weighted_lat = sum(
                per_v_latency[v]['mean'] * percentages[v] / 100
                for v in per_v_latency if v in percentages
            )
            weighted_lat_str = f'{round(weighted_lat * 1_000):,}' if per_v_latency else 'N/A'
            total_misses = sum(self.misses_by_validator.values())
            output += (
                f' {"Overall":<12} {round(total_tps):<13,} {weighted_lat_str + " (wtd)":<15} {total_misses}\n'
            )

            output += (
                '\n'
                ' + PER-VALIDATOR COMMIT TAIL LATENCY (p95):\n'
            )
            for v in sorted(per_v_latency.keys()):
                lat_metrics = per_v_latency.get(v)
                p95_str = f'{round(lat_metrics["p95"] * 1_000):,}' if lat_metrics else 'N/A'
                output += (
                    f' Validator {v}: {p95_str} ms\n'
                )

            # Calculate weighted p95 for overall
            if per_v_latency:
                weighted_p95 = sum(
                    per_v_latency[v]['p95'] * percentages[v] / 100
                    for v in per_v_latency if v in percentages
                )
                output += (
                    f' Overall (weighted): {round(weighted_p95 * 1_000):,} ms\n'
                )

        if warnings_str:
            output += warnings_str

        output += '-----------------------------------------\n'
        return output

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0, duration=None, warmup=0):
        assert isinstance(directory, str)

        clients = []
        clients_by_validator = defaultdict(list)
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                content = f.read()
            clients.append(content)
            m = search(r'client-(\d+)-\d+', basename(filename))
            if m:
                clients_by_validator[int(m.group(1))].append(content)

        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]

        workers = []
        workers_by_validator = defaultdict(list)
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                content = f.read()
            workers.append(content)
            m = search(r'worker-(\d+)-\d+', basename(filename))
            if m:
                workers_by_validator[int(m.group(1))].append(content)

        return cls(
            clients, primaries, workers, faults=faults,
            workers_by_validator=dict(workers_by_validator),
            clients_by_validator=dict(clients_by_validator),
            duration=duration,
            warmup=warmup,
        )
