# Copyright(C) Facebook, Inc. and its affiliates.
from collections import defaultdict
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import basename, join
from re import findall, search
from statistics import mean

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
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

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
                for log in logs:
                    _, _, _, _, samples = self._parse_clients(log)
                    v_sent_list.append(samples)
                self.sent_samples_by_validator[v] = v_sent_list

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

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

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

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

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
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = self.effective_start, max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    if start < self.effective_start:
                        continue
                    end = self.commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0

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

    def _per_validator_end_to_end_tps(self):
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

    def _per_validator_end_to_end_latency(self):
        result = {}
        for v in sorted(self.sizes_by_validator.keys()):
            v_received_list = self.received_samples_by_validator.get(v, [])
            v_sent_list = self.sent_samples_by_validator.get(v, [])
            latencies = []
            for sent, received in zip(v_sent_list, v_received_list):
                for tx_id, batch_id in received.items():
                    if batch_id in self.commits and tx_id in sent:
                        if sent[tx_id] < self.effective_start:
                            continue
                        latencies.append(self.commits[batch_id] - sent[tx_id])
            if latencies:
                result[v] = mean(latencies)
        return result

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, consensus_duration = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, e2e_duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000
        
        warnings = []
        assert isinstance(self.bench_duration, float), 'Bench duration is not set'
        effective_bench_duration = self.bench_duration - self.warmup
        if (consensus_duration / effective_bench_duration) < 0.9:
            warnings.append('Consensus stalled the system')
        if (e2e_duration / effective_bench_duration) < 0.9:
            warnings.append('End-to-end stalled the system')
        assert consensus_latency <= end_to_end_latency, f"Consensus latency {consensus_latency} ms should be less than or equal to committed latency {end_to_end_latency} ms"

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
            f' End-to-end duration: {round(e2e_duration, 2):,} s\n'
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
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
        )

        if self.sizes_by_validator:
            tx_counts, percentages = self._validator_load_distribution()
            per_v_tps = self._per_validator_end_to_end_tps()
            per_v_latency = self._per_validator_end_to_end_latency()

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
                ' + PER-VALIDATOR END-TO-END METRICS:\n'
            )
            for v in sorted(per_v_tps.keys()):
                lat = per_v_latency.get(v)
                lat_str = f'{round(lat * 1_000):,} ms' if lat is not None else 'N/A'
                output += (
                    f' Validator {v}: {round(per_v_tps[v]):,} tx/s,'
                    f' latency {lat_str}\n'
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
