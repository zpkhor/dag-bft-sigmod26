# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    LocalCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 5000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file, env_prefix=''):
        name = splitext(basename(log_file))[0]
        cmd = f"{env_prefix}{command} 2> {log_file}"
        subprocess.run(["tmux", "new", "-d", "-s", name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError("Failed to kill testbed", e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading("Starting local benchmark")

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info("Setting up testbed...")
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT, self.workers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Run the clients (they will wait for the nodes to be ready).
            workers_addresses = committee.workers_addresses(self.faults)
            # Build environment prefix for tokio thread limit
            tokio_threads = self.node_parameters.json.get('tokio_threads', 0)
            env_prefix = f'TOKIO_WORKER_THREADS={tokio_threads} ' if tokio_threads > 0 else ''

            weights = self.rate_weights or [1] * len(workers_addresses)
            total_weight = sum(weights)
            validator_rates = [ceil(rate * w / total_weight) for w in weights]
            for i, addresses in enumerate(workers_addresses):
                num_workers = len(addresses)
                worker_rate = ceil(validator_rates[i] / num_workers)
                for id, address in addresses:
                    cmd = CommandMaker.run_client(
                        address,
                        self.tx_size,
                        worker_rate,
                        [x for y in workers_addresses for _, x in y],
                    )
                    log_file = PathMaker.client_log_file(i, id)
                    self._background_run(cmd, log_file, env_prefix)

            # Run the primaries (except the faulty ones).
            for i, address in enumerate(committee.primary_addresses(self.faults)):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug,
                )
                log_file = PathMaker.primary_log_file(i)
                self._background_run(cmd, log_file, env_prefix)

            # Run executors if in isolated mode (must start before workers to receive messages).
            execution_mode = self.node_parameters.json.get('execution_mode', 'in_process')
            if execution_mode == 'isolated':
                Print.info("Running in Isolated mode - spawning executor processes...")
                for i, addresses in enumerate(workers_addresses):
                    for id, address in addresses:
                        cmd = CommandMaker.run_executor(
                            PathMaker.key_file(i),
                            PathMaker.committee_file(),
                            PathMaker.db_path(i, id),
                            PathMaker.parameters_file(),
                            id,  # The executor's id (matches worker id).
                            debug=debug,
                        )
                        log_file = PathMaker.executor_log_file(i, id)
                        self._background_run(cmd, log_file, env_prefix)

            # Run the workers (except the faulty ones).
            for i, addresses in enumerate(workers_addresses):
                for id, address in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        debug=debug,
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file, env_prefix)

            # Wait for all transactions to be processed.
            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info("Parsing logs...")
            return LogParser.process(PathMaker.logs_path(), faults=self.faults, duration=self.duration)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError("Failed to run benchmark", e)
