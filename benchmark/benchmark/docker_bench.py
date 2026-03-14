# Copyright(C) Facebook, Inc. and its affiliates.
import json
import os
import socket
import subprocess
import sys
from math import ceil
from time import sleep, time as _now

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    DockerCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


def _port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=0.3):
            return True
    except OSError:
        return False


class DockerBench:
    BASE_PORT = 5000
    NETWORK_SUBNET = "172.20.0.0/16"
    NETWORK_NAME = "narwhal-net"
    IMAGE_NAME = "narwhal-bench"
    CONTAINER_PREFIX = "narwhal-validator"

    def __init__(
        self,
        bench_parameters_dict,
        node_parameters_dict,
        bandwidth="10gbit",
        latency="0ms",
        jitter="0ms",
        cpus_per_validator=0,
        lan_bandwidth="100gbit",
        check_mismatch=False,
        primary_bw="500mbit",
        bandwidths=None,
    ):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

        self.latency = latency
        self.jitter = jitter
        self.cpus_per_validator = cpus_per_validator
        self.lan_bandwidth = lan_bandwidth
        self.check_mismatch = check_mismatch

        # QoS bandwidth allocation
        nodes = self.bench_parameters.nodes[0]
        if bandwidths is not None:
            assert len(bandwidths) == nodes, (
                f"BANDWIDTHS_MBPS has {len(bandwidths)} entries but nodes={nodes}"
            )
            self.bandwidths = bandwidths
        else:
            self.bandwidths = [bandwidth] * nodes

        self.primary_bw = primary_bw
        primary_mbit = self._parse_bw_mbit(primary_bw)
        self.worker_bws = []
        for i, bw in enumerate(self.bandwidths):
            total_mbit = self._parse_bw_mbit(bw)
            worker_mbit = total_mbit - primary_mbit
            assert worker_mbit > 0, (
                f"primary_bw ({primary_bw}={primary_mbit}mbit) must be less than "
                f"bandwidth for validator {i} ({bw}={total_mbit}mbit)"
            )
            self.worker_bws.append(self._format_bw(worker_mbit))

    @staticmethod
    def _parse_bw_mbit(bw_str):
        """Parse a TC bandwidth string (e.g., '10gbit', '500mbit') to megabits."""
        bw_str = bw_str.strip().lower()
        if bw_str.endswith('gbit'):
            return int(float(bw_str[:-4]) * 1000)
        elif bw_str.endswith('mbit'):
            return int(float(bw_str[:-4]))
        elif bw_str.endswith('kbit'):
            return max(1, int(float(bw_str[:-4]) / 1000))
        raise ValueError(f'Cannot parse bandwidth: {bw_str}')

    @staticmethod
    def _format_bw(mbit):
        """Format megabits as a TC bandwidth string."""
        if mbit >= 1000 and mbit % 1000 == 0:
            return f'{mbit // 1000}gbit'
        return f'{mbit}mbit'

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _container_ip(self, i):
        return f"172.20.0.{10 + i}"

    @staticmethod
    def _docker_down():
        try:
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    "docker-compose.yml",
                    "down",
                    "--remove-orphans",
                ],
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
        except subprocess.SubprocessError:
            pass

    @staticmethod
    def _entrypoint_hash():
        import hashlib
        h = hashlib.sha256()
        for path in ['docker/entrypoint.sh', 'docker/client-entrypoint.sh']:
            with open(path, 'rb') as f:
                h.update(f.read())
        return h.hexdigest()

    @staticmethod
    def _image_exists():
        result = subprocess.run(
            ['docker', 'image', 'inspect', DockerBench.IMAGE_NAME],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0

    def _build_image(self):
        cmd = [
            "docker",
            "build",
            "-t",
            self.IMAGE_NAME,
            "-f",
            "docker/Dockerfile",
            "docker/",
        ]
        subprocess.run(cmd, check=True)

    def _generate_compose(self, nodes, commands_per_validator, wait_ports_per_validator, client_commands, client_ips, container_ips, client_wait_ports, primary_ports_str):
        """Generate docker-compose.yml programmatically."""
        services = []
        for i in range(nodes):
            ip = self._container_ip(i)
            cmds = commands_per_validator[i]

            cpuset = ""
            if self.cpus_per_validator > 0:
                slot = self.cpus_per_validator + 4
                start = i * slot
                end = start + self.cpus_per_validator - 1
                cpuset = f'\n    cpuset: "{start}-{end}"'

            if self.cpus_per_validator > 0:
                tokio_threads = self.cpus_per_validator // (1 + self.workers)
            else:
                tokio_threads = 0

            service = f"""  validator-{i}:
    image: {self.IMAGE_NAME}
    container_name: {self.CONTAINER_PREFIX}-{i}
    working_dir: /app
    cap_add:
      - NET_ADMIN{cpuset}
    networks:
      {self.NETWORK_NAME}:
        ipv4_address: {ip}
    volumes:
      - ./node:/app/node:ro
      - ./.node-{i}.json:/app/.node-{i}.json:ro
      - ./.committee.json:/app/.committee.json:ro
      - ./.parameters.json:/app/.parameters.json:ro
      - ./logs:/logs:rw
      - ./.db-{i}:/app/.db-{i}:rw"""

            # Mount worker db dirs
            for w in range(self.workers):
                service += f"""
      - ./.db-{i}-{w}:/app/.db-{i}-{w}:rw"""

            service += f"""
    environment:
      - VALIDATOR_ID={i}
      - TC_BANDWIDTH={self.bandwidths[i]}
      - TC_PRIMARY_BW={self.primary_bw}
      - TC_WORKER_BW={self.worker_bws[i]}
      - TC_LATENCY={self.latency}
      - TC_JITTER={self.jitter}
      - TC_LAN_BANDWIDTH={self.lan_bandwidth}
      - OWN_CLIENT_IP={client_ips[i]}
      - PRIMARY_PORTS={primary_ports_str}
      - TOKIO_WORKER_THREADS={tokio_threads}
      - PRIMARY_CMD={cmds['primary']}"""

            # Join worker commands with semicolons
            worker_cmd = ";".join(cmds["workers"])
            service += f"""
      - WORKER_CMD={worker_cmd}"""

            service += f"""
      - WAIT_PORTS={wait_ports_per_validator[i]}"""

            services.append(service)

        # Client containers
        for i in range(nodes):
            client_ip = client_ips[i]
            client_command = client_commands[i]
            own_validator_ip = container_ips[i]

            client_cpuset = ""
            if self.cpus_per_validator > 0:
                slot = self.cpus_per_validator + 4
                c_start = i * slot + self.cpus_per_validator
                client_cpuset = f'\n    cpuset: "{c_start}-{c_start + 3}"'

            service = f"""  client-{i}:
    image: {self.IMAGE_NAME}
    container_name: narwhal-client-{i}
    working_dir: /app
    cap_add:
      - NET_ADMIN{client_cpuset}
    entrypoint: ["/client-entrypoint.sh"]
    networks:
      {self.NETWORK_NAME}:
        ipv4_address: {client_ip}
    volumes:
      - ./benchmark_client:/app/benchmark_client:ro
      - ./logs:/logs:rw
    environment:
      - CLIENT_CMD={client_command}
      - OWN_VALIDATOR_IP={own_validator_ip}
      - TC_LATENCY={self.latency}
      - TC_JITTER={self.jitter}
      - TC_BANDWIDTH={self.bandwidths[i]}
      - WAIT_PORTS={client_wait_ports}"""

            services.append(service)
# end of for loop
        compose = f"""services:
{chr(10).join(services)}

networks:
  {self.NETWORK_NAME}:
    driver: bridge
    ipam:
      config:
        - subnet: {self.NETWORK_SUBNET}
"""
        with open("docker-compose.yml", "w") as f:
            f.write(compose)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading("Starting Docker benchmark")

        self._docker_down()

        try:
            Print.info("Setting up testbed...")
            nodes, rate = self.nodes[0], self.rate[0]

            # Clean root-owned files from previous Docker run, then do regular cleanup.
            subprocess.run(
                'docker run --rm -v "$PWD":/work -w /work ubuntu:24.04 '
                'sh -c "rm -rf logs .db-*"',
                shell=True,
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)

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
            container_ips = [self._container_ip(i) for i in range(nodes)]
            client_ips = [f"172.20.0.{10 + nodes + i}" for i in range(nodes)]
            committee = DockerCommittee(
                names, self.BASE_PORT, self.workers, container_ips, client_ips
            )

            capacities = [] # max tps/validator
            for i in range(nodes):
                # worker_bw_bytes = self._parse_bw_mbit(self.worker_bws[i]) * 1_000_000 / 8
                # capacities.append(int(worker_bw_bytes / self.tx_size / (nodes - 1)))
                capacities.append(2200) # TODO: to be replaced by inferred broadcast capacity based on worker bandwidth and tx size tps_i=sum_worker_bw_bytes_i/tx_size/(num_nodes-1)
            committee.set_capacities(capacities)
            latency_ms = int(self.latency.rstrip('ms')) if self.latency not in ('0ms', '') else 0
            sorted_names = sorted(committee.json['authorities'].keys())
            name_to_idx = {name: i for i, name in enumerate(names)}
            latency_matrix = [
                [0 if name_to_idx[row_name] == name_to_idx[col_name] else latency_ms
                 for col_name in sorted_names]
                for row_name in sorted_names
            ]
            committee.set_latency_matrix(latency_matrix)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Create db directories
            for i in range(nodes):
                subprocess.run(f"mkdir -p .db-{i}", shell=True)
                for w in range(self.workers):
                    subprocess.run(f"mkdir -p .db-{i}-{w}", shell=True)

            # Build commands for each validator (paths relative to /app/ inside container)
            workers_addresses = committee.workers_addresses(self.faults)
            weights = self.rate_weights or [1] * len(workers_addresses)
            total_weight = sum(weights)
            validator_rates = [ceil(rate * w / total_weight) for w in weights]
            excess = sum(validator_rates) - rate
            if excess > 0:
                max_idx = weights.index(max(weights))
                validator_rates[max_idx] -= excess

            v = "-vvv" if debug else "-vv"

            num_accounts = self.bench_parameters.num_accounts
            total_clients = nodes
            account_weights = self.bench_parameters.account_weights or [1] * total_clients
            total_aw = sum(account_weights)
            acct_counts = [num_accounts * w // total_aw for w in account_weights]
            remainder = num_accounts - sum(acct_counts)
            for k in range(remainder):
                acct_counts[k] += 1
            acct_starts = []
            s = 0
            for c in acct_counts:
                acct_starts.append(s)
                s += c

            commands_per_validator = {}
            client_commands = {}
            running_rate = 0
            for i, addresses in enumerate(workers_addresses):
                primary_cmd = (
                    f"./node {v} run --keys .node-{i}.json --committee .committee.json "
                    f"--store .db-{i} --parameters .parameters.json primary"
                    f" 2> /logs/primary-{i}.log"
                )

                worker_cmds = []

                for id, address in addresses:
                    w_cmd = (
                        f"./node {v} run --keys .node-{i}.json --committee .committee.json "
                        f"--store .db-{i}-{id} --parameters .parameters.json worker --id {id}"
                    )
                    w_cmd += f" 2> /logs/worker-{i}-{id}.log"
                    worker_cmds.append(w_cmd)

                worker_addrs = [addr for _, addr in addresses]
                nodes_arg = " ".join(worker_addrs)

                acct_start = acct_starts[i]
                acct_count = acct_counts[i]
                account_args = f"--account-start {acct_start} --num-accounts {acct_count}"

                num_workers = len(addresses)
                client_id_val = i * num_workers
                if self.rr:
                    all_worker_addrs = [addr for all_addrs in workers_addresses for _, addr in all_addrs]
                    addrs_str = " ".join(all_worker_addrs)
                else:
                    addrs_str = " ".join(worker_addrs)
                c_cmd = (
                    f"./benchmark_client {addrs_str} --size {self.tx_size} "
                    f"--rate {validator_rates[i]} --nodes {nodes_arg} "
                    f"{account_args} --client-id {client_id_val}"
                )
                c_cmd += f" 2> /logs/client-{i}-0.log"
                running_rate += validator_rates[i]

                commands_per_validator[i] = {
                    "primary": primary_cmd,
                    "workers": worker_cmds,
                }
                client_commands[i] = c_cmd

            assert abs(running_rate - rate) <= len(
                workers_addresses
            ), f"Running rate {running_rate} deviates too much from target rate {rate}"

            # Compute remote wait ports for each validator.
            wait_ports_per_validator = {}
            for i, name in enumerate(names):
                wait_ports_per_validator[i] = " ".join(
                    committee.remote_addresses(name)
                )

            # All worker transaction addresses across all validators (for client wait).
            all_tx_addrs = []
            for auth in committee.json['authorities'].values():
                for worker in auth['workers'].values():
                    all_tx_addrs.append(worker['transactions'])
            client_wait_ports = " ".join(all_tx_addrs)

            # Extract all primary_to_primary ports for QoS classification
            primary_ports = []
            for auth in committee.json['authorities'].values():
                addr = auth['primary']['primary_to_primary']
                primary_ports.append(addr.split(':')[1])
            primary_ports_str = " ".join(primary_ports)

            # Build Docker image.
            HASH_FILE = '.docker-image-hash'
            current_hash = self._entrypoint_hash()
            stored_hash = open(HASH_FILE).read().strip() if os.path.exists(HASH_FILE) else ''
            if not self._image_exists() or current_hash != stored_hash:
                Print.info("Building Docker image...")
                self._build_image()
                with open(HASH_FILE, 'w') as f:
                    f.write(current_hash)
            else:
                Print.info("Docker image up to date, skipping build.")

            # Generate docker-compose.yml.
            self._generate_compose(nodes, commands_per_validator, wait_ports_per_validator, client_commands, client_ips, container_ips, client_wait_ports, primary_ports_str)

            # Start containers.
            Print.info("Starting containers...")
            _devnull = subprocess.DEVNULL if not sys.stdout.isatty() else None
            subprocess.run(
                ["docker", "compose", "-f", "docker-compose.yml", "up", "-d"],
                check=True,
                stdout=_devnull,
                stderr=_devnull,
            )

            with open(PathMaker.bench_params_file(), 'w') as f:
                json.dump({'duration': self.duration, 'warmup': self.warmup, 'faults': self.faults}, f)

            Print.info("Waiting for all containers to be ready...")
            addrs = [(a.split(':')[0], int(a.split(':')[1])) for a in client_wait_ports.split()]
            deadline = _now() + 120
            while True:
                if all(_port_open(h, p) for h, p in addrs):
                    break
                if _now() > deadline:
                    raise BenchError("Containers did not become ready within 120s", Exception())
                sleep(0.5)
            Print.info("All containers ready.")

            # Wait for benchmark duration.
            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)

            # Stop containers.
            Print.info("Stopping containers...")
            self._docker_down()
            sleep(1)

            # Parse logs and return the parser.
            Print.info("Parsing logs...")
            return LogParser.process(
                PathMaker.logs_path(),
                faults=self.faults,
                duration=self.duration,
                warmup=self.warmup,
                verbose=debug,
                rr=self.rr,
            )

        except (subprocess.SubprocessError, ParseError) as e:
            self._docker_down()
            raise BenchError("Failed to run benchmark", e)
