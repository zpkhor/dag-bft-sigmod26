# Copyright(C) Facebook, Inc. and its affiliates.
import json
import os
import socket
import subprocess
import sys
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
        worker_bws,
        latency="0ms",
        jitter="0ms",
        cpus_per_validator=0,
        lan_bandwidth="100gbit",
        check_mismatch=False,
        primary_bw="500mbit",
        baseline=False,
        tc_netem_limit=0,
        tc_netem_limit_client=0,
        round_robin=False,
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
        self.tc_netem_limit = tc_netem_limit
        self.tc_netem_limit_client = tc_netem_limit_client

        # QoS bandwidth allocation
        nodes = self.bench_parameters.nodes[0]
        assert len(worker_bws) == nodes, (
            f"worker_bws has {len(worker_bws)} entries but nodes={nodes}"
        )

        self.baseline = baseline
        self.round_robin = round_robin
        self.primary_bw = primary_bw
        primary_mbit = self._parse_bw_mbit(primary_bw)
        self.worker_bws = []
        self.total_bws = []
        for i, wbw in enumerate(worker_bws):
            worker_mbit = self._parse_bw_mbit(wbw)
            assert worker_mbit > 0, (
                f"worker_bw for validator {i} ({wbw}={worker_mbit}mbit) must be > 0"
            )
            self.worker_bws.append(self._format_bw(worker_mbit))
            self.total_bws.append(self._format_bw(primary_mbit + worker_mbit))

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

    def _generate_compose(self, nodes, commands_per_validator, wait_ports_per_validator, client_command, client_ip, container_ips, client_wait_ports, primary_ports_str):
        """Generate docker-compose.yml programmatically."""
        # Halve latency for netem: egress-only delay on both endpoints means
        # each side contributes half the RTT (self.latency is the target RTT).
        if self.latency not in ('0ms', ''):
            half_lat_ms = int(self.latency.rstrip('ms')) // 2
            tc_latency = f'{half_lat_ms}ms'
        else:
            tc_latency = self.latency

        services = []
        for i in range(nodes):
            ip = self._container_ip(i)
            cmds = commands_per_validator[i]

            cpuset = ""
            if self.cpus_per_validator > 0:
                slot = self.cpus_per_validator
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

            tokio_env = f"\n      - TOKIO_WORKER_THREADS={tokio_threads}" if tokio_threads > 0 else ""
            netem_limit_env = f"\n      - TC_NETEM_LIMIT={self.tc_netem_limit}" if self.tc_netem_limit > 0 else ""
            service += f"""
    environment:
      - VALIDATOR_ID={i}
      - TC_BANDWIDTH={self.total_bws[i]}
      - TC_PRIMARY_BW={self.primary_bw}
      - TC_WORKER_BW={self.worker_bws[i]}
      - TC_LATENCY={tc_latency}
      - TC_JITTER={self.jitter}
      - TC_LAN_BANDWIDTH={self.lan_bandwidth}
      - OWN_CLIENT_IP={client_ip}
      - PRIMARY_PORTS={primary_ports_str}{tokio_env}{netem_limit_env}
      - PRIMARY_CMD={cmds['primary']}"""

            # Join worker commands with semicolons
            worker_cmd = ";".join(cmds["workers"])
            service += f"""
      - WORKER_CMD={worker_cmd}"""

            service += f"""
      - WAIT_PORTS={wait_ports_per_validator[i]}"""

            services.append(service)

        # Single client container
        max_bw = -1
        for bw in self.total_bws:
            max_bw = max(max_bw, self._parse_bw_mbit(bw))
        max_bw = self._format_bw(max_bw)

        client_cpuset = ""
        if self.cpus_per_validator > 0:
            slot = self.cpus_per_validator
            c_start = nodes * slot
            client_cpuset = f'\n    cpuset: "{c_start}-{c_start + 2 * nodes - 1}"'

        netem_limit_client_env = f"\n      - TC_NETEM_LIMIT_CLIENT={self.tc_netem_limit_client}" if self.tc_netem_limit_client > 0 else ""
        validator_ips_str = " ".join(container_ips)
        service = f"""  client-0:
    image: {self.IMAGE_NAME}
    container_name: narwhal-client-0
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
      - NUM_REGIONS={nodes}
      - VALIDATOR_IPS={validator_ips_str}
      - TC_LATENCY={tc_latency}
      - TC_JITTER={self.jitter}
      - TC_BANDWIDTH={max_bw}
      - WAIT_PORTS={client_wait_ports}{netem_limit_client_env}"""

        services.append(service)

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
            client_ip = f"172.20.0.{10 + nodes}"
            committee = DockerCommittee(
                names, self.BASE_PORT, self.workers, container_ips, client_ip
            )
            
            if not self.baseline:
                capacities = [] # max tps/validator
                for i in range(nodes):
                    workers_bw_bytes = self._parse_bw_mbit(self.worker_bws[i]) * 1_000_000 / 8
                    capacity = int(workers_bw_bytes / (self.tx_size + 44) / (nodes - 1) * 0.9) # 40 TCP/IP + 4 length-prefix codec
                    capacities.append(capacity)
                committee.set_capacities(capacities)
            latency_ms = (int(self.latency.rstrip('ms')) if self.latency not in ('0ms', '') else 0) // 2
            latency_matrix = {
                name: {other: (0 if name == other else latency_ms) for other in names}
                for name in names
            }
            committee.set_latency_matrix(latency_matrix)

            if self.baseline:
                self.node_parameters.json['baseline_mode'] = True
            self.node_parameters.print(PathMaker.parameters_file())

            # Create db directories
            for i in range(nodes):
                subprocess.run(f"mkdir -p .db-{i}", shell=True)
                for w in range(self.workers):
                    subprocess.run(f"mkdir -p .db-{i}-{w}", shell=True)

            # Build commands for each validator (paths relative to /app/ inside container)
            workers_addresses = committee.workers_addresses(self.faults)

            v = "-vvv" if debug else "-vv"

            num_accounts = self.bench_parameters.num_accounts
            account_weights = self.bench_parameters.account_weights or [1] * nodes
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

            # Set account ranges in committee (keyed by public key)
            account_ranges = {
                name: (acct_starts[i], acct_counts[i])
                for i, name in enumerate(names)
            }
            committee.set_account_ranges(account_ranges)
            committee.print(PathMaker.committee_file())

            # Build --validator-workers args for all validators
            vw_args_parts = []
            for name in names:
                auth = committee.json['authorities'][name]
                waddrs = "+".join(
                    auth['workers'][wid]['transactions']
                    for wid in sorted(auth['workers'].keys())
                )
                vw_args_parts.append(f"--validator-workers {name}:{waddrs}")
            vw_args = " ".join(vw_args_parts)

            # Build --account-ranges args for all validators
            ar_args_parts = []
            for name in names:
                start, count = account_ranges[name]
                ar_args_parts.append(f"--account-ranges {name}:{start}:{count}")
            ar_args = " ".join(ar_args_parts)

            # Build --rate-weights aligned with sorted validator order
            weights = self.rate_weights or [1] * nodes
            sorted_names = committee.sorted_authority_names()
            name_to_idx = {name: i for i, name in enumerate(names)}
            sorted_weights = [weights[name_to_idx[n]] for n in sorted_names]
            rate_weights_str = ",".join(str(w) for w in sorted_weights)

            # All worker transaction addresses (for single client --nodes and WAIT_PORTS)
            all_worker_addrs = [addr for addresses in workers_addresses for _, addr in addresses]
            all_nodes_arg = " ".join(all_worker_addrs)

            reply_addr = list(committee.json['authorities'].values())[0]['client_reply']

            rr_flag = " --round-robin" if self.round_robin else ""
            client_command = (
                f"./benchmark_client --size {self.tx_size} "
                f"--rate {rate} --nodes {all_nodes_arg} "
                f"{ar_args} --rate-weights {rate_weights_str} "
                f"--reply-addr {reply_addr} --own-validator {names[0]} "
                f"{vw_args}{rr_flag}"
                f" 2> /logs/client-0-0.log"
            )

            commands_per_validator = {}
            for i, addresses in enumerate(workers_addresses):
                primary_cmd = (
                    f"./node {v} run --keys .node-{i}.json --committee .committee.json "
                    f"--store .db-{i} --parameters .parameters.json primary"
                    f" 2> /logs/primary-{i}.log"
                )

                worker_cmds = []
                for id, _ in addresses:
                    w_cmd = (
                        f"./node {v} run --keys .node-{i}.json --committee .committee.json "
                        f"--store .db-{i}-{id} --parameters .parameters.json worker --id {id}"
                    )
                    w_cmd += f" 2> /logs/worker-{i}-{id}.log"
                    worker_cmds.append(w_cmd)

                commands_per_validator[i] = {
                    "primary": primary_cmd,
                    "workers": worker_cmds,
                }

            # Compute remote wait ports for each validator.
            wait_ports_per_validator = {}
            for i, name in enumerate(names):
                wait_ports_per_validator[i] = " ".join(
                    committee.remote_addresses(name)
                )

            client_wait_ports = all_nodes_arg

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
            self._generate_compose(nodes, commands_per_validator, wait_ports_per_validator, client_command, client_ip, container_ips, client_wait_ports, primary_ports_str)

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
            )

        except (subprocess.SubprocessError, ParseError) as e:
            self._docker_down()
            raise BenchError("Failed to run benchmark", e)
