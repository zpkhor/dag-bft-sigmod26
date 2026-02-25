# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil
from time import sleep

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
    ):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

        self.bandwidth = bandwidth
        self.latency = latency
        self.jitter = jitter
        self.cpus_per_validator = cpus_per_validator
        self.lan_bandwidth = lan_bandwidth
        self.check_mismatch = check_mismatch

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _container_ip(self, i):
        return f"172.20.0.{10 + i}"

    def _docker_down(self):
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

    def _generate_compose(self, nodes, commands_per_validator, wait_ports_per_validator):
        """Generate docker-compose.yml programmatically."""
        services = []
        for i in range(nodes):
            ip = self._container_ip(i)
            cmds = commands_per_validator[i]

            cpuset = ""
            if self.cpus_per_validator > 0:
                start = i * self.cpus_per_validator
                end = start + self.cpus_per_validator - 1
                cpuset = f'\n    cpuset: "{start}-{end}"'

            tokio_threads = self.node_parameters.json.get("tokio_threads", 0)

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
      - ./benchmark_client:/app/benchmark_client:ro
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
      - TC_BANDWIDTH={self.bandwidth}
      - TC_LATENCY={self.latency}
      - TC_JITTER={self.jitter}
      - TC_LAN_BANDWIDTH={self.lan_bandwidth}
      - TOKIO_WORKER_THREADS={tokio_threads}
      - PRIMARY_CMD={cmds['primary']}"""

            # Join worker commands with semicolons
            worker_cmd = ";".join(cmds["workers"])
            service += f"""
      - WORKER_CMD={worker_cmd}"""

            client_cmd = ";".join(cmds["clients"])
            service += f"""
      - CLIENT_CMD={client_cmd}"""

            service += f"""
      - WAIT_PORTS={wait_ports_per_validator[i]}"""

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
            committee = DockerCommittee(
                names, self.BASE_PORT, self.workers, container_ips
            )
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

            v = "-vvv" if debug else "-vv"

            num_accounts = self.bench_parameters.num_accounts
            total_clients = nodes
            accounts_base = num_accounts // total_clients
            accounts_remainder = num_accounts % total_clients

            commands_per_validator = {}
            running_rate = 0
            for i, addresses in enumerate(workers_addresses):
                primary_cmd = (
                    f"./node {v} run --keys .node-{i}.json --committee .committee.json "
                    f"--store .db-{i} --parameters .parameters.json primary"
                    f" 2> /logs/primary-{i}.log"
                )

                worker_cmds = []
                client_cmds = []

                for id, address in addresses:
                    w_cmd = (
                        f"./node {v} run --keys .node-{i}.json --committee .committee.json "
                        f"--store .db-{i}-{id} --parameters .parameters.json worker --id {id}"
                    )
                    w_cmd += f" 2> /logs/worker-{i}-{id}.log"
                    worker_cmds.append(w_cmd)

                worker_addrs = [addr for _, addr in addresses]
                nodes_arg = " ".join(worker_addrs)
                open_loop_flag = "--open-loop" if self.open_loop else ""

                acct_start = i * accounts_base + min(i, accounts_remainder)
                acct_count = accounts_base + (1 if i < accounts_remainder else 0)
                account_args = f"--account-start {acct_start} --num-accounts {acct_count}"

                num_workers = len(addresses)
                client_id_val = i * num_workers
                # TODO: hack — there is one client per validator but client_reply is
                # defined per worker in the committee. We arbitrarily use worker 0's
                # port. Ideally client_reply should be a validator-level address, not
                # per-worker, and exposed via a proper committee API method.
                worker_0_info = committee.json['authorities'][names[i]]['workers'][0]
                reply_port = worker_0_info['client_reply'].split(':')[1]
                reply_args = f"--client-id {client_id_val} --reply-port {reply_port}"

                rr = self.bench_parameters.rr
                if rr:
                    all_worker_addrs = []
                    for v_idx, addrs in enumerate(workers_addresses):
                        cip = container_ips[v_idx]
                        for _, addr in addrs:
                            port = addr.split(':')[1]
                            all_worker_addrs.append(f'{cip}:{port}')
                    addrs_str = " ".join(all_worker_addrs)
                    rr_args = f"--rr --num-validators {nodes}"
                else:
                    addrs_str = " ".join(worker_addrs)
                    rr_args = ""
                print(f"addrs_str: {addrs_str}")
                check_mismatch_flag = "--check-mismatch" if self.check_mismatch else ""
                c_cmd = (
                    f"./benchmark_client {addrs_str} --size {self.tx_size} "
                    f"--rate {validator_rates[i]} --nodes {nodes_arg} {open_loop_flag} {account_args} {reply_args} {rr_args} {check_mismatch_flag}"
                )
                c_cmd += f" 2> /logs/client-{i}-0.log"
                client_cmds.append(c_cmd)
                running_rate += validator_rates[i]

                commands_per_validator[i] = {
                    "primary": primary_cmd,
                    "workers": worker_cmds,
                    "clients": client_cmds,
                }

            assert abs(running_rate - rate) <= len(
                workers_addresses
            ), f"Running rate {running_rate} deviates too much from target rate {rate}"

            # Compute remote wait ports for each validator.
            wait_ports_per_validator = {}
            for i, name in enumerate(names):
                wait_ports_per_validator[i] = " ".join(
                    committee.remote_addresses(name)
                )

            # Build Docker image.
            Print.info("Building Docker image...")
            self._build_image()

            # Generate docker-compose.yml.
            self._generate_compose(nodes, commands_per_validator, wait_ports_per_validator)

            # Start containers.
            Print.info("Starting containers...")
            subprocess.run(
                ["docker", "compose", "-f", "docker-compose.yml", "up", "-d"],
                check=True,
            )

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
            )

        except (subprocess.SubprocessError, ParseError) as e:
            self._docker_down()
            raise BenchError("Failed to run benchmark", e)
