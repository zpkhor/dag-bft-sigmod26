# CloudLab benchmark orchestrator.
# Deploys Narwhal on CloudLab physical machines using SSH + tmux.
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from os.path import basename, splitext
from time import sleep, time as _now

from fabric import Connection

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    CloudLabCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.instance import CloudLabInstanceManager
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class CloudLabBench:
    BASE_PORT = 5000

    def __init__(
        self,
        bench_parameters_dict,
        node_parameters_dict,
        manifest_file,
        username,
        baseline=False,
        round_robin=False,
        latency_ms=100,
        primary_bw_kbps=25000,
        worker_bws_kbps=None,
    ):
        self.username = username
        self.latency_ms = latency_ms

        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        self.manager = CloudLabInstanceManager.make(manifest_file, username)
        nodes = self.bench_parameters.nodes[0]
        assert nodes <= self.manager.num_validators(), (
            f'Requested {nodes} nodes but manifest has {self.manager.num_validators()} validators'
        )

        self.baseline = baseline
        self.round_robin = round_robin
        self.primary_bw_kbps = primary_bw_kbps
        assert worker_bws_kbps is not None, 'worker_bws_kbps must be provided'
        assert len(worker_bws_kbps) >= nodes, (
            f'worker_bws_kbps has {len(worker_bws_kbps)} entries but need {nodes}'
        )
        self.worker_bws_kbps = worker_bws_kbps[:nodes]

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _ssh(self, host):
        """Create a Fabric Connection to a host."""
        return Connection(host, user=self.username)

    def _background_run(self, ssh_host, command, log_file):
        """Run a command in a tmux session on a remote host."""
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = self._ssh(ssh_host)
        c.run(cmd, hide=True)

    def _parallel_ssh(self, hosts, fn):
        """Run fn(host) in parallel across all hosts. Asserts no failures."""
        failed = []
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            futures = {pool.submit(fn, h): h for h in hosts}
            for future in as_completed(futures):
                host = futures[future]
                try:
                    future.result()
                except Exception as e:
                    failed.append((host, e))
        assert not failed, f'Parallel SSH failed on: {failed}'

    def _apply_tc_shaping(self, v_ssh, c_ssh_host, nodes, committee):
        """Replace Emulab default TC with QoS classes for primary/worker/client."""
        Print.info('Applying TC QoS shaping...')

        primary_bw = self.primary_bw_kbps
        latency_ms = self.latency_ms // 4  # per-interface delay (25ms for 100ms RTT)

        # Extract primary_to_primary ports from committee
        primary_ports = []
        for auth in committee.json['authorities'].values():
            addr = auth['primary']['primary_to_primary']
            primary_ports.append(addr.split(':')[1])

        def _port_filters(dev, ports):
            """Generate tc filter rules to classify primary ports into class 1:10."""
            lines = []
            for p in ports:
                lines.append(f'tc filter add dev {dev} parent 1:0 protocol ip prio 2 u32 match ip sport {p} 0xffff flowid 1:10')
                lines.append(f'tc filter add dev {dev} parent 1:0 protocol ip prio 2 u32 match ip dport {p} 0xffff flowid 1:10')
            return lines

        def _htb_classes(dev, total_bw, primary_bw, worker_bw):
            """Generate HTB root + primary/worker classes with netem delay."""
            return [
                f'tc qdisc add dev {dev} root handle 1: htb default 20',
                f'tc class add dev {dev} parent 1: classid 1:1 htb rate {total_bw}kbit',
                f'tc class add dev {dev} parent 1:1 classid 1:10 htb rate {primary_bw}kbit ceil {primary_bw}kbit prio 0',
                f'tc class add dev {dev} parent 1:1 classid 1:20 htb rate {worker_bw}kbit ceil {worker_bw}kbit prio 1',
                f'tc qdisc add dev {dev} parent 1:10 handle 10: netem delay {latency_ms}ms limit 1000',
                f'tc qdisc add dev {dev} parent 1:20 handle 20: netem delay {latency_ms}ms limit 1000',
            ]

        def _shape_validator(i):
            worker_bw = self.worker_bws_kbps[i]
            total_bw = primary_bw + worker_bw
            lines = [
                'set -e',
                'IFACE=$(ip -o addr show | grep "10\\\\." | awk "{print \\$2}" | head -1)',
                '[ -z "$IFACE" ] && echo "FATAL: no 10.x interface" && exit 1',
                'tc qdisc del dev $IFACE root 2>/dev/null || true',
                'tc qdisc del dev $IFACE ingress 2>/dev/null || true',
                'tc qdisc del dev ifb0 root 2>/dev/null || true',
                # Egress: HTB with primary/worker classes (both with netem delay)
            ]
            lines += _htb_classes('$IFACE', total_bw, primary_bw, worker_bw)
            lines += _port_filters('$IFACE', primary_ports)
            # Ingress: redirect to ifb0
            lines += [
                'ip link set dev ifb0 up',
                'tc qdisc add dev $IFACE handle ffff: ingress',
                'tc filter add dev $IFACE parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb0',
            ]
            lines += _htb_classes('ifb0', total_bw, primary_bw, worker_bw)
            lines += _port_filters('ifb0', primary_ports)
            script = '\n'.join(lines)
            self._ssh(v_ssh[i]).run(f'sudo bash -c \'{script}\'', hide=True)

        def _shape_client():
            client_bw = max(self.worker_bws_kbps)
            remote_extra_lat = self.latency_ms // 2  # 50ms one-way for remote
            n = nodes
            lines = [
                'set -e',
                'IFACE=$(ip -o addr show | grep "10\\\\." | awk "{print \\$2}" | head -1)',
                '[ -z "$IFACE" ] && echo "FATAL: no 10.x interface" && exit 1',
                'tc qdisc del dev $IFACE root 2>/dev/null || true',
                f'tc qdisc add dev $IFACE root handle 1: htb default 99',
                f'tc class add dev $IFACE parent 1: classid 1:1 htb rate {client_bw}kbit',
                f'tc class add dev $IFACE parent 1:1 classid 1:99 htb rate 1mbit ceil {client_bw}kbit',
            ]
            for region_id in range(n):
                for v_idx in range(n):
                    mark = region_id * n + v_idx + 1
                    classid = mark + 10
                    lines.append(
                        f'tc class add dev $IFACE parent 1:1 classid 1:{classid} htb rate 1mbit ceil {client_bw}kbit'
                    )
                    lines.append(
                        f'tc filter add dev $IFACE parent 1:0 protocol ip prio 1 handle {mark} fw flowid 1:{classid}'
                    )
                    if region_id != v_idx:
                        lines.append(
                            f'tc qdisc add dev $IFACE parent 1:{classid} handle {classid}: netem delay {remote_extra_lat}ms limit 1000'
                        )
            script = '\n'.join(lines)
            self._ssh(c_ssh_host).run(f'sudo bash -c \'{script}\'', hide=True)

        with ThreadPoolExecutor(max_workers=nodes + 1) as pool:
            futures = [pool.submit(_shape_validator, i) for i in range(nodes)]
            futures.append(pool.submit(_shape_client))
            failed = []
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    failed.append(e)
            assert not failed, f'TC shaping failed: {failed}'

        Print.info(
            f'TC QoS applied: primary={primary_bw}kbps, '
            f'worker_bws={[self.worker_bws_kbps[i] for i in range(nodes)]}kbps, '
            f'client={max(self.worker_bws_kbps)}kbps'
        )

    def _kill(self, ssh_hosts, delete_logs=False):
        """Kill all tmux sessions on the given hosts."""
        delete_cmd = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = f'{delete_cmd} && ({CommandMaker.kill()} || true)'
        def _kill_one(host):
            try:
                self._ssh(host).run(cmd, hide=True)
            except Exception:
                pass
        self._parallel_ssh(ssh_hosts, _kill_one)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting CloudLab benchmark')

        nodes, rate = self.nodes[0], self.rate[0]

        # Slice to requested node count
        v_ssh = self.manager.validator_ssh_hosts()[:nodes]
        c_ssh = self.manager.client_ssh_hosts()
        assert len(c_ssh) >= 1, 'Need at least 1 client node in manifest'
        c_ssh_host = c_ssh[0]
        v_ips = self.manager.validator_ips()[:nodes]
        c_ip = self.manager.client_ips()[0]
        all_ssh = v_ssh + [c_ssh_host]

        try:
            # Kill any previous run
            Print.info('Killing previous processes...')
            self._kill(all_ssh, delete_logs=True)

            # Clean up locally
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)

            # Compile locally
            Print.info('Compiling...')
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create local symlinks
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Rsync binaries and required shared libs to all machines (parallel)
            Print.info(f'Distributing binaries to {len(all_ssh)} machines...')
            binary_path = PathMaker.binary_path()
            libs_to_ship = []
            for lib_name in ['libstdc++.so.6', 'libgcc_s.so.1']:
                result = subprocess.run(
                    f'ldd {binary_path}/node | grep {lib_name}',
                    shell=True, capture_output=True, text=True,
                )
                for line in result.stdout.strip().split('\n'):
                    parts = line.strip().split('=>')
                    if len(parts) == 2:
                        path = parts[1].strip().split('(')[0].strip()
                        if path and '/lib/x86_64-linux-gnu/' not in path:
                            libs_to_ship.append(path)

            def _distribute_one(host):
                subprocess.run(
                    f'rsync -azL {binary_path}/node {binary_path}/benchmark_client '
                    f'{" ".join(libs_to_ship)} '
                    f'{self.username}@{host}:~/',
                    shell=True, check=True,
                )
                post_cmds = ['sudo setcap cap_net_admin+ep ~/benchmark_client']
                if libs_to_ship:
                    post_cmds.insert(0,
                        'sudo cp ~/libstdc++.so.6 ~/libgcc_s.so.1 '
                        '/usr/lib/x86_64-linux-gnu/ && sudo ldconfig'
                    )
                self._ssh(host).run(' && '.join(post_cmds), hide=True)

            self._parallel_ssh(all_ssh, _distribute_one)

            # Generate keys
            Print.info('Generating configuration files...')
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]

            # Build committee (single client IP)
            committee = CloudLabCommittee(
                names, self.BASE_PORT, self.workers, v_ips, c_ip,
            )

            latency_matrix = {
                name: {other: (0 if name == other else self.latency_ms) for other in names}
                for name in names
            }
            committee.set_latency_matrix(latency_matrix)

            if not self.baseline:
                capacities = []
                for i in range(nodes):
                    workers_bw_bytes = self.worker_bws_kbps[i] * 1000 / 8
                    capacity = int(workers_bw_bytes / (self.tx_size + 44) / (nodes - 1) * 0.9)
                    capacities.append(capacity)
                committee.set_capacities(capacities)

            if self.baseline:
                self.node_parameters.json['baseline_mode'] = True
            self.node_parameters.print(PathMaker.parameters_file())

            # Account distribution
            num_accounts = self.bench_parameters.num_accounts
            account_weights = self.bench_parameters.account_weights or [1] * nodes
            total_aw = sum(account_weights)
            acct_counts = [num_accounts * w // total_aw for w in account_weights]
            remainder = num_accounts - sum(acct_counts)
            for k in range(remainder):
                acct_counts[k] += 1
            acct_starts = []
            s = 0
            for c_val in acct_counts:
                acct_starts.append(s)
                s += c_val

            account_ranges = {
                name: (acct_starts[i], acct_counts[i])
                for i, name in enumerate(names)
            }
            committee.set_account_ranges(account_ranges)
            committee.print(PathMaker.committee_file())

            # Upload config files to all machines (parallel)
            Print.info('Uploading config files...')
            def _upload_validator(i):
                conn = self._ssh(v_ssh[i])
                db_dirs = f'mkdir -p .db-{i} ' + ' '.join(f'.db-{i}-{w}' for w in range(self.workers))
                conn.run(
                    f'{CommandMaker.remote_cleanup()} || true && mkdir -p logs && {db_dirs}',
                    hide=True,
                )
                conn.put(PathMaker.committee_file(), '.')
                conn.put(PathMaker.parameters_file(), '.')
                conn.put(PathMaker.key_file(i), '.')

            def _upload_client():
                conn = self._ssh(c_ssh_host)
                conn.run(
                    f'{CommandMaker.remote_cleanup()} || true && mkdir -p logs',
                    hide=True,
                )
                conn.put(PathMaker.committee_file(), '.')
                conn.put(PathMaker.parameters_file(), '.')

            with ThreadPoolExecutor(max_workers=nodes + 1) as pool:
                futures = [pool.submit(_upload_validator, i) for i in range(nodes)]
                futures.append(pool.submit(_upload_client))
                for f in as_completed(futures):
                    f.result()

            # Build single client command (consolidated)
            workers_addresses = committee.workers_addresses(self.faults)

            # --validator-workers args
            vw_args_parts = []
            for name in names:
                auth = committee.json['authorities'][name]
                waddrs = '+'.join(
                    auth['workers'][wid]['transactions']
                    for wid in sorted(auth['workers'].keys())
                )
                vw_args_parts.append(f'--validator-workers {name}:{waddrs}')
            vw_args = ' '.join(vw_args_parts)

            # --account-ranges args
            ar_args_parts = []
            for name in names:
                start, count = account_ranges[name]
                ar_args_parts.append(f'--account-ranges {name}:{start}:{count}')
            ar_args = ' '.join(ar_args_parts)

            # --rate-weights aligned with sorted validator order
            weights = self.rate_weights or [1] * nodes
            sorted_names = committee.sorted_authority_names()
            name_to_idx = {name: i for i, name in enumerate(names)}
            sorted_weights = [weights[name_to_idx[n]] for n in sorted_names]
            rate_weights_str = ','.join(str(w) for w in sorted_weights)

            # All worker transaction addresses
            all_worker_addrs = [addr for addresses in workers_addresses for _, addr in addresses]
            all_nodes_arg = ' '.join(all_worker_addrs)

            reply_addr = list(committee.json['authorities'].values())[0]['client_reply']

            rr_flag = ' --round-robin' if self.round_robin else ''
            client_command = (
                f'./benchmark_client --size {self.tx_size} '
                f'--rate {rate} --nodes {all_nodes_arg} '
                f'{ar_args} --rate-weights {rate_weights_str} '
                f'--reply-addr {reply_addr} --own-validator {names[0]} '
                f'{vw_args}{rr_flag}'
            )

            # Start primaries
            Print.info('Starting primaries...')
            for i in range(nodes):
                primary_cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug,
                )
                self._background_run(
                    v_ssh[i], primary_cmd, PathMaker.primary_log_file(i),
                )

            # Start workers
            Print.info('Starting workers...')
            for i, addresses in enumerate(workers_addresses):
                for wid, _ in addresses:
                    worker_cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, wid),
                        PathMaker.parameters_file(),
                        wid,
                        debug=debug,
                    )
                    self._background_run(
                        v_ssh[i], worker_cmd, PathMaker.worker_log_file(i, wid),
                    )

            # Wait for worker ports to be ready
            Print.info('Waiting for workers to be ready...')
            per_validator_checks = {}
            for addr in all_worker_addrs:
                host, port = addr.split(':')
                per_validator_checks.setdefault(host, []).append(port)

            ip_to_ssh = dict(zip(v_ips, v_ssh))

            deadline = _now() + 120
            while True:
                all_ready = True
                for ip, ports in per_validator_checks.items():
                    ssh_host = ip_to_ssh[ip]
                    checks = ' && '.join(
                        f'bash -c "echo >/dev/tcp/{ip}/{p}"' for p in ports
                    )
                    c = self._ssh(ssh_host)
                    result = c.run(checks, hide=True, warn=True)
                    if result.failed:
                        all_ready = False
                        break
                if all_ready:
                    break
                assert _now() <= deadline, 'Workers did not become ready within 120s'
                sleep(0.5)
            Print.info('All workers ready.')

            # Apply QoS TC shaping (replaces Emulab defaults + geni-script SO_MARK)
            self._apply_tc_shaping(v_ssh, c_ssh_host, nodes, committee)

            # Start single client
            Print.info('Starting client...')
            self._background_run(
                c_ssh_host, client_command, PathMaker.client_log_file(0, 0),
            )

            # Save bench params for log parsing
            with open(PathMaker.bench_params_file(), 'w') as f:
                json.dump({
                    'duration': self.duration,
                    'warmup': self.warmup,
                    'faults': self.faults,
                }, f)

            # Run benchmark
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)

            # Stop
            Print.info('Stopping processes...')
            self._kill(all_ssh, delete_logs=False)
            sleep(1)

            # Download logs (parallel)
            Print.info('Downloading logs...')
            cmd = CommandMaker.clean_logs()
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            subprocess.run(['mkdir', '-p', PathMaker.logs_path()])

            def _download_validator(i):
                conn = self._ssh(v_ssh[i])
                conn.get(PathMaker.primary_log_file(i), local=PathMaker.primary_log_file(i))
                for w in range(self.workers):
                    conn.get(
                        PathMaker.worker_log_file(i, w),
                        local=PathMaker.worker_log_file(i, w),
                    )

            def _download_client():
                conn = self._ssh(c_ssh_host)
                conn.get(
                    PathMaker.client_log_file(0, 0),
                    local=PathMaker.client_log_file(0, 0),
                )

            with ThreadPoolExecutor(max_workers=nodes + 1) as pool:
                futures = [pool.submit(_download_validator, i) for i in range(nodes)]
                futures.append(pool.submit(_download_client))
                for f in as_completed(futures):
                    f.result()

            # Parse logs
            Print.info('Parsing logs...')
            return LogParser.process(
                PathMaker.logs_path(),
                faults=self.faults,
                duration=self.duration,
                warmup=self.warmup,
                verbose=debug,
            )

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill(all_ssh, delete_logs=False)
            raise BenchError('Failed to run benchmark', e)


class CloudLabInstaller:
    """Install dependencies on CloudLab machines."""

    def __init__(self, manifest_file, username):
        self.manager = CloudLabInstanceManager.make(manifest_file, username)
        self.username = username

    def _ssh(self, host):
        return Connection(host, user=self.username)

    def install(self):
        """Install Rust and build dependencies on all machines in parallel."""
        hosts = self.manager.all_ssh_hosts()
        Print.info(f'Installing dependencies on {len(hosts)} machines in parallel...')
        cmd = ' && '.join([
            'sudo apt-get update',
            'sudo apt-get -y install build-essential cmake clang',
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',
        ])

        def _install_one(host):
            c = self._ssh(host)
            c.run(cmd, hide=True)
            return host

        failed = []
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            futures = {pool.submit(_install_one, h): h for h in hosts}
            for future in as_completed(futures):
                host = futures[future]
                try:
                    future.result()
                    Print.info(f'  Done: {host}')
                except Exception as e:
                    Print.warn(f'  FAILED: {host}: {e}')
                    failed.append(host)

        assert not failed, f'Installation failed on: {", ".join(failed)}'
        Print.heading(f'Installed on {len(hosts)} machines')
