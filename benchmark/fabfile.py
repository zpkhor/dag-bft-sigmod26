import json
import os
from fabric import task, Connection

from benchmark.local import LocalBench
from benchmark.docker_bench import DockerBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError
from benchmark.cloudlab_bench import CloudLabBench, CloudLabInstaller

@task
def docker(ctx, debug=False, worker_bw='10gbit', latency='0ms', jitter='0ms',
           cpus_per_validator=0, lan_bandwidth='100gbit', primary_bw='500mbit'):
    ''' Run benchmarks in Docker containers with tc bandwidth shaping '''
    rate = int(os.environ.get('RATE', 50_000))
    duration = int(os.environ.get('DURATION', 20))
    warmup = int(os.environ.get('WARMUP', 0))
    rate_weights_raw = os.environ.get('RATE_WEIGHTS')
    rate_weights = [float(w) for w in rate_weights_raw.split(',')] if rate_weights_raw else None
    account_weights_raw = os.environ.get('ACCOUNT_WEIGHTS')
    account_weights = [int(w) for w in account_weights_raw.split(',')] if account_weights_raw else None
    worker_bandwidths_mbps_raw = os.environ.get('WORKER_BANDWIDTHS_MBPS')
    worker_bandwidths = [f"{int(v)}mbit" for v in worker_bandwidths_mbps_raw.split(',')] if worker_bandwidths_mbps_raw else None
    num_accounts = int(os.environ.get('NUM_ACCOUNTS', 1_000_000))
    routing_mode = os.environ.get('ROUTING_MODE', '')
    baseline = os.environ.get('BASELINE', '0') == '1'
    round_robin = routing_mode == 'round-robin'
    if round_robin:
        baseline = True
    check_mismatch = os.environ.get('CHECK_MISMATCH', '0') == '1'
    tc_netem_limit = int(os.environ.get('TC_NETEM_LIMIT', 0))
    tc_netem_limit_client = int(os.environ.get('TC_NETEM_LIMIT_CLIENT', 0))
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': rate,
        'tx_size': 512,
        'duration': duration,
        'rate_weights': rate_weights,
        'account_weights': account_weights,
        'warmup': warmup,
        'num_accounts': num_accounts,
    }
    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
    }
    try:
        ret = DockerBench(
            bench_params, node_params,
            worker_bw=worker_bw, latency=latency, jitter=jitter,
            cpus_per_validator=int(cpus_per_validator),
            lan_bandwidth=lan_bandwidth,
            check_mismatch=check_mismatch,
            primary_bw=primary_bw,
            worker_bandwidths=worker_bandwidths,
            baseline=baseline,
            tc_netem_limit=tc_netem_limit,
            tc_netem_limit_client=tc_netem_limit_client,
            round_robin=round_robin,
        ).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def docker_down(ctx):
    ''' Tear down Docker containers from a previous docker benchmark run '''
    DockerBench._docker_down()


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=False):
    ''' Run benchmarks on AWS '''
    bench_params = {
        'faults': 3,
        'nodes': [10],
        'workers': 1,
        'collocate': True,
        'rate': [10_000, 110_000],
        'tx_size': 512,
        'duration': 300,
        'runs': 2,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        params_file = os.path.join('logs', 'bench-params.json')
        if os.path.exists(params_file):
            with open(params_file) as f:
                p = json.load(f)
            duration, warmup, faults = p['duration'], p['warmup'], p['faults']
        else:
            duration, warmup, faults = 60, 5, '?'
        print(LogParser.process('./logs', faults=faults, duration=duration, warmup=warmup, verbose=True).result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))


@task
def cloudlab(ctx, debug=False,
             manifest='manifest.xml', username='zpkhor', latency_ms=100):
    ''' Run benchmarks on CloudLab physical machines '''
    rate = int(os.environ.get('RATE', 8300))
    duration = int(os.environ.get('DURATION', 50))
    warmup = int(os.environ.get('WARMUP', 5))
    rate_weights_raw = os.environ.get('RATE_WEIGHTS')
    rate_weights = [float(w) for w in rate_weights_raw.split(',')] if rate_weights_raw else None
    account_weights_raw = os.environ.get('ACCOUNT_WEIGHTS')
    account_weights = [int(w) for w in account_weights_raw.split(',')] if account_weights_raw else None
    num_accounts = int(os.environ.get('NUM_ACCOUNTS', 1_000_000))
    primary_bw_kbps = int(os.environ.get('PRIMARY_BW_KBPS', 25000))
    worker_bws_raw = os.environ.get('WORKER_BWS_KBPS')
    worker_bws_kbps = [int(v) for v in worker_bws_raw.split(',')] if worker_bws_raw else [75000, 75000, 75000, 75000]
    routing_mode = os.environ.get('ROUTING_MODE', '')
    baseline = os.environ.get('BASELINE', '0') == '1'
    round_robin = routing_mode == 'round-robin'
    if round_robin:
        baseline = True
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': rate,
        'tx_size': 512,
        'duration': duration,
        'rate_weights': rate_weights,
        'account_weights': account_weights,
        'warmup': warmup,
        'num_accounts': num_accounts,
    }
    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
    }
    try:
        ret = CloudLabBench(
            bench_params, node_params,
            manifest_file=manifest,
            username=username,
            baseline=baseline,
            round_robin=round_robin,
            latency_ms=int(latency_ms),
            primary_bw_kbps=primary_bw_kbps,
            worker_bws_kbps=worker_bws_kbps,
        ).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def cloudlab_install(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Install Rust and dependencies on all CloudLab machines '''
    try:
        CloudLabInstaller(manifest, username).install()
    except BenchError as e:
        Print.error(e)


@task
def cloudlab_kill(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Kill all processes on CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor
    try:
        mgr = CloudLabInstanceManager.make(manifest, username)
        hosts = mgr.all_ssh_hosts()
        Print.info(f'Killing processes on {len(hosts)} machines...')
        def _kill_one(host):
            try:
                Connection(host, user=username).run('tmux kill-server || true', hide=True)
            except Exception:
                pass
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            list(pool.map(_kill_one, hosts))
        Print.heading('Done.')
    except Exception as e:
        Print.error(BenchError('Failed to kill processes', e))

@task
def cloudlab_get_ssh(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Print SSH connection information for all CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    try:
        mgr = CloudLabInstanceManager.make(manifest, username)
        clients = mgr.client_ssh_hosts()
        for client in clients:
            Print.info(f"Client: {username}@{client}")
        validators = mgr.validator_ssh_hosts()
        for validator in validators:
            Print.info(f"Validator: {username}@{validator}")
    except Exception as e:
        Print.error(BenchError('Failed to get SSH information', e))


@task
def cloudlab_nettest(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Test bandwidth (iperf3) and pairwise latency on CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import json as _json
    import time
    try:
        mgr = CloudLabInstanceManager.make(manifest, username)
        v_ssh = mgr.validator_ssh_hosts()
        v_ips = mgr.validator_ips()
        c_ssh = mgr.client_ssh_hosts()
        all_ssh = v_ssh + c_ssh
        all_ips = v_ips + mgr.client_ips()
        n = len(all_ssh)
        labels = [f'node-{i}' for i in range(len(v_ssh))] + [f'client-{i}' for i in range(len(c_ssh))]

        def ssh(host):
            return Connection(host, user=username)

        # Kill any leftover iperf3 servers
        def _kill_iperf(host):
            try:
                ssh(host).run('killall iperf3 2>/dev/null || true', hide=True)
            except Exception:
                pass
        with ThreadPoolExecutor(max_workers=n) as pool:
            list(pool.map(_kill_iperf, all_ssh))

        # === Pairwise egress bandwidth test ===
        # Start one iperf3 server per machine (sequential tests, no port conflicts)
        Print.info('Starting iperf3 servers...')
        for host in all_ssh:
            ssh(host).run('iperf3 -s -p 5201 -D', hide=True)
        time.sleep(1)

        Print.info('Testing pairwise egress bandwidth (iperf3, 5s each)...')
        bw_matrix = [[None] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                if i == j:
                    bw_matrix[i][j] = 0.0
                    continue
                result = ssh(all_ssh[i]).run(
                    f'iperf3 -c {all_ips[j]} -p 5201 -t 5 -w 4M -J',
                    hide=True, warn=True,
                )
                try:
                    data = _json.loads(result.stdout)
                    bps = data['end']['sum_sent']['bits_per_second']
                except (KeyError, _json.JSONDecodeError, ValueError):
                    bps = 0
                bw_matrix[i][j] = bps / 1_000_000

        Print.info('')
        Print.info('Pairwise egress BW (Mbps, sender=row, receiver=col):')
        header = '{:<12s}'.format('') + ''.join('{:>12s}'.format(l) for l in labels)
        Print.info(header)
        for i in range(n):
            row = '{:<12s}'.format(labels[i])
            row += ''.join(
                '{:>12s}'.format('-') if i == j else '{:>12.1f}'.format(bw_matrix[i][j])
                for j in range(n)
            )
            Print.info(row)

        # Kill servers before ingress test (need multiple servers per target)
        with ThreadPoolExecutor(max_workers=n) as pool:
            list(pool.map(_kill_iperf, all_ssh))
        time.sleep(1)

        # === Total ingress bandwidth test ===
        # For each target, all other nodes send simultaneously
        Print.info('')
        Print.info('Testing total ingress bandwidth (all senders -> one target, 5s)...')
        for target in range(n):
            senders = [s for s in range(n) if s != target]
            # Start one iperf3 server per sender on the target (different ports)
            for k, _ in enumerate(senders):
                ssh(all_ssh[target]).run(
                    f'iperf3 -s -p {5201 + k} -D', hide=True,
                )
            time.sleep(0.5)
            # All senders blast simultaneously
            with ThreadPoolExecutor(max_workers=len(senders)) as pool:
                def _send(args):
                    k, s = args
                    r = ssh(all_ssh[s]).run(
                        f'iperf3 -c {all_ips[target]} -p {5201 + k} -t 5 -w 4M -J',
                        hide=True, warn=True,
                    )
                    try:
                        data = _json.loads(r.stdout)
                        return data['end']['sum_sent']['bits_per_second']
                    except (KeyError, _json.JSONDecodeError, ValueError):
                        return 0
                results = list(pool.map(_send, enumerate(senders)))
            total_mbps = sum(results) / 1_000_000
            per_sender = ', '.join(
                f'{labels[senders[k]]}={results[k]/1e6:.1f}'
                for k in range(len(senders))
            )
            Print.info(f'  {labels[target]}: {total_mbps:.1f} Mbps total ingress ({per_sender})')
            # Kill servers on target
            _kill_iperf(all_ssh[target])
            time.sleep(0.5)

        # === Latency test (pairwise RTT via ping) ===
        Print.info('Testing pairwise latency (ping, 5 probes each)...')
        rtt_matrix = [[None] * n for _ in range(n)]

        def _ping(i, j):
            if i == j:
                return i, j, 0.0
            result = ssh(all_ssh[i]).run(
                f'ping -c 5 -q {all_ips[j]}',
                hide=True, warn=True,
            )
            # Parse "rtt min/avg/max/mdev = ..."
            for line in result.stdout.splitlines():
                if 'avg' in line:
                    # e.g. "rtt min/avg/max/mdev = 49.5/50.1/50.8/0.4 ms"
                    stats = line.split('=')[1].strip().split('/')
                    avg_ms = float(stats[1])
                    return i, j, avg_ms
            assert False, f'ping {labels[i]} -> {labels[j]} failed: {result.stdout}'

        with ThreadPoolExecutor(max_workers=n * n) as pool:
            futures = []
            for i in range(n):
                for j in range(n):
                    futures.append(pool.submit(_ping, i, j))
            for f in as_completed(futures):
                i, j, rtt = f.result()
                rtt_matrix[i][j] = rtt

        # Print RTT matrix
        Print.info('')
        Print.info('RTT matrix (ms):')
        header = '{:<12s}'.format('') + ''.join('{:>12s}'.format(l) for l in labels)
        Print.info(header)
        for i in range(n):
            row = '{:<12s}'.format(labels[i])
            row += ''.join('{:>12.1f}'.format(rtt_matrix[i][j]) for j in range(n))
            Print.info(row)

        Print.heading('Network test complete.')
    except Exception as e:
        # Clean up iperf3
        for host in all_ssh:
            try:
                Connection(host, user=username).run('killall iperf3 2>/dev/null || true', hide=True)
            except Exception:
                pass
        Print.error(BenchError('Network test failed', e))