# Copyright(C) Facebook, Inc. and its affiliates.
import os
from fabric import task

from benchmark.local import LocalBench
from benchmark.docker_bench import DockerBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError


@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    rate = int(os.environ.get('RATE', 50_000))
    duration = int(os.environ.get('DURATION', 20))
    warmup = int(os.environ.get('WARMUP', 0))
    rate_weights_raw = os.environ.get('RATE_WEIGHTS')
    rate_weights = [int(w) for w in rate_weights_raw.split(',')] if rate_weights_raw else None
    tokio_threads = int(os.environ.get('TOKIO_THREADS', 0))  # 0 = use tokio default (num_cpus)
    open_loop = os.environ.get('OPEN_LOOP', 'false').lower() in ('true', '1', 'yes')
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': rate,
        'tx_size': 512,
        'duration': duration,
        'rate_weights': rate_weights,
        'warmup': warmup,
        'open_loop': open_loop,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'tokio_threads': tokio_threads,
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def docker(ctx, debug=True, bandwidth='10gbit', latency='0ms', jitter='0ms',
           cpus_per_validator=0, lan_bandwidth='100gbit'):
    ''' Run benchmarks in Docker containers with tc bandwidth shaping '''
    rate = int(os.environ.get('RATE', 50_000))
    duration = int(os.environ.get('DURATION', 20))
    warmup = int(os.environ.get('WARMUP', 0))
    rate_weights_raw = os.environ.get('RATE_WEIGHTS')
    rate_weights = [int(w) for w in rate_weights_raw.split(',')] if rate_weights_raw else None
    tokio_threads = int(os.environ.get('TOKIO_THREADS', 8))
    open_loop = os.environ.get('OPEN_LOOP', 'false').lower() in ('true', '1', 'yes')
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': rate,
        'tx_size': 512,
        'duration': duration,
        'rate_weights': rate_weights,
        'warmup': warmup,
        'open_loop': open_loop,
    }
    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
        'tokio_threads': tokio_threads,
    }
    try:
        ret = DockerBench(
            bench_params, node_params,
            bandwidth=bandwidth, latency=latency, jitter=jitter,
            cpus_per_validator=int(cpus_per_validator),
            lan_bandwidth=lan_bandwidth,
        ).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


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
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
