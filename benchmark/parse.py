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

r = LogParser.process(
                PathMaker.logs_path(), faults=0, duration=25, warmup=5
            )
print(r.result())