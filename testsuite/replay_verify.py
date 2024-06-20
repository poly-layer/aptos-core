#!/usr/bin/env python3

# Copyright © Aptos Foundation
# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import shutil
import sys
from multiprocessing import Pool, freeze_support
from typing import Tuple
from collections import deque


from verify_core.common import clear_artifacts, query_backup_latest_version


TESTNET_RANGES = [
    (1032373939, 1048163412),
    (1048163412, 1063952885),
    (1063952885, 1079742358),
    (1079742358, 1095531831),
    (1095531831, 1111321304),
    (1111321304, 1127110777),
    (1127110777, 1142900250),
    (1142900250, 1158689723),
    (1158689723, 1174479196),
    (1174479196, 1190268669),
    (1190268669, 1206058142),
    (1206058142, 1221847615),
    (1221847615, 1237637088),
    (1237637088, 1253426561),
    (1253426561, 1269216034),
    (1269216034, 1285005507),
    (1285005507, 1300794980),
    (1300794980, 1316584453),
    (1316584453, sys.maxsize),
]


MAINNET_RANGES = [
    (676365253, 692154726),
    (692154726, 707944199),
    (707944199, 723733672),
    (723733672, 739523145),
    (739523145, 755312618),
    (755312618, 771102091),
    (771102091, 786891564),
    (786891564, 802681037),
    (802681037, 818470510),
    (818470510, 834259983),
    (834259983, 850049456),
    (850049456, 865838929),
    (865838929, 881628402),
    (881628402, 897417875),
    (897417875, 913207348),
    (913207348, 928996821),
    (928996821, 944786294),
    (944786294, 960575767),
    (960575767, sys.maxsize),
]


# retry the replay_verify_partition if it fails
def retry_replay_verify_partition(func, *args, **kwargs) -> Tuple[int, int, bytes]:
    (partition_number, code, msg) = (0, 0, b"")
    NUM_OF_RETRIES = 6
    for i in range(1, NUM_OF_RETRIES + 1):
        print(f"try {i}")
        (partition_number, code, msg) = func(*args, **kwargs)
        # let's only not retry on txn error and success case,
        if code == 2 or code == 0:
            break
    return (partition_number, code, msg)


def replay_verify_partition(
    n: int,
    N: int,
    history_start: int,
    per_partition: int,
    latest_version: int,
    txns_to_skip: Tuple[int],
    backup_config_template_path: str,
) -> Tuple[int, int, bytes]:
    """
    Run replay-verify for a partition of the backup, returning a tuple of the (partition number, return code)

    n: partition number
    N: total number of partitions
    history_start: start version of the history to verify
    per_partition: number of versions per partition
    latest_version: last version to verify
    txns_to_skip: list of transactions to skip
    backup_config_template_path: path to the backup config template
    """
    end = history_start + n * per_partition
    if n == N and end < latest_version:
        end = latest_version

    start = end - per_partition
    partition_name = f"run_{n}_{start}_{end}"

    print(f"[partition {n}] spawning {partition_name}")
    if not os.path.exists(partition_name):
        os.mkdir(partition_name)
        # the metadata cache is shared across partitions and downloaded when querying the latest version.
        shutil.copytree("metadata-cache", f"{partition_name}/metadata-cache")

    txns_to_skip_args = [f"--txns-to-skip={txn}" for txn in txns_to_skip]

    # run and print output
    process = subprocess.Popen(
        [
            "target/release/aptos-debugger",
            "aptos-db",
            "replay-verify",
            *txns_to_skip_args,
            "--concurrent-downloads",
            "8",
            "--replay-concurrency-level",
            "2",
            "--metadata-cache-dir",
            f"./{partition_name}/metadata-cache",
            "--target-db-dir",
            f"./{partition_name}/db",
            "--start-version",
            str(start),
            "--end-version",
            str(end),
            "--lazy-quit",
            "--command-adapter-config",
            backup_config_template_path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # redirect stderr to stdout
    )
    if process.stdout is None:
        raise Exception(f"[partition {n}] stdout is None")
    last_lines = deque(maxlen=10)
    for line in iter(process.stdout.readline, b""):
        print(f"[partition {n}] {line}", flush=True)
        last_lines.append(line)
    process.communicate()

    return (n, process.returncode, b"\n".join(last_lines))


def main(runner_no=None, runner_cnt=None, start_version=None, end_version=None):
    # collect all required ENV variables
    REQUIRED_ENVS = [
        "BUCKET",
        "SUB_DIR",
        "HISTORY_START",
        "TXNS_TO_SKIP",
        "BACKUP_CONFIG_TEMPLATE_PATH",
    ]

    if not all(env in os.environ for env in REQUIRED_ENVS):
        raise Exception("Missing required ENV variables")

    # the runner may have small overlap at the boundary to prevent missing any transactions
    runner_mapping = (
        TESTNET_RANGES if "testnet" in os.environ["BUCKET"] else MAINNET_RANGES
    )

    # by default we only have 1 runner
    if runner_no is None or runner_cnt is None:
        runner_no = 0
        runner_cnt = 1
        runner_mapping = [[runner_mapping[0][0], runner_mapping[-1][1]]]

    assert (
        runner_no >= 0 and runner_no < runner_cnt
    ), "runner_no must be between 0 and runner_cnt"

    TXNS_TO_SKIP = [int(txn) for txn in os.environ["TXNS_TO_SKIP"].split(" ")]
    BACKUP_CONFIG_TEMPLATE_PATH = os.environ["BACKUP_CONFIG_TEMPLATE_PATH"]

    if not os.path.exists(BACKUP_CONFIG_TEMPLATE_PATH):
        raise Exception("BACKUP_CONFIG_TEMPLATE_PATH does not exist")
    with open(BACKUP_CONFIG_TEMPLATE_PATH, "r") as f:
        config = f.read()
        if "aws" in config and shutil.which("aws") is None:
            raise Exception("Missing required AWS CLI for pulling backup data from S3")

    if os.environ.get("REUSE_BACKUP_ARTIFACTS", "true") != "true":
        print("[main process] clearing existing backup artifacts")
        clear_artifacts()
    else:
        print("[main process] skipping clearing backup artifacts")

    assert runner_cnt == len(
        runner_mapping
    ), "runner_cnt must match the number of runners in the mapping"
    runner_start = runner_mapping[runner_no][0]
    runner_end = runner_mapping[runner_no][1]
    latest_version = query_backup_latest_version(BACKUP_CONFIG_TEMPLATE_PATH)
    if runner_no == runner_cnt - 1:
        runner_end = latest_version
        if runner_end is None:
            raise Exception("Failed to query latest version from backup")
    print("runner start %d end %d" % (runner_start, runner_end))
    if start_version is not None and end_version is not None:
        runner_start = start_version
        runner_end = end_version

    # run replay-verify in parallel
    N = 16
    PER_PARTITION = (runner_end - runner_start) // N

    with Pool(N) as p:
        all_partitions = p.starmap(
            retry_replay_verify_partition,
            [
                (
                    replay_verify_partition,
                    n,
                    N,
                    runner_start,
                    PER_PARTITION,
                    runner_end,
                    TXNS_TO_SKIP,
                    BACKUP_CONFIG_TEMPLATE_PATH,
                )
                for n in range(1, N + 1)
            ],
        )

    print("[main process] finished")

    err = False
    for partition_num, return_code, msg in all_partitions:
        if return_code != 0:
            print("======== ERROR ========")
            print(
                f"ERROR: partition {partition_num} failed with exit status {return_code}, {msg})"
            )
            err = True

    if err:
        sys.exit(1)


if __name__ == "__main__":
    freeze_support()
    (runner_no, runner_cnt) = (
        (int(sys.argv[1]), int(sys.argv[2])) if len(sys.argv) > 2 else (None, None)
    )
    main(runner_no, runner_cnt)
