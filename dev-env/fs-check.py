#!/usr/bin/env python3
"""Run a file system check."""
import argparse
import csv
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Tuple, Union

import numpy as np
import yaml

from metadata_crawler import add


def run_add(
    config_file: Path, dataset: str, concurrency: int, temp_dir: Path
) -> Tuple[float, int]:
    """Run mdc add."""
    start = time.perf_counter()
    cat_file = temp_dir / "data.yml"
    add(
        config_file,
        store=cat_file,
        data_set=[dataset],
        verbosity=0,
        fail_under=-1,
        n_proc=8,
        scan_concurrency=concurrency,
    )
    end = time.perf_counter()
    n_files = yaml.safe_load(cat_file.read_text())["metadata"]["indexed_objects"]
    return end - start, n_files


def write_header(path: Path, header: List[str]):
    path.parent.mkdir(exist_ok=True, parents=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)


def append_row(path: Path, values: Dict[str, Union[str, int, float]]):
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(values.values())
        f.flush()
        os.fsync(f.fileno())


def main() -> None:

    parser = argparse.ArgumentParser(
        description="Apply a file system performance test."
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=100_000,
        help="Number of max files to ingest.",
    )
    parser.add_argument(
        "-n",
        "--num-itt",
        type=int,
        help="Number of itterations",
        default=20,
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=None,
        help="Config file.",
    )
    parser.add_argument(
        "--concurrency", type=int, default=[8, 1048, 100], nargs=3
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        type=Path,
        default=Path.cwd() / "mdc-fs-benchmark",
        help="Output directory to store the result.",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="cordex",
        help="The dataset that is tests.",
    )
    args = parser.parse_args()
    host = socket.gethostname()
    header = [
        "host",
        "starttime",
        "endtime",
        "num_files",
        "itt",
        "runtime",
        "concurrency",
    ]
    now = datetime.now().strftime("%a_%F")
    outfile = (
        args.out_dir / f"fs-performance-test-{args.dataset}-{host}-{now}.csv"
    )
    env = os.environ.copy()
    old_stdout = sys.stdout
    curr_dir = Path.cwd()
    cfg_file = args.config_file.absolute()
    with TemporaryDirectory() as td:
        write_header(outfile, header)
        stdout = (Path(td) / ".out").open("w")
        sys.stdout = stdout
        try:
            result = {}
            os.environ["MDC_MAX_FILES"] = str(args.num_files)
            os.environ["MDC_INTERACTIVE"] = "0"
            for cur in np.linspace(*args.concurrency).astype(int):
                for num in range(args.num_itt):
                    result["host"] = host
                    result["itt"] = num + 1
                    result["concurrency"] = cur
                    result["startime"] = datetime.now().isoformat()
                    dt, num_files = run_add(cfg_file, args.dataset, cur, Path(td))
                    result["endtime"] = datetime.now().isoformat()
                    result["num_files"] = num_files
                    result["runtime"] = dt
                    append_row(outfile, result)
        finally:
            os.environ = env
            sys.stdout = old_stdout
            os.chdir(curr_dir)


if __name__ == "__main__":
    main()
