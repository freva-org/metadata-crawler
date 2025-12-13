#!/usr/bin/env python3
"""Run a file system check."""
import argparse
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Tuple

import numpy as np
import pandas as pd
import yaml

from metadata_crawler import add


def run_add(
    config_file: Path, dataset: str, concurrency: int, temp_dir: Path
) -> Tuple[float, int]:
    """Run mdc add."""
    start = time.perf_counter()
    add(
        config_file,
        store=temp_dir / "data.yml",
        data_set=[dataset],
        verbosity=0,
        fail_under=-1,
        scan_concurrency=concurrency,
    )
    end = time.perf_counter()
    n_files = yaml.safe_load(Path("data.yml").read_text())["metadata"][
        "indexed_objects"
    ]
    return end - start, n_files


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
    result = {
        "host": [],
        "startime": [],
        "endtime": [],
        "num_files": [],
        "itt": [],
        "runtime": [],
        "concurrency": [],
    }
    env = os.environ.copy()
    old_stdout = sys.stdout
    curr_dir = Path.cwd()
    cfg_file = args.config_file.absolute()
    with TemporaryDirectory() as td:
        stdout = (Path(td) / ".out").open("w")
        sys.stdout = stdout
        try:
            os.environ["MDC_MAX_FILES"] = str(args.num_files)
            os.environ["MDC_INTERACTIVE"] = "0"
            for cur in np.linspace(*args.concurrency).astype(int):
                for num in range(args.num_itt):
                    result["host"].append(host)
                    result["itt"].append(num + 1)
                    result["concurrency"].append(cur)
                    result["startime"].append(datetime.now())
                    dt, num_files = run_add(cfg_file, args.dataset, cur, Path(td))
                    result["endtime"].append(datetime.now().isoformat())
                    result["num_files"].append(num_files)
                    result["runtime"].append(dt)
        finally:
            os.environ = env
            sys.stdout = old_stdout
    os.chdir(curr_dir)
    now = datetime.now().strftime("%a_%F")
    args.out_dir.mkdir(exist_ok=True, parents=True)
    outfile = (
        args.out_dir / f"fs-performance-test-{args.dataset}-{host}-{now}.csv"
    )
    pd.DataFrame(result).to_csv(outfile)


if __name__ == "__main__":
    main()
