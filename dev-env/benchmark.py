#!/usr/bin/env python3
"""
Profile metadata_crawler.add with cProfile (cumtime) + yappi (per-thread wall time).

Outputs:
- cordex.prof.pstats     -> cProfile stats file (open with snakeviz or pstats)
- cordex.cprofile.txt    -> human-readable top-N by cumulative time
- cordex.yappi.funcs.txt -> yappi hot functions (ttot = inclusive)
- cordex.yappi.threads.txt -> yappi per-thread summary

Usage:
  python profile_cordex.py --top 60
"""
import argparse
import atexit
import cProfile
import io
import os
import pstats
import sys
import time
from pathlib import Path
from typing import Any, Optional

from metadata_crawler import add

# --- your workload ---------------------------------------------------------


def run_workload(
    num_files: int,
    data_set: str = "cordex-benchmark",
    config_file: Optional[str] = None,
) -> None:
    # Keep the same arguments you currently use:

    env = os.environ.copy()
    config_file = Path(
        config_file or Path(__file__).parent.parent / "drs_config.toml"
    )
    try:
        os.environ["MDC_MAX_FILES"] = str(num_files)
        add(
            "data.yml",
            config_file=config_file,
            batch_size=2_000,
            data_set=[data_set],
            verbosity=0,
            catalogue_backend="jsonlines",
            data_store_prefix="benchmark",
        )
    finally:
        os.environ = env


# --- profiling harness -----------------------------------------------------


def run_with_cprofile(top: int, num_files: int, **kwargs: Any) -> None:
    """
    Run workload once, collect cProfile simultaneously.
    cProfile gives cumtime;
    """
    prof = cProfile.Profile()

    t0 = time.perf_counter()
    prof.enable()
    try:
        run_workload(num_files, **kwargs)
    finally:
        prof.disable()
        t1 = time.perf_counter()

        # --- Dump cProfile as .pstats file
        prof.dump_stats("cordex.prof.pstats")

        # Text summary (top-N by cumtime)
        s = io.StringIO()
        ps = pstats.Stats(prof, stream=s).strip_dirs().sort_stats("cumtime")
        ps.print_stats(top)
        text = s.getvalue()
        print("\n=== cProfile (cumtime, top {} funcs) ===".format(top))
        print(text)
        # Write with safe error handling for any stray surrogates
        with open(
            "cordex.cprofile.txt", "w", encoding="utf-8", errors="replace"
        ) as f:
            f.write(text)

        elapsed = t1 - t0
        print(f"\nTotal wall time: {elapsed:.2f} s")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Profile metadata_crawler.add")
    parser.add_argument(
        "--top", type=int, default=50, help="Top N rows to show in cProfile table"
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=10_000,
        help="Number of max files to ingest.",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=None,
        help="Config file.",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="cordex-benchmark",
        help="The dataset that is tests.",
    )
    args = parser.parse_args(argv)

    # Make sure we flush all reports even on unexpected exit
    @atexit.register
    def _goodbye():
        sys.stdout.flush()
        sys.stderr.flush()

    run_with_cprofile(
        top=args.top,
        num_files=args.num_files,
        data_set=args.dataset,
        config_file=args.config_file,
    )


if __name__ == "__main__":
    main()
