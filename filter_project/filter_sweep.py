#!/usr/bin/env python3
"""Filter-only measurements with filter_bench (no database needed).

Two sweeps, written as one CSV row per filter_bench run:

  sawtooth     Cuckoo FPR over a fine grid of bits/key settings, showing the
               steps caused by whole-bit fingerprint widths. Each row carries
               the fingerprint width, load factor and model FPR.
  equal_bits   Bloom, Ribbon and Cuckoo at the same REAL bits/key. Each
               filter's bits/key setting is first calibrated (bisection on
               filter_bench's "Bits/key stored"), because the settings do not
               mean the same thing: Ribbon's is Bloom-equivalent (~0.7x real)
               and Bloom rounds each filter up to whole cache lines.

Every row records build ns/key (construction cost, hashing included), stored
bits/key, FPR, and query ns/op for filter_bench's quick modes ("Single
filter" = hot in CPU cache, "Random filter" = a different filter per query).

Filter implementations (filter_bench -impl): 1 = Bloom (format_version 5,
what NewBloomFilterPolicy builds), 2 = Standard128Ribbon (what db_bench's
ribbon with -bloom_before_level=-1 builds), 3 = cuckoo.

Defaults mirror the 10M-key base DB: ~1910 keys per SST filter, 64-byte keys.

Usage (from the repository root, with a release filter_bench built):
  python3 filter_project/filter_sweep.py calibrate
  python3 filter_project/filter_sweep.py run [--only sawtooth|equal_bits]
  python3 filter_project/filter_sweep.py run --dry-run

`run` is resumable: rows already in the CSV (same run_id) are skipped. The
run_id includes the host and the filter_bench parameters, so results from
different machines can share a file without colliding, but construction and
query times are only comparable within one host.
"""

import argparse
import csv
import datetime
import math
import os
import re
import socket
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT = os.path.join(REPO, "filter_project")

IMPLS = {"bloom": 1, "ribbon": 2, "cuckoo": 3}

# Cuckoo constants, mirrored from table/block_based/cuckoo_filter_policy.cc.
CUCKOO_SLOTS = 4
CUCKOO_MAX_LOAD = 0.95
CUCKOO_MIN_FP, CUCKOO_MAX_FP = 4, 32

FIELDS = [
    "run_id", "experiment", "filter_type", "impl", "config_bits",
    "target_real_bits", "real_bits", "build_ns_per_key", "fp_rate_pct",
    "query_single_filter_ns", "query_random_filter_ns", "num_filters",
    "total_size_mb", "cuckoo_fp_bits", "cuckoo_load", "cuckoo_model_fp_pct",
    "repeat", "seed", "keys_per_filter", "vary_ratio", "key_size",
    "m_queries", "working_mem_mb", "host", "timestamp", "log",
]

CALIB_FIELDS = [
    "filter_type", "target_real_bits", "config_bits", "real_bits",
    "keys_per_filter", "vary_ratio", "key_size", "host", "timestamp",
]

PATTERNS = {
    "build_ns_per_key": r"Build avg ns/key: ([0-9.eE+-]+)",
    "num_filters": r"Number of filters: (\d+)",
    "total_size_mb": r"Total size \(MB\): ([0-9.eE+-]+)",
    "real_bits": r"Bits/key stored: ([0-9.eE+-]+)",
    "fp_rate_pct": r"Average FP rate %: ([0-9.eE+-]+)",
    "query_single_filter_ns": r"Single filter net ns/op: ([0-9.eE+-]+)",
    "query_random_filter_ns": r"Random filter net ns/op: ([0-9.eE+-]+)",
}


def cuckoo_model(config_bits, real_bits):
    """Fingerprint width, load factor and approximate FPR of a cuckoo filter.

    The policy picks f = floor(0.95 * bits/key) (in millibits, like the C++
    code) and sizes the table to the space budget, so load ~ f / real bits.
    A query checks 2 buckets of 4 slots, so FPR ~ 1 - (1 - 2^-f)^(8 * load).
    """
    millibits = int(config_bits * 1000.0 + 0.500001)
    f = int(millibits * CUCKOO_MAX_LOAD) // 1000
    f = max(CUCKOO_MIN_FP, min(CUCKOO_MAX_FP, f))
    load = min(CUCKOO_MAX_LOAD, f / real_bits) if real_bits > 0 else 0.0
    fpr = 1.0 - (1.0 - 2.0 ** -f) ** (2 * CUCKOO_SLOTS * load)
    return f, load, 100.0 * fpr


def frange(spec):
    """'6:24:0.25' -> [6.0, 6.25, ..., 24.0]."""
    start, stop, step = (float(x) for x in spec.split(":"))
    n = int(round((stop - start) / step))
    return [round(start + i * step, 6) for i in range(n + 1)]


def fmt(x):
    """Compact, stable text for a bits/key setting (also used in run_ids)."""
    return ("%.3f" % x).rstrip("0").rstrip(".")


class Bench:
    def __init__(self, args):
        self.args = args
        self.host = socket.gethostname().split(".")[0]
        self.log_dir = os.path.join(args.results_dir, "logs", "filter_bench")

    def cmd(self, impl, bits, seed, m_queries, working_mem_mb):
        a = self.args
        cmd = [
            a.filter_bench, "-quick", "-allow_bad_fp_rate",
            "-impl=%d" % impl, "-bits_per_key=%s" % fmt(bits),
            "-average_keys_per_filter=%d" % a.keys_per_filter,
            "-vary_key_count_ratio=%s" % a.vary_ratio,
            "-key_size=%d" % a.key_size, "-seed=%d" % seed,
            "-m_queries=%s" % m_queries,
            "-working_mem_size_mb=%s" % working_mem_mb,
        ]
        if a.nice:
            cmd = ["nice", "-n", str(a.nice)] + cmd
        return cmd

    def run(self, impl, bits, seed, m_queries, working_mem_mb, log_name=None):
        cmd = self.cmd(impl, bits, seed, m_queries, working_mem_mb)
        if self.args.verbose:
            print("  $ " + " ".join(cmd), flush=True)
        proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT,
                              universal_newlines=True)
        out = proc.stdout
        log_path = ""
        if log_name:
            os.makedirs(self.log_dir, exist_ok=True)
            log_path = os.path.join(self.log_dir, log_name + ".log")
            with open(log_path, "w") as f:
                f.write(" ".join(cmd) + "\n\n" + out)
        if proc.returncode != 0:
            sys.exit("filter_bench failed (exit %d):\n%s\n%s"
                     % (proc.returncode, " ".join(cmd), out[-2000:]))
        res = {}
        for key, pat in PATTERNS.items():
            m = re.search(pat, out)
            if m:
                res[key] = float(m.group(1))
        if "real_bits" not in res:
            sys.exit("could not parse filter_bench output:\n" + out[-2000:])
        return res, os.path.relpath(log_path, PROJECT) if log_path else ""

    def real_bits(self, ftype, config):
        # Build only (no queries) on a smaller working set: stored bits/key
        # depends on keys per filter, not on how many filters there are.
        res, _ = self.run(IMPLS[ftype], config, seed=0, m_queries=0,
                          working_mem_mb=self.args.calib_mem_mb)
        return res["real_bits"]

    def calibrate(self, ftype, target):
        """Setting whose stored bits/key is closest to target.

        Stored bits/key is a non-decreasing step function of the setting, so
        bisect for the smallest setting reaching the target, then compare it
        with the setting just below. Settings have 0.001 resolution.
        """
        lo, hi = (0.4 * target, 2.5 * target) if ftype == "ribbon" else (
            max(0.5, target - 3.0), target + 3.0)
        lo_m, hi_m = int(lo * 1000), int(hi * 1000)
        if self.real_bits(ftype, hi_m / 1000.0) < target:
            sys.exit("calibration range too small for %s at %s"
                     % (ftype, target))
        cache = {}

        def real(m):
            if m not in cache:
                cache[m] = self.real_bits(ftype, m / 1000.0)
            return cache[m]

        while hi_m - lo_m > 1:
            mid = (lo_m + hi_m) // 2
            if real(mid) >= target:
                hi_m = mid
            else:
                lo_m = mid
        best = min((hi_m, lo_m), key=lambda m: abs(real(m) - target))
        return best / 1000.0, real(best)


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def append_row(path, fields, row):
    new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new:
            w.writeheader()
        w.writerow(row)


def now():
    return datetime.datetime.now().isoformat(timespec="seconds")


def calibration_table(bench, args):
    """{(ftype, target): (config, real)}, loaded or computed, for this host."""
    path = os.path.join(args.results_dir, "filter_calibration.csv")
    have = {}
    for r in read_csv(path):
        if (r["host"] == bench.host
                and int(r["keys_per_filter"]) == args.keys_per_filter
                and float(r["vary_ratio"]) == args.vary_ratio
                and int(r["key_size"]) == args.key_size):
            have[(r["filter_type"], float(r["target_real_bits"]))] = (
                float(r["config_bits"]), float(r["real_bits"]))
    table = {}
    for ftype in args.filters:
        for target in args.targets:
            key = (ftype, target)
            if key not in have:
                if args.dry_run:
                    print("would calibrate %s at %s real bits/key"
                          % (ftype, fmt(target)))
                    continue
                config, real = bench.calibrate(ftype, target)
                have[key] = (config, real)
                append_row(path, CALIB_FIELDS, {
                    "filter_type": ftype, "target_real_bits": target,
                    "config_bits": config, "real_bits": real,
                    "keys_per_filter": args.keys_per_filter,
                    "vary_ratio": args.vary_ratio, "key_size": args.key_size,
                    "host": bench.host, "timestamp": now(),
                })
            table[key] = have[key]
            config, real = have[key]
            print("  %-6s target %6s  setting %7s  -> real %.3f"
                  % (ftype, fmt(target), fmt(config), real), flush=True)
    return table


def planned_runs(bench, args):
    """(experiment, ftype, config, target) for every requested measurement."""
    runs = []
    if "equal_bits" in args.only:
        table = calibration_table(bench, args)
        for target in args.targets:
            for ftype in args.filters:
                if (ftype, target) in table:
                    runs.append(("equal_bits", ftype,
                                 table[(ftype, target)][0], target))
    if "sawtooth" in args.only:
        for config in frange(args.sawtooth):
            runs.append(("sawtooth", "cuckoo", config, ""))
    return runs


def cmd_calibrate(bench, args):
    calibration_table(bench, args)


def cmd_run(bench, args):
    results = os.path.join(args.results_dir, "filter_bench.csv")
    done = {r["run_id"] for r in read_csv(results)}
    params = "k%d-v%s-s%d-q%s-w%s" % (args.keys_per_filter, args.vary_ratio,
                                      args.key_size, args.m_queries,
                                      args.working_mem_mb)
    todo = []
    for exp, ftype, config, target in planned_runs(bench, args):
        for rep in range(args.repeats):
            run_id = "%s|%s|%s-%s|r%d|%s" % (bench.host, exp, ftype,
                                             fmt(config), rep, params)
            if run_id not in done:
                todo.append((run_id, exp, ftype, config, target, rep))
    print("%d runs to do (%d already in %s)"
          % (len(todo), len(done), os.path.relpath(results)), flush=True)
    for i, (run_id, exp, ftype, config, target, rep) in enumerate(todo, 1):
        seed = args.seed + rep
        if args.dry_run:
            print(" ".join(bench.cmd(IMPLS[ftype], config, seed,
                                     args.m_queries, args.working_mem_mb)))
            continue
        log_name = "%s_%s-%s_r%d" % (exp, ftype, fmt(config), rep)
        res, log = bench.run(IMPLS[ftype], config, seed, args.m_queries,
                             args.working_mem_mb, log_name)
        row = dict.fromkeys(FIELDS, "")
        row.update(res)
        row.update({
            "run_id": run_id, "experiment": exp, "filter_type": ftype,
            "impl": IMPLS[ftype], "config_bits": config,
            "target_real_bits": target, "repeat": rep, "seed": seed,
            "keys_per_filter": args.keys_per_filter,
            "vary_ratio": args.vary_ratio, "key_size": args.key_size,
            "m_queries": args.m_queries,
            "working_mem_mb": args.working_mem_mb, "host": bench.host,
            "timestamp": now(), "log": log,
        })
        if ftype == "cuckoo":
            f, load, model = cuckoo_model(config, res["real_bits"])
            row.update({"cuckoo_fp_bits": f, "cuckoo_load": round(load, 4),
                        "cuckoo_model_fp_pct": round(model, 5)})
        append_row(results, FIELDS, row)
        print("[%d/%d] %-10s %-6s %7s  real %6.3f  FPR %8.4f%%  build %6.1f "
              "ns/key  query %5.1f/%5.1f ns"
              % (i, len(todo), exp, ftype, fmt(config), res["real_bits"],
                 res.get("fp_rate_pct", math.nan),
                 res.get("build_ns_per_key", math.nan),
                 res.get("query_single_filter_ns", math.nan),
                 res.get("query_random_filter_ns", math.nan)), flush=True)


def csv_floats(s):
    return [float(x) for x in s.split(",") if x]


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("command", choices=["calibrate", "run"])
    p.add_argument("--filter-bench", default=os.path.join(REPO, "filter_bench"),
                   help="filter_bench binary (release build)")
    p.add_argument("--results-dir", default=os.path.join(PROJECT, "results"))
    p.add_argument("--only", default="equal_bits,sawtooth",
                   help="comma list of: equal_bits, sawtooth")
    p.add_argument("--filters", default="bloom,ribbon,cuckoo",
                   help="filters for equal_bits")
    p.add_argument("--targets", type=csv_floats, default="6,8,10,12,16,20",
                   help="real bits/key targets for equal_bits")
    p.add_argument("--sawtooth", default="6:24:0.25",
                   help="cuckoo settings start:stop:step")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--seed", type=int, default=1,
                   help="seed of repeat 0; repeat r uses seed + r")
    p.add_argument("--keys-per-filter", type=int, default=1910,
                   help="keys per filter (10M-key base DB: ~1910 per SST)")
    p.add_argument("--vary-ratio", type=float, default=0.0,
                   help="filter_bench -vary_key_count_ratio")
    p.add_argument("--key-size", type=int, default=64)
    p.add_argument("--m-queries", type=float, default=50,
                   help="millions of queries (quick mode divides by 7; half "
                        "are absent keys used for the FPR)")
    p.add_argument("--working-mem-mb", type=float, default=200,
                   help="total filter memory per run")
    p.add_argument("--calib-mem-mb", type=float, default=20,
                   help="total filter memory per calibration build")
    p.add_argument("--nice", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    if isinstance(args.targets, str):
        args.targets = csv_floats(args.targets)
    args.only = set(args.only.split(","))
    bad = args.only - {"equal_bits", "sawtooth"}
    if bad:
        p.error("unknown --only value(s): " + ", ".join(sorted(bad)))
    args.filters = [f for f in args.filters.split(",") if f]
    bad = set(args.filters) - set(IMPLS)
    if bad:
        p.error("unknown filter(s): " + ", ".join(sorted(bad)))
    if not os.access(args.filter_bench, os.X_OK):
        p.error("filter_bench not found at %s; build it with "
                "DEBUG_LEVEL=0 make filter_bench" % args.filter_bench)

    bench = Bench(args)
    {"calibrate": cmd_calibrate, "run": cmd_run}[args.command](bench, args)


if __name__ == "__main__":
    main()
