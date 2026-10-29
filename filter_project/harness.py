#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).
"""Benchmark harness comparing SST filters (Bloom, Ribbon, Cuckoo) in RocksDB.

One dataset is loaded once (the "base" DB). For each filter configuration, a
copy of the base is made and every SST file is rewritten in place with
`db_bench -benchmarks=rebuildfilters`, which changes only the filter blocks.
The harness verifies that each copy has exactly the base's LSM shape (files,
entries and data bytes per level) before using it, so every configuration
answers lookups by touching the same files.

Read experiments open each copy with -readonly (the DB directory is checked to
be unchanged after every run), warm up with one pass of lookups, clear the
statistics with resetstats, then measure a second pass.

Usage (run from anywhere; paths are relative to the current directory):
  harness.py plan                      # what would run, and how much
  harness.py load                      # build the base DB (once)
  harness.py run [--only E1,E3] [--filters bloom-10,cuckoo-10]
  harness.py run --dry-run             # print db_bench commands only

Results are appended to <results>/results.csv (one row per measured run) and
<results>/filters.csv (one row per filter rebuild). `run` is resumable: runs
already in results.csv are skipped.

Common options: --db-root DIR (scratch space for DBs; needs ~3x the base DB
size), --db-bench PATH, --num N (override key count, e.g. for a quick test),
--repeats N, --nice N (lower CPU priority on shared machines).
"""

import argparse
import csv
import datetime
import hashlib
import itertools
import json
import os
import re
import resource
import shutil
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))

FILTER_NAME_RE = re.compile(r"^(bloom|ribbon|cuckoo)-(\d+(?:\.\d+)?)$")
MIXED_RE = re.compile(r"^mixed-(0?\.\d+)$")
SHAPE_LINE_RE = re.compile(
    r"^rebuildfilters (before|after) L(\d+): # files=(\d+); (.*)$")
REWROTE_RE = re.compile(r"rewrote (\d+) files in ([\d.]+) s")
OPS_RE = re.compile(
    r"^(readrandom|readmissing)\s*:\s*([\d.]+) micros/op (\d+) ops/sec")
FOUND_RE = re.compile(r"\((\d+) of (\d+) found\)")
PCT_RE = re.compile(r"Percentiles: P50: ([\d.]+) P75: ([\d.]+) P99: ([\d.]+) "
                    r"P99\.9: ([\d.]+) P99\.99: ([\d.]+)")
TICKER_RE = re.compile(r"^(rocksdb\.[\w.]+) COUNT : (\d+)\s*$")

# Per-level table properties that must match the base DB exactly. Filter
# size is expected to differ; index size is not compared because it does not
# affect which files a lookup touches (and is checked indirectly by data).
SHAPE_KEYS = ["files", "# entries", "# data blocks", "data block size",
              "raw key size", "raw value size"]

RESULT_COLUMNS = [
    "run_id", "experiment", "filter", "filter_type", "filter_bits_config",
    "repeat", "workload", "benchmark", "threads", "cache_size",
    "read_random_exp_range", "reads_per_thread", "lookups",
    "ops_per_sec", "micros_per_op",
    "p50_us", "p75_us", "p99_us", "p999_us", "p9999_us",
    "found_fraction", "expected_found_fraction",
    "filter_checks", "filter_useful", "filter_positive",
    "filter_true_positive", "fpr", "filter_checks_per_lookup",
    "false_positives_per_lookup",
    "bc_data_hit", "bc_data_miss", "bc_data_miss_ratio",
    "bc_index_hit", "bc_index_miss", "bc_index_miss_ratio",
    "bc_filter_hit", "bc_filter_miss", "bc_filter_miss_ratio",
    "data_misses_per_lookup",
    "filter_bytes_total", "filter_bits_per_key_actual",
    "params", "timestamp", "host", "cpus", "db_bench_version", "log",
    "command",
]

FILTER_COLUMNS = [
    "filter", "filter_type", "filter_bits_config", "filter_bytes_total",
    "entries_total", "filter_bits_per_key_actual", "files_rewritten",
    "rebuild_seconds", "per_level_filter_bytes", "timestamp", "host",
    "db_bench_version", "log",
]


def die(msg):
    sys.exit(f"harness: error: {msg}")


def fmt_flag(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def to_args(flags):
    return [f"-{k}={fmt_flag(v)}" for k, v in flags.items()]


def filter_flags(name):
    """db_bench flags for a filter name such as bloom-10 or none."""
    if name == "none":
        return {"bloom_bits": 0}
    m = FILTER_NAME_RE.match(name)
    if not m:
        die(f"bad filter name {name!r}; expected none or "
            f"<bloom|ribbon|cuckoo>-<bits>, e.g. cuckoo-7.5")
    ftype, bits = m.groups()
    flags = {"filter_type": ftype, "bloom_bits": bits}
    if ftype == "ribbon":
        # Ribbon at every level, including L0 flushes (the default would
        # use Bloom for L0)
        flags["bloom_before_level"] = -1
    return flags


def workload_spec(workload, num):
    """(db_bench benchmark, extra flags, expected fraction of keys found)."""
    if workload == "nonempty":
        return "readrandom", {"num": num}, 1.0
    if workload == "empty":
        return "readmissing", {"num": num}, 0.0
    m = MIXED_RE.match(workload)
    if m:
        missing = float(m.group(1))
        if not 0.0 < missing < 1.0:
            die(f"bad workload {workload!r}: fraction must be in (0, 1)")
        # Missing keys sort between existing ones (as in readmissing), so
        # only filters or data block reads can rule them out. (Widening
        # -num instead would make them larger than every key, and SST key
        # ranges alone would exclude them.)
        return "readrandom", {"num": num,
                              "read_missing_fraction": missing}, \
            1.0 - missing
    die(f"bad workload {workload!r}; expected nonempty, empty or mixed-F")


def auto_threads():
    cpus = os.cpu_count() or 1
    counts, t = [], 1
    while t < cpus:
        counts.append(t)
        t *= 2
    counts.append(cpus)
    return counts


def parse_props(text):
    props = {}
    for part in text.split("; "):
        if "=" in part:
            k, v = part.split("=", 1)
            props[k.strip()] = v.strip()
    return props


def parse_shapes(output):
    """{'before': {level: props}, 'after': {...}} from rebuildfilters output."""
    shapes = {"before": {}, "after": {}}
    for line in output.splitlines():
        m = SHAPE_LINE_RE.match(line)
        if m:
            when, level, files, rest = m.groups()
            props = parse_props(rest)
            props["files"] = files
            shapes[when][int(level)] = props
    return shapes


def shape_key(shape):
    return {lvl: {k: props.get(k) for k in SHAPE_KEYS}
            for lvl, props in sorted(shape.items())}


def dir_snapshot(path):
    """Cheap fingerprint of a directory: names, sizes and mtimes."""
    entries = []
    for root, _, files in os.walk(path):
        for f in files:
            st = os.stat(os.path.join(root, f))
            entries.append((os.path.relpath(os.path.join(root, f), path),
                            st.st_size, st.st_mtime_ns))
    return sorted(entries)


def dir_size(path):
    return sum(size for _, size, _ in dir_snapshot(path))


def ratio(num, den):
    return num / den if den else ""


class Harness:
    def __init__(self, args):
        self.args = args
        with open(args.config) as f:
            self.cfg = json.load(f)
        self.load_flags = dict(self.cfg["load"])
        if args.num:
            self.load_flags["num"] = args.num
        self.num = int(self.load_flags["num"])
        self.repeats = args.repeats or int(self.cfg.get("repeats", 3))
        self.db_root = os.path.abspath(args.db_root)
        self.base = os.path.join(self.db_root, "base")
        self.results_dir = os.path.abspath(args.results)
        self.logs = os.path.join(self.results_dir, "logs")
        self.db_bench = os.path.abspath(args.db_bench)
        self.host = socket.gethostname()
        self.cpus = os.cpu_count()
        self._version = None

    # ----------------------------------------------------------- helpers --

    def version(self):
        """git commit of the source tree db_bench was built from, if any."""
        if self._version is None:
            src = os.path.dirname(self.db_bench)
            try:
                rev = subprocess.run(
                    ["git", "-C", src, "rev-parse", "--short", "HEAD"],
                    capture_output=True, text=True, check=True).stdout.strip()
                dirty = subprocess.run(
                    ["git", "-C", src, "diff", "--quiet", "HEAD", "--",
                     "*.cc", "*.h"]).returncode != 0
                self._version = rev + ("-dirty" if dirty else "")
            except (OSError, subprocess.CalledProcessError):
                self._version = "unknown"
        return self._version

    def command(self, flags):
        cmd = []
        if self.args.nice:
            cmd += ["nice", "-n", str(self.args.nice)]
        return cmd + [self.db_bench] + to_args(flags)

    def run_db_bench(self, flags, log_path):
        cmd = self.command(flags)
        if self.args.dry_run:
            print(" ".join(cmd))
            return None
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        start = time.time()
        proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True)
        with open(log_path, "w") as f:
            f.write("# " + " ".join(cmd) + "\n")
            f.write(proc.stdout)
        if proc.returncode != 0:
            die(f"db_bench exited with {proc.returncode}; see {log_path}")
        if self.args.verbose:
            print(f"    ({time.time() - start:.1f} s) {log_path}")
        return proc.stdout

    def table_flags(self):
        """Load flags that also describe how to open/read the DB."""
        flags = dict(self.load_flags)
        flags.pop("seed", None)
        return flags

    def filter_db(self, name):
        return os.path.join(self.db_root, "filters", name)

    @staticmethod
    def append_csv(path, columns, row):
        new = not os.path.exists(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            if new:
                w.writeheader()
            w.writerow(row)

    def completed_runs(self):
        path = os.path.join(self.results_dir, "results.csv")
        if not os.path.exists(path):
            return set()
        with open(path, newline="") as f:
            return {row["run_id"] for row in csv.DictReader(f)}

    # ------------------------------------------------- open-file limit --

    def open_files_needed(self):
        """Estimated file descriptors db_bench needs, or None if the config
        caps open SST files itself (-open_files)."""
        if "open_files" in self.load_flags or \
                "open_files" in self.cfg.get("read_defaults", {}):
            return None
        # db_bench defaults to max_open_files=-1: every SST stays open
        bytes_total = self.num * (int(self.load_flags.get("key_size", 16)) +
                                  int(self.load_flags.get("value_size", 100)))
        sst_files = bytes_total / int(
            self.load_flags.get("target_file_size_base", 64 << 20))
        # Headroom for files being compacted or rebuilt, logs, etc.
        return int(sst_files * 1.25) + 512

    def ensure_open_file_limit(self):
        """Raise this process's soft open-file limit (inherited by db_bench)
        or stop early with an explanation."""
        need = self.open_files_needed()
        if need is None:
            return
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft != resource.RLIM_INFINITY and soft < need:
            if hard != resource.RLIM_INFINITY and hard < need:
                die(f"db_bench keeps every SST file open and this DB needs "
                    f"~{need} file descriptors, but the hard limit is {hard} "
                    f"(`ulimit -Hn`). Ask an admin to raise it, or add "
                    f"\"open_files\": 1000 to \"load\" in the config (RocksDB "
                    f"then reopens files as needed, which adds some read "
                    f"overhead).")
            new_soft = need if hard == resource.RLIM_INFINITY else hard
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
            print(f"Raised open-file limit from {soft} to {new_soft} "
                  f"(need ~{need})")

    # -------------------------------------------------------------- plan --

    def experiments(self):
        exps = self.cfg["experiments"]
        if self.args.only:
            wanted = self.args.only.split(",")
            unknown = [e for e in wanted
                       if not any(name.startswith(e) for name in exps)]
            if unknown:
                die(f"unknown experiment(s) {unknown}; have {list(exps)}")
            exps = {name: e for name, e in exps.items()
                    if any(name.startswith(w) for w in wanted)}
        return exps

    def run_specs(self):
        """Every measured run (without repeats), grouped by filter."""
        only_filters = (set(self.args.filters.split(","))
                        if self.args.filters else None)
        by_filter = {}
        for exp_name, exp in self.experiments().items():
            vary = dict(exp.get("vary", {}))
            for k, v in vary.items():
                if v == "auto" and k == "threads":
                    vary[k] = auto_threads()
                elif not isinstance(v, list):
                    vary[k] = [v]
            keys = list(vary)
            for fname in exp["filters"]:
                filter_flags(fname)  # validate early
                if only_filters is not None and fname not in only_filters:
                    continue
                for values in itertools.product(*(vary[k] for k in keys)):
                    params = dict(self.cfg.get("read_defaults", {}))
                    params.update(exp.get("fixed", {}))
                    params.update(zip(keys, values))
                    workload_spec(params.get("workload", "nonempty"),
                                  self.num)  # validate early
                    by_filter.setdefault(fname, []).append(
                        (exp_name, params))
        return by_filter

    def cmd_plan(self):
        specs = self.run_specs()
        done = self.completed_runs()
        total = todo = 0
        print(f"Base DB: {self.base} "
              f"({'exists' if os.path.isdir(self.base) else 'not loaded'}), "
              f"{self.num:,} keys")
        print(f"Repeats: {self.repeats}")
        need = self.open_files_needed()
        if need is not None:
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft == resource.RLIM_INFINITY or soft >= need:
                status = "OK"
            elif hard == resource.RLIM_INFINITY or hard >= need:
                status = "OK, soft limit will be raised automatically"
            else:
                status = "TOO LOW: see `ulimit -Hn`"
            print(f"Open files: need ~{need}; limit soft {soft}, hard {hard}"
                  f" ({status})")
        for fname, runs in specs.items():
            n = len(runs) * self.repeats
            left = sum(1 for rep in range(1, self.repeats + 1)
                       for exp, p in runs
                       if self.run_id(exp, fname, p, rep) not in done)
            total += n
            todo += left
            print(f"  {fname:14s} {n:4d} runs ({left} not yet done)")
        print(f"Total: {total} runs, {todo} remaining, across "
              f"{len(specs)} filter rebuilds")

    # -------------------------------------------------------------- load --

    def cmd_load(self):
        if os.path.exists(self.base):
            if not self.args.force:
                die(f"{self.base} exists; use --force to reload")
            if not self.args.dry_run:
                shutil.rmtree(self.base)
        flags = dict(self.load_flags)
        flags.update(filter_flags(self.cfg.get("load_filter", "bloom-10")))
        flags.update({
            "db": self.base,
            # flush: leave nothing in memtables, so later opens never create
            # a new L0 file (which would get whatever filter is configured)
            "benchmarks": "filluniquerandom,flush,waitforcompaction",
            "use_existing_db": False,
            "statistics": False,
        })
        if not self.args.dry_run:
            os.makedirs(self.db_root, exist_ok=True)
        print(f"Loading {self.num:,} keys into {self.base} ...")
        out = self.run_db_bench(flags, os.path.join(self.logs, "load.log"))
        if out is not None:
            print(f"Done: {dir_size(self.base) / 2**30:.2f} GiB")

    # ----------------------------------------------------------- rebuild --

    def ensure_filter_db(self, fname):
        """Copy the base and rebuild its filters as `fname`; verify shape."""
        path = self.filter_db(fname)
        marker = os.path.join(path, "HARNESS_REBUILT_OK")
        if os.path.exists(marker):
            return path
        if not os.path.isdir(self.base):
            die(f"no base DB at {self.base}; run `harness.py load` first")
        if not self.args.dry_run:
            if os.path.exists(path):
                shutil.rmtree(path)  # partial copy or rebuild from a crash
            need = dir_size(self.base) * 1.1
            free = shutil.disk_usage(self.db_root).free
            if free < need:
                die(f"not enough space for a copy of the base "
                    f"({need / 2**30:.1f} GiB needed, "
                    f"{free / 2**30:.1f} GiB free)")
            print(f"  [{fname}] copying base ...", flush=True)
            shutil.copytree(self.base, path)

        flags = self.table_flags()
        flags.update(filter_flags(fname))
        flags.update({
            "db": path,
            "benchmarks": "rebuildfilters",
            "use_existing_db": True,
            "disable_auto_compactions": True,
            "statistics": False,
        })
        print(f"  [{fname}] rebuilding filters ...", flush=True)
        log = os.path.join(self.logs, fname, "rebuild.log")
        out = self.run_db_bench(flags, log)
        if out is None:  # dry run
            return path

        shapes = parse_shapes(out)
        if not shapes["before"] or not shapes["after"]:
            die(f"could not parse rebuildfilters output; see {log}")
        # Every copy starts from the same base ...
        base_shape_path = os.path.join(self.db_root, "base_shape.json")
        before = shape_key(shapes["before"])
        before_json = json.loads(json.dumps(before))  # int keys -> str keys
        if os.path.exists(base_shape_path):
            with open(base_shape_path) as f:
                if json.load(f) != before_json:
                    die(f"base DB shape differs from earlier rebuilds; was "
                        f"the base modified? See {log} and {base_shape_path}")
        else:
            with open(base_shape_path, "w") as f:
                json.dump(before_json, f, indent=1)
        # ... and only the filters may change
        if shape_key(shapes["after"]) != before:
            die(f"LSM shape changed during rebuild of {fname}; see {log}")

        after = shapes["after"]
        filter_bytes = sum(int(p.get("filter block size", 0))
                           for p in after.values())
        entries = sum(int(p.get("# entries", 0)) for p in after.values())
        m = REWROTE_RE.search(out)
        ftype, bits = (("none", 0) if fname == "none"
                       else FILTER_NAME_RE.match(fname).groups())
        row = {
            "filter": fname, "filter_type": ftype,
            "filter_bits_config": bits,
            "filter_bytes_total": filter_bytes, "entries_total": entries,
            "filter_bits_per_key_actual":
                round(8.0 * filter_bytes / entries, 4) if entries else "",
            "files_rewritten": m.group(1) if m else "",
            "rebuild_seconds": m.group(2) if m else "",
            "per_level_filter_bytes": json.dumps(
                {f"L{lvl}": int(p.get("filter block size", 0))
                 for lvl, p in sorted(after.items())}),
            "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
            "host": self.host, "db_bench_version": self.version(),
            "log": os.path.relpath(log, self.results_dir),
        }
        self.append_csv(os.path.join(self.results_dir, "filters.csv"),
                        FILTER_COLUMNS, row)
        print(f"  [{fname}] {row['filter_bits_per_key_actual']} bits/key, "
              f"{filter_bytes / 2**20:.1f} MiB of filters, rebuilt in "
              f"{row['rebuild_seconds']} s; shape verified", flush=True)
        with open(marker, "w") as f:
            f.write(json.dumps(row) + "\n")
        return path

    # --------------------------------------------------------------- run --

    @staticmethod
    def run_id(exp, fname, params, rep):
        return f"{exp}|{fname}|{json.dumps(params, sort_keys=True)}|r{rep}"

    def filter_stats(self, fname):
        marker = os.path.join(self.filter_db(fname), "HARNESS_REBUILT_OK")
        if os.path.exists(marker):
            with open(marker) as f:
                return json.loads(f.readline())
        return {}

    def measure(self, exp, fname, params, rep, path):
        params = dict(params)
        workload = params.pop("workload", "nonempty")
        bench, wflags, expected_found = workload_spec(workload, self.num)

        flags = self.table_flags()
        flags.update(filter_flags(fname))
        flags.update(params)
        flags.update(wflags)
        flags.update({
            "db": path,
            "readonly": True,
            "seed": self.load_flags.get("seed", 42),
            # warm up, clear statistics, then measure
            "benchmarks": f"{bench},resetstats,{bench}",
            "statistics": True,
            "histogram": True,
        })
        # Stable, readable log name: experiment, workload, params digest
        digest = hashlib.md5(json.dumps(params, sort_keys=True).encode())
        safe_exp = re.sub(r"[^\w.-]", "_", exp)
        log = os.path.join(
            self.logs, fname,
            f"{safe_exp}_{workload}_{digest.hexdigest()[:8]}_r{rep}.log")
        snap = None if self.args.dry_run else dir_snapshot(path)
        out = self.run_db_bench(flags, log)
        if out is None:
            return None
        if dir_snapshot(path) != snap:
            die(f"DB directory {path} changed during a read-only run; "
                f"see {log}")

        ops = [m for m in map(OPS_RE.match, out.splitlines()) if m]
        if len(ops) < 2:
            die(f"could not find the measured {bench} result; see {log}")
        micros, ops_sec = float(ops[-1].group(2)), int(ops[-1].group(3))
        found = FOUND_RE.findall(out)
        found_frac = int(found[-1][0]) / int(found[-1][1]) if found else None
        if found_frac is None:
            die(f"could not parse found count; see {log}")
        tolerance = 0.0 if expected_found in (0.0, 1.0) else 0.03
        if abs(found_frac - expected_found) > tolerance + 1e-12:
            die(f"found {found_frac:.4f} of keys, expected "
                f"{expected_found:.4f}: workload/key range mismatch; see {log}")
        pct = PCT_RE.findall(out)
        pct = [float(x) for x in pct[-1]] if pct else [""] * 5
        t = {k: int(v) for k, v in
             (m.groups() for m in map(TICKER_RE.match, out.splitlines()) if m)}

        threads = int(params.get("threads", 1))
        reads = int(params.get("reads", 0))
        lookups = threads * reads
        useful = t.get("rocksdb.bloom.filter.useful", 0)
        positive = t.get("rocksdb.bloom.filter.full.positive", 0)
        true_pos = t.get("rocksdb.bloom.filter.full.true.positive", 0)
        checks = useful + positive

        def bc(kind):
            hit = t.get(f"rocksdb.block.cache.{kind}.hit", 0)
            miss = t.get(f"rocksdb.block.cache.{kind}.miss", 0)
            return hit, miss, ratio(miss, hit + miss)

        dh, dm, dr = bc("data")
        ih, im, ir = bc("index")
        fh, fm, fr = bc("filter")
        fstats = self.filter_stats(fname)
        ftype, bits = (("none", 0) if fname == "none"
                       else FILTER_NAME_RE.match(fname).groups())
        return {
            "run_id": self.run_id(exp, fname, dict(params, workload=workload),
                                  rep),
            "experiment": exp, "filter": fname, "filter_type": ftype,
            "filter_bits_config": bits, "repeat": rep, "workload": workload,
            "benchmark": bench, "threads": threads,
            "cache_size": params.get("cache_size", ""),
            "read_random_exp_range": params.get("read_random_exp_range", ""),
            "reads_per_thread": reads, "lookups": lookups,
            "ops_per_sec": ops_sec, "micros_per_op": micros,
            "p50_us": pct[0], "p75_us": pct[1], "p99_us": pct[2],
            "p999_us": pct[3], "p9999_us": pct[4],
            "found_fraction": round(found_frac, 6),
            "expected_found_fraction": expected_found,
            "filter_checks": checks, "filter_useful": useful,
            "filter_positive": positive, "filter_true_positive": true_pos,
            # Checks on the file actually holding the key cannot be false
            # positives, so they are excluded from the denominator
            "fpr": ratio(positive - true_pos, checks - true_pos),
            "filter_checks_per_lookup": ratio(checks, lookups),
            "false_positives_per_lookup": ratio(positive - true_pos, lookups),
            "bc_data_hit": dh, "bc_data_miss": dm, "bc_data_miss_ratio": dr,
            "bc_index_hit": ih, "bc_index_miss": im, "bc_index_miss_ratio": ir,
            "bc_filter_hit": fh, "bc_filter_miss": fm,
            "bc_filter_miss_ratio": fr,
            "data_misses_per_lookup": ratio(dm, lookups),
            "filter_bytes_total": fstats.get("filter_bytes_total", ""),
            "filter_bits_per_key_actual":
                fstats.get("filter_bits_per_key_actual", ""),
            "params": json.dumps(dict(params, workload=workload),
                                 sort_keys=True),
            "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
            "host": self.host, "cpus": self.cpus,
            "db_bench_version": self.version(),
            "log": os.path.relpath(log, self.results_dir),
            "command": " ".join(self.command(flags)),
        }

    def cmd_run(self):
        specs = self.run_specs()
        done = self.completed_runs()
        results = os.path.join(self.results_dir, "results.csv")
        for fname, runs in specs.items():
            pending = [(rep, exp, p) for rep in range(1, self.repeats + 1)
                       for exp, p in runs
                       if self.run_id(exp, fname, p, rep) not in done]
            if not pending:
                print(f"[{fname}] all {len(runs) * self.repeats} runs done")
                continue
            print(f"[{fname}] {len(pending)} runs to do", flush=True)
            path = self.ensure_filter_db(fname)
            for i, (rep, exp, params) in enumerate(pending, 1):
                row = self.measure(exp, fname, params, rep, path)
                if row is None:
                    continue
                self.append_csv(results, RESULT_COLUMNS, row)
                fpr = ("n/a" if row["fpr"] == ""
                       else f"{100 * row['fpr']:.3f}%")
                print(f"  [{fname}] {i}/{len(pending)} {exp} "
                      f"{row['workload']} r{rep}: {row['ops_per_sec']} ops/s, "
                      f"FPR {fpr}", flush=True)
            if not self.args.keep_filter_dbs and not self.args.dry_run:
                shutil.rmtree(path)


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["plan", "load", "run"])
    p.add_argument("--config", default=os.path.join(HERE, "experiments.json"))
    p.add_argument("--db-root", default=os.path.join(HERE, "dbs"),
                   help="scratch directory for the base DB and filter copies")
    p.add_argument("--results", default=os.path.join(HERE, "results"))
    p.add_argument("--db-bench",
                   default=os.path.join(os.path.dirname(HERE), "db_bench"),
                   help="path to a release (DEBUG_LEVEL=0) db_bench")
    p.add_argument("--num", type=int, help="override key count from config")
    p.add_argument("--repeats", type=int, help="override repeats from config")
    p.add_argument("--only", help="comma-separated experiment name prefixes")
    p.add_argument("--filters", help="comma-separated filter names to run")
    p.add_argument("--keep-filter-dbs", action="store_true",
                   help="keep each rebuilt DB copy instead of deleting it")
    p.add_argument("--force", action="store_true", help="load: replace base")
    p.add_argument("--dry-run", action="store_true",
                   help="print db_bench commands without running them")
    p.add_argument("--nice", type=int, default=0,
                   help="run db_bench under `nice -n N`")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    h = Harness(args)
    if args.command != "plan" and not args.dry_run:
        h.ensure_open_file_limit()
    if args.command != "plan" and not args.dry_run and \
            not os.access(h.db_bench, os.X_OK):
        die(f"no executable db_bench at {h.db_bench}; build it with "
            f"`DEBUG_LEVEL=0 make db_bench` or pass --db-bench")
    {"plan": h.cmd_plan, "load": h.cmd_load, "run": h.cmd_run}[args.command]()


if __name__ == "__main__":
    main()
