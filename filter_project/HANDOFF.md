# Project handoff: SST filter comparison in RocksDB (course project P4)

Status as of 2026-11-25. Read this first, then `filter_project/README.md`.

## Goal

Course project for a database architecture class (option P4, "which structure?"):
compare SST filters in RocksDB using `db_bench`: false-positive rate (FPR), memory,
lookup cost and build cost. The setup mirrors the course's P1 (LRU vs HyperClockCache)
template. Filters in scope:

| Filter | Status |
|---|---|
| Bloom | built into RocksDB |
| Ribbon | built into RocksDB |
| Cuckoo | **implemented in this fork** (per-SST `FilterPolicy`) |
| Chucky (SIGMOD '21) | out of scope to implement: it is one global filter across the LSM, which RocksDB's per-SST `FilterPolicy` interface cannot express. Discussion only. |
| ElasticBF (ATC '19) | stretch goal, not started. E4 (skew) is its baseline. |

**Instructor constraint:** all experiments must use the same database. Filters live
inside SST files, so this is met by loading one base DB and rebuilding only the filter
blocks per configuration (see `rebuildfilters` below).

## Repository

- Fork of `facebook/rocksdb`, remote `origin = git@github.com:lucaudill/rocksdb.git`.
- Branch `filter-benchmark`, based on tag `v11.8.1` (commit `abeebd963`; upstream tags
  v11.8.0 and v11.8.1 point at the same commit).
- Commits on the branch (oldest first): `409d50462` new ribbon option, `739f448fa` cuckoo
  filter, `4ea5c136c` db_bench cuckoo support, `746749d15` rebuildfilters, `ac22fe383` and
  `c2b11946a` harness, `8951aacf3` results.
- The repo root `CLAUDE.md` is RocksDB's upstream contributor guide. Its conventions apply
  to C++ changes here (see "Conventions" below).
- The user commits and pushes; do not commit unless asked.

## What has been changed or added

### C++ (RocksDB fork)

1. **Cuckoo filter**: `table/block_based/cuckoo_filter_policy.{h,cc}`.
   - Public API: `NewCuckooFilterPolicy(double bits_per_key)` in
     `include/rocksdb/filter_policy.h`; string form `cuckoofilter:<bits>` (nickname
     `rocksdb.CuckooFilter`), registered in `table/block_based/filter_policy.cc`.
   - Partial-key cuckoo hashing, 4 slots per bucket. Fingerprint bits
     `f = floor(0.95 * bits_per_key)`, clamped to [4, 32]; table sized for load `f/bits`
     (max 0.95), so real memory tracks the bits/key target.
   - Alternate bucket is `(H(fp) - b) mod m`, an involution for ANY bucket count, so the
     table size is not restricted to powers of two.
   - Static build at `Finish()`: random-walk eviction (500 kicks); on failure, grow buckets
     by ~1/16 and retry (up to 8 attempts), then fall back to an always-true filter.
     A key whose fingerprint is already in one of its buckets is skipped, which also
     absorbs non-adjacent duplicate keys.
   - On-disk layout: bit-packed fingerprint array plus an 8-byte trailer (num_buckets u32,
     fingerprint bits, slots per bucket, version 1, magic 0xC7). An empty filter means
     always false; an unrecognized or corrupt filter means always true (safe).
   - Its own `CompatibilityName()`, so cuckoo and Bloom/Ribbon filters cannot read each
     other: a DB switched between them filters nothing on old SSTs until they are rewritten.
   - Supports partitioned filters, `AddKeyAndAlt` (prefix filtering), batched `MayMatch`
     with prefetch, and `detect_filter_construct_corruption` (`MaybePostVerify`).
   - Construction memory (8 B/key hashes + 4 B/slot) is not charged to the block cache;
     `optimize_filters_for_memory` is ignored.
   - The implementation is straightforward, not tuned: query CPU is ~23-27 ns vs Bloom's
     6-8 ns in `filter_bench` (in-cache).
   - Build files updated: `src.mk`, `CMakeLists.txt`, `BUCK` (regenerated with
     `buckifier/buckify_rocksdb.py`).
2. **Tests**: `CuckooFilterTest.*` (9 tests) in `util/bloom_test.cc`;
   `DBBloomFilterTest.CuckooFilterEndToEnd` in `db/db_bloom_filter_test.cc` (whole and
   partitioned filters, Get and MultiGet, reopen under Bloom); cuckoo names added to the
   expected list in `options/customizable_test.cc`. All of `bloom_test`,
   `db_bloom_filter_test` and `customizable_test` passed.
3. **db_bench** (`tools/db_bench_tool.cc`):
   - `-bloom_bits` is now a double (e.g. 7.5) and applies to every filter type.
   - `-filter_type=bloom|ribbon|cuckoo` (empty = legacy behavior). Errors on unknown types,
     on conflict with `-use_ribbon_filter`, and on a filter type without `-bloom_bits > 0`.
   - `-bloom_before_level` (passed to `NewRibbonFilterPolicy`; -1 = Ribbon at every level,
     0 = RocksDB default hybrid with Bloom for flushes).
   - `rebuildfilters` benchmark: rewrites every SST with a single-file `CompactFiles` into
     its own level (no size limit, `allow_trivial_move=false`), so only filter blocks
     change. Requires `-use_existing_db=1 -disable_auto_compactions=1`. Prints per-level
     aggregated table properties before and after (`rebuildfilters before|after L<n>: ...`).
   - `resetstats` now also resets the `-statistics` tickers and histograms (previously it
     did not, so warm-up counts leaked into measurements).
   - `-read_missing_fraction=F` for `readrandom`: fraction F of lookups use a missing key
     made by appending one byte to an existing key (as `readmissing` does), so missing keys
     sort BETWEEN existing keys. (Widening `-num` instead is wrong: those keys sort above
     every key and are excluded by SST key ranges before any filter is checked.)
4. **filter_bench** (`util/filter_bench.cc`): `-impl=3` is the cuckoo filter; its FP
   sanity check uses the cuckoo filter's own `EstimatedFpRate` (the Bloom-tuned bound
   would wrongly reject cuckoo at low bits/key).

### Benchmark harness: `filter_project/`

- `harness.py` (Python 3.8+, stdlib only), commands:
  - `plan`: runs per filter, done vs remaining, open-file limit check. Runs nothing.
  - `load`: builds `<db-root>/base` with `filluniquerandom,flush,waitforcompaction`.
    Refuses to overwrite without `--force`.
  - `run`: for each filter: copy base to `<db-root>/filters/<name>`, run `rebuildfilters`,
    verify per-level shape (files, entries, data blocks, data bytes, raw key/value bytes)
    against `<db-root>/base_shape.json`, then run each measurement read-only
    (`-readonly`, warm-up pass, `resetstats`, measured pass), verify the DB directory is
    unchanged and the found fraction matches the workload, append to `results.csv`.
    Deletes each copy afterwards unless `--keep-filter-dbs`. Resumable: completed
    `run_id`s are skipped. Fails fast on any anomaly.
  - Options: `--db-root`, `--results`, `--db-bench`, `--num`, `--repeats`, `--only`,
    `--filters`, `--nice`, `--dry-run`, `--keep-filter-dbs`, `--force`, `-v`.
  - Raises `RLIMIT_NOFILE` soft limit automatically (db_bench keeps every SST open;
    ~5,200 SSTs at 10M keys) or stops early if the hard limit is too low.
- `experiments.json`: P1 LSM settings in `load`; `read_defaults`; experiments E1-E5 with
  `filters`, `fixed`, `vary` (cross product). Filter names: `none` or
  `<bloom|ribbon|cuckoo>-<bits>`; ribbon gets `bloom_before_level=-1`. Workloads:
  `nonempty` (readrandom), `empty` (readmissing), `mixed-F` (readrandom with
  `read_missing_fraction=F`). Any other key becomes `-key=value`.
  Flag layering for a run: load (minus seed) < filter < read_defaults < fixed < vary <
  workload < harness run flags.
- `analysis.ipynb`: loads the CSVs, sanity checks, one section per experiment, findings,
  helpers (`runs()`, `summarize()`, `show_log()`). Writes `figures/*.png|pdf`. Edit the
  notebook directly (there is no generator script in the repo). It starts with
  `%matplotlib inline` (needed for matplotlib 3.8).
- `environment.yml`: conda env `rocksdb-filters` (conda-forge only: python 3.12,
  numpy>=1.26, pandas>=2.2, matplotlib>=3.8, jinja2, jupyterlab, ipykernel). Tested at
  both the minimum and current versions.
- `results/results.csv` (444 rows, 48 columns) and `results/filters.csv` (20 rows) are
  committed. `results/logs/` (17 MB) and `dbs/` are gitignored.

## Environments

- **Server** (where results came from): host `third-workstation`, 24 x Intel Xeon Silver
  4214R @ 2.40 GHz (check `lscpu` for physical cores vs hyperthreads), shared machine.
  Repo at `/home/lucas/rocksdb`. DB root `/var/tmp/lucas/filterdbs` (base DB 4.89 GiB,
  `base_shape.json`). `/var/tmp` is often purged after ~30 days: the base was loaded
  2026-10-22, so **verify it still exists** before assuming it can be reused. A reloaded
  base has a different LSM shape; new results would then need a fresh results directory
  and a new `base_shape.json`.
  Before long runs: `ulimit -n $(ulimit -Hn)`, run inside `tmux`, consider `--nice 10`.
- **User's Mac** (Apple Silicon): repo at `/Users/lucascaudill/rocksdb`, used for analysis.
  A Linux VM (aarch64, 4 cores, Python 3.10, no gflags) is available for compiling.
- Build: `AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j<cores> db_bench filter_bench` (release for
  benchmarks). Tests: `AUTO_CLEAN=1 make -j<cores> bloom_test db_bloom_filter_test
  customizable_test` (debug). v11.8.1 fails to compile without Snappy because of a
  `-Werror` issue in `util/compression.cc`: install `libsnappy-dev` (plus lz4, zstd) or
  build with `DISABLE_WARNING_AS_ERROR=1`.

## Results so far (10M keys, 3 repeats, all experiments complete)

Dataset: 10M `filluniquerandom` keys (64 B key, 440 B value), 4.89 GiB on disk, 5,237
SSTs in L1-L8 (L0 empty), index 23 MiB, Bloom-10 filters 12 MiB. Fixed level targets
(`level_compaction_dynamic_level_bytes=false`) leave L7 the largest level (2 GiB) and L8
partial. An absent-key lookup checks ~4.5 filters.

- **E1 bits/key:**

  | Real bits/key (config) | Bloom | Ribbon | Cuckoo |
  |---|---|---|---|
  | 6 | – | – | ~20% FPR (5-bit fingerprints) |
  | 10 | 0.90% | 0.93% at 7.2 real bits | 1.40% |
  | 16 | 0.08% | – | 0.03% |

  Cuckoo matches its model; Bloom drifts above the textbook model at high bits (RocksDB's
  cache-local Bloom). Cuckoo/Bloom crossover at ~12 bits/key, as predicted by
  `1.44 x = (x + 3) / 0.95`.
- **Cost model:** absent-key time per lookup = 5.7 us + 86 us x (data block reads per
  lookup), R^2 = 0.991 across all filters; predicts the no-filter case within 11%.
  I/O-bound regime: FPR per bit decides performance, filter CPU cost is invisible.
- **E1 throughput:** absent keys: no filter ~3.1K lookups/s, filters 30-65x higher.
  Existing keys: ~4.2K/s without a filter, ~2.5x with any filter, filters within a few %.
  p99 drops ~10x once 4.5 x FPR < 1%.
- **E2 cache size:** absent keys: Ribbon-14.3 (Bloom-10's memory) is 1.6-2.0x Bloom-10 at
  every cache size. Existing keys: metadata (index 23 MiB + filters) misses at 32-64 MB;
  Ribbon-10 halves filter misses at 64 MB.
- **E3 absent fraction:** no filter flat at 3-5K/s; filters climb steeply near 100%;
  16 bits/key ~2x 10 bits/key only near 100% absent.
- **E4 skew:** throughput rises via data block caching (data miss ratio 99% -> 44%); FPR
  flat; filters within ~6%.
- **E5 threads:** apparent superlinear scaling is an artifact: warm-up is `reads` per
  thread, so more threads warm the cache more (data reads per lookup fall 0.045 -> 0.014
  for absent keys). Only "no filter-dependent scaling bottleneck" is defensible.
- Noise: median CV across repeats ~1.5%; 15 configurations have CV > 10%.

## Known issues and caveats

- **E5 warm-up confound** (above).
- `run_id` does not include the key count or db-root: trial runs with `--num` must use a
  separate `--results` directory, or they mark full-size runs as done.
- `rebuild_seconds` in `filters.csv` is dominated by rewriting data; use `filter_bench`
  for filter construction cost (Bloom ~35, Cuckoo ~52, Ribbon ~130 ns/key).
- E5's description originally assumed the 2 GB cache held everything; it does not (data
  is 4.9 GB), so false positives still hit disk.
- At small scale (200K keys) the P1 config range-partitions the tree (each key range in one
  level, ~1 filter check per lookup), likely from trivial moves. Mostly gone at 2M+ keys,
  but upper levels are still rarely checked. Report `filter_checks_per_lookup` with FPR.
- RocksDB's actual default is no filter (`BlockBasedTableOptions::filter_policy =
  nullptr`); Bloom-10 is the conventional recommendation.

## Open tasks (suggested priority)

1. **CPU-bound regime** (config only): rerun `bloom-10`, `ribbon-10`, `ribbon-14.3` (add
   `cuckoo-10`) with data mostly in memory: `cache_size` 8 GiB (> 4.9 GiB DB) or
   `use_direct_reads=false`, for several thread counts, both workloads. Hypothesis: Bloom
   matches or beats Ribbon once false positives are cheap. Add as a new experiment in
   `experiments.json` and a notebook section.
2. **Fix the E5 warm-up confound**: keep total warm-up lookups constant across thread
   counts (e.g. `reads = total / threads` for both passes, or a separate warm-up count),
   then rerun only E5 into a fresh results file or with new run_ids. Acceptance: data
   block reads per lookup roughly constant across thread counts.
3. **Hybrid analysis** (`bloom_before_level`): plot filter bytes per level from
   `filters.csv` (`per_level_filter_bytes`): L7-L8 hold ~60% of filter bytes, L0-L3 < 5%.
   Optionally run `ribbon-10` with `bloom_before_level` 0, 3, 6 (needs harness support for
   per-filter extra flags; filter names currently encode only type and bits).
4. **Write/build cost experiment (E6)**: load a fresh DB per filter (same seed), measure
   load throughput, `rocksdb.compaction.times.cpu_micros`, flush time and
   `rocksdb.stall.micros`; include Ribbon with `bloom_before_level` 0 and -1. Separate from
   the read experiments (writes modify the DB). ~10 loads of ~5 GB.
5. **RocksDB-default tree shape**: second base DB (size ratio 10, dynamic level sizing,
   64 MB files) in its own `--db-root` and `--results`; rerun E1 absent-key runs.
6. **Notebook**: add a theoretical-minimum line `FPR = 2^-b` to the E1 FPR figure; optional
   Ribbon model.
7. Optional: include key count in `run_id`; optimize cuckoo lookups (SIMD compare of 4
   slots, single-load buckets); simplified ElasticBF (per-file multiple Bloom units enabled
   by access counters).

## Conventions

- C++: follow the repo-root `CLAUDE.md`: `make format-auto` (or clang-format), ASCII only
  in source, license header on new files, keep `src.mk` / `CMakeLists.txt` / `BUCK` in
  sync, release builds use `-fno-rtti` (no `dynamic_cast` outside tests), `AUTO_CLEAN=1`
  with every make. Add tests for new behavior.
- Python: stdlib only in `harness.py`, Python 3.8 compatible (no backslashes inside
  f-string expressions). Validate with `harness.py plan` and `run --dry-run`.
- Verify before claiming: build and run tests for C++; for experiments, smoke-test at
  small scale with `--num 1000000 --repeats 1` in a separate `--db-root` and `--results`.
- Figures: one hue per filter type (Bloom `#2a78d6`, Ribbon `#eb6834`, Cuckoo `#1baf7a`,
  none gray `#898781`), bit variants by line style, markers per type, a legend for >= 2
  series, error bars = +-1 std over repeats, never dual y-axes.
