# Filter comparison harness (Bloom vs Ribbon vs Cuckoo)

`harness.py` runs the experiments in `experiments.json` with `db_bench` and
writes one CSV row per measured run. LSM and workload settings follow the
course P1 (LRU vs HCC) setup: 10M `filluniquerandom` keys (64 B keys,
440 B values), 4 MB write buffer, 1 MB SSTs, size ratio 2, direct I/O,
no compression, `cache_index_and_filter_blocks=true`, seed 42.

## One database for every experiment

Filters live inside SST files, so they cannot be switched at read time. The
harness loads one base DB, then for each filter configuration copies it and
runs `db_bench -benchmarks=rebuildfilters`, which rewrites every SST file into
the level it already occupies. Before a copy is used, the harness checks that
every level has the same files, entries and data bytes as the base. Only the
filter blocks differ, so every configuration's lookups touch the same files.
Reads open each copy with `-readonly`, and the harness checks that the DB
directory is unchanged after every run.

Each run warms up with one pass of lookups, clears statistics (`resetstats`),
then measures a second pass with different random keys.

## Usage

Build a release `db_bench` from this fork first:

```bash
DEBUG_LEVEL=0 make -j"$(nproc)" db_bench
```

Then, from the repository root:

```bash
python3 filter_project/harness.py plan              # runs per filter, what is left
python3 filter_project/harness.py load --db-root /scratch/$USER/filterdbs
python3 filter_project/harness.py run  --db-root /scratch/$USER/filterdbs
```

Useful options:

| Option | Effect |
|---|---|
| `--only E1,E3` | run only experiments whose names start with these |
| `--filters bloom-10,cuckoo-10` | run only these filter configurations |
| `--num 1000000` | smaller DB for a quick trial (use a separate `--db-root`) |
| `--repeats 1` | override the number of repeats |
| `--nice 10` | lower CPU priority, for shared machines |
| `--keep-filter-dbs` | keep each rebuilt copy (default: delete once its runs finish) |
| `--dry-run` | print the db_bench commands without running anything |

`run` is resumable: runs already in `results.csv` are skipped, so it can be
stopped and restarted. Run long jobs inside `tmux` or with `nohup`.

Disk: the base DB is about 5 GB at 10M keys. By default only one rebuilt copy
exists at a time, so allow about 3x the base size. `--keep-filter-dbs` keeps
a copy per filter configuration (about 20 in the default config).

## Configuration

Filter names are `none` or `<bloom|ribbon|cuckoo>-<bits_per_key>`, e.g.
`cuckoo-7.5`. For Ribbon, bits per key is *Bloom-equivalent* (same FP rate as
a Bloom filter of that size), so `ribbon-N` uses about 0.7 N real bits per
key; `ribbon-14.3` is about 10. Real bits per key for every configuration is
recorded in `filters.csv`.

Workloads:

| Workload | db_bench | Keys |
|---|---|---|
| `nonempty` | `readrandom` | all exist |
| `empty` | `readmissing` | none exist |
| `mixed-F` | `readrandom -read_missing_fraction=F` | fraction F missing |

Missing keys are existing keys with one byte appended, so they sort between
existing keys and SST key ranges cannot exclude them; only filters (or reading
data blocks) can.

Any other key under an experiment's `fixed` or `vary` is passed to db_bench
as `-key=value`. `"threads": "auto"` means powers of two up to the core count.

## Output

In the results directory (default `filter_project/results/`):

- `results.csv`: one row per measured run. Throughput, latency percentiles
  (`p50_us` ... `p9999_us`), filter checks per lookup, measured FPR
  (false positives / negative filter checks), block cache hit and miss counts
  and ratios for data, index and filter blocks, data block misses per lookup,
  real filter bits per key, the exact db_bench command, and its log path.
- `filters.csv`: one row per rebuild. Filter bytes in total and per level,
  real bits per key, and rebuild time. Rebuild time is dominated by rewriting
  data; use `filter_bench` (`-impl=1` Bloom, `2` Ribbon, `3` Cuckoo) to
  measure filter construction and query cost in isolation.
- `logs/`: full db_bench output for the load, every rebuild, and every run.

## Analysis

`analysis.ipynb` reads `results/results.csv` and `results/filters.csv`, makes one
section of plots and tables per experiment, and writes the figures to `figures/`
(PNG and PDF). It ends with a findings summary and helpers for exploring the data
interactively. To set up its environment (conda or mamba):

```bash
conda env create -f filter_project/environment.yml
conda activate rocksdb-filters
cd filter_project && jupyter lab analysis.ipynb
```
