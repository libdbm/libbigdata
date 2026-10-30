# Parquet benchmark: libbigdata vs DuckDB vs parquet-mr

Compares the pure-Dart **libbigdata** reader/writer against **DuckDB** (native
C++) and **parquet-mr** (the canonical Java implementation) for Parquet **read**
and **write**, all inside one Docker image so the comparison is apples-to-apples.

## Run it

```bash
# from the repository root
docker build -f benchmark/Dockerfile -t libbigdata-bench .
docker run --rm -v "$PWD/benchmark/results:/app/benchmark/results" libbigdata-bench
```

The image build compiles the Dart benchmark to a native executable, builds the
parquet-mr fat jar, and installs the DuckDB CLI. The run prints a markdown table
to stdout and writes it to `benchmark/results/report.md`.

Knobs (environment variables on `docker run -e ...`):

| var | default | meaning |
|---|---|---|
| `REPS` | `5` | repetitions per cell (first is discarded as warm-up; median reported) |
| `ROWS` | `1000000` | rows in the generated dataset |
| `OPS` | `read,write` | which operations to run |
| `DATASETS` | `gen,flights` | read datasets |
| `WRITE_CODECS` | `snappy,zstd` | codecs for the write benchmark |

Example: `docker run --rm -e OPS=read -e REPS=3 libbigdata-bench`

## What it measures

- **Read**: fully decode every column and compute one deterministic aggregate
  (sum of numeric columns + string lengths + counts). Using per-column
  aggregates (not `COUNT(*)`) forces DuckDB to actually decode the data. Each
  engine prints its aggregate and the harness asserts they all match
  (`RESULT_CHECK PASS`), so the benchmark also guards correctness.
- **Write**: convert the generated `gen.csv` to Parquet with each codec; report
  time and output size. (Timing includes CSV parsing, so it measures a realistic
  "CSV → Parquet" conversion.)

Datasets: a deterministic generated set (`gen`, written to a neutral Parquet by
DuckDB so the read input favours no engine) and the real `flights-1m.parquet`
(1M rows).

## Fairness / methodology

- Single linux image and architecture for all three engines.
- Warm runs: the first repetition is discarded, the **median** of the rest is reported.
- **In-process** timing of the operation only:
  - Dart is **AOT-compiled** (matches native DuckDB / the JVM fairly).
  - parquet-mr times the read/write loop in-process (excludes JVM startup).
  - DuckDB is timed via `.timer` (query execution only, excludes process startup).
- DuckDB is pinned to **`threads=1`** for parity with single-threaded Dart/Java.
  Its multi-threaded throughput (its real-world strength) will be higher.
- Caveat: libbigdata's reader loads the whole table into memory (`read()`), so it
  is not suited to files larger than RAM; the write timing includes CSV parsing.

## Layout

```
benchmark/
  Dockerfile, Dockerfile.dockerignore
  run.sh          # orchestrator (entrypoint)
  report.py       # aggregates RESULT lines -> markdown + correctness check
  gen_data.dart   # deterministic CSV generator
  dart/bench.dart # libbigdata read/write
  java/           # parquet-mr read/write (Maven project)
  results/        # report.md written here
```

Each engine emits machine-parseable lines that `report.py` consumes:

```
RESULT <engine> <op> <dataset> <codec> <ms> <rows> <bytes> <ints> <floats>
```

## In-process benchmarks

The other benchmarks in `benchmark/` measure libbigdata alone and run with the
Dart SDK, from the repository root:

| benchmark | command | measures |
|---|---|---|
| `sql_bench.dart` | `dart run benchmark/sql_bench.dart [rows=1000000] [reps=3]` | SQL engine operators over synthesized tables |
| `dataframe_bench.dart` | `dart run benchmark/dataframe_bench.dart [rows=1000000] [reps=3]` | DataFrame pipelines in memory and over `benchmark/work/gen.snappy.parquet` when present |
| `exec_bench.dart` | `dart run benchmark/exec_bench.dart [rows] [reps]` | vectorized against per-row filter and projection |
| `format_bench.dart` | `dart run benchmark/format_bench.dart [rows] [reps] [csvPath]` | write time, read time and size across CSV, JSON, JSON Lines, Arrow IPC and Parquet |
| `column_bench.dart` | `dart run benchmark/column_bench.dart <path> [reps]` | per-column Parquet read times |

`synth.dart` (data synthesis) and `results.dart` (unwrapping a `Result` or
stopping the run) are shared helpers, not benchmarks.
