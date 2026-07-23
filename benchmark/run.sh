#!/usr/bin/env bash
# Orchestrates the libbigdata vs DuckDB vs parquet-mr Parquet benchmark.
# Run from the repository root (the Docker image sets WORKDIR there).
set -euo pipefail

REPS=${REPS:-5}
ROWS=${ROWS:-1000000}
OPS=${OPS:-read,write}
DATASETS=${DATASETS:-gen,flights}
WRITE_CODECS=${WRITE_CODECS:-snappy,zstd}

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
WORK=benchmark/work
mkdir -p "$WORK" benchmark/results
RESULTS="$WORK/results.txt"
: > "$RESULTS"

# Engine entry points (overridable). DART_BIN is the AOT binary in the image.
DART_BIN=${DART_BIN:-}
JAVA=${JAVA:-java}
JAR="${JAR:-benchmark/java/target/parquet-bench.jar}"
# Make JAR absolute relative to the repo root (portable; macOS realpath lacks -m).
case "$JAR" in /*) ;; *) JAR="$ROOT/$JAR" ;; esac

dart_bench() { # args passed to the dart benchmark
  if [ -n "$DART_BIN" ]; then "$DART_BIN" "$@"; else dart run benchmark/dart/bench.dart "$@"; fi
}

log() { echo "[run] $*" >&2; }

# ---- data prep ----------------------------------------------------------
log "generating gen.csv ($ROWS rows)"
dart run benchmark/gen_data.dart "$ROWS" "$WORK/gen.csv"
log "writing neutral gen.snappy.parquet via DuckDB"
# Force column types so the parquet types are deterministic and line up with the
# Java getters and Dart column types across engines.
gen_cols="{'id':'BIGINT','amount':'DOUBLE','category':'VARCHAR','name':'VARCHAR','quantity':'BIGINT','active':'BOOLEAN','ts':'BIGINT','score':'DOUBLE'}"
duckdb -c "COPY (SELECT * FROM read_csv('$WORK/gen.csv', header=true, columns=$gen_cols)) TO '$WORK/gen.snappy.parquet' (FORMAT parquet, COMPRESSION snappy);"

declare -A RPATH=( [gen]="$WORK/gen.snappy.parquet" [flights]="test/fixtures/flights-1m.parquet" )

# gen vs flights aggregate expressions (must match the dart/java benches).
gen_ints="SUM(id)+SUM(quantity)+SUM(ts)+SUM(length(category))+SUM(length(name))+COUNT(*) FILTER(WHERE active)+COUNT(score)"
gen_floats="SUM(amount)+SUM(score)"
fl_ints="SUM(DEP_DELAY)+SUM(ARR_DELAY)+SUM(AIR_TIME)+SUM(DISTANCE)+COUNT(FL_DATE)+COUNT(*)"
fl_floats="SUM(DEP_TIME)+SUM(ARR_TIME)"

# DuckDB dot-commands (.timer) only work over stdin; the first "real" line is
# the SET statement, which we skip. Times are query-execution only (no startup).
duckdb_read() { # dataset path
  local ds=$1 path=$2 ints floats
  if [ "$ds" = flights ]; then ints=$fl_ints; floats=$fl_floats; else ints=$gen_ints; floats=$gen_floats; fi
  local bytes nrows val iv fv q
  bytes=$(stat -c%s "$path" 2>/dev/null || stat -f%z "$path")
  nrows=$(duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('$path');")
  val=$(duckdb -noheader -list -c "SELECT ($ints)::HUGEINT AS i, ($floats) AS f FROM read_parquet('$path');")
  iv=${val%|*}; fv=${val#*|}
  q="SELECT ($ints)::HUGEINT, ($floats) FROM read_parquet('$path');"
  { printf '.timer on\nSET threads=1;\n'; for _ in $(seq 1 "$REPS"); do printf '%s\n' "$q"; done; } \
    | duckdb 2>/dev/null | grep -oE 'real [0-9.]+' | awk 'NR>1{print $2}' \
    | while read -r t; do
        printf 'RESULT duckdb read %s none %.3f %s %s %s %s\n' \
          "$ds" "$(echo "$t*1000" | bc -l)" "$nrows" "$bytes" "$iv" "$fv"
      done
}

duckdb_write() { # csv codec   (COPY TO overwrites, so no rm needed)
  local csv=$1 codec=$2 out="$WORK/out_duckdb_${2}.parquet" nrows q
  nrows=$(($(wc -l < "$csv") - 1))
  q="COPY (SELECT * FROM read_csv('$csv', header=true)) TO '$out' (FORMAT parquet, COMPRESSION '$codec');"
  { printf '.timer on\nSET threads=1;\n'; for _ in $(seq 1 "$REPS"); do printf '%s\n' "$q"; done; } \
    | duckdb 2>/dev/null | grep -oE 'real [0-9.]+' | awk 'NR>1{print $2}' \
    | while read -r t; do
        local bytes; bytes=$(stat -c%s "$out" 2>/dev/null || stat -f%z "$out")
        printf 'RESULT duckdb write gen %s %.3f %s %s 0 0\n' \
          "$codec" "$(echo "$t*1000" | bc -l)" "$nrows" "$bytes"
      done
}

# ---- read benchmarks ----------------------------------------------------
if [[ ",$OPS," == *",read,"* ]]; then
  for ds in ${DATASETS//,/ }; do
    path=${RPATH[$ds]}
    [ -f "$path" ] || { log "skip read $ds (missing $path)"; continue; }
    log "read $ds"
    dart_bench read "$ds" "$path" "$REPS" >> "$RESULTS"
    "$JAVA" -jar "$JAR" read "$ds" "$path" "$REPS" >> "$RESULTS"
    duckdb_read "$ds" "$path" >> "$RESULTS"
  done
fi

# ---- write benchmarks (gen only) ----------------------------------------
if [[ ",$OPS," == *",write,"* ]]; then
  for codec in ${WRITE_CODECS//,/ }; do
    log "write gen $codec"
    dart_bench write gen "$WORK/gen.csv" "$codec" "$REPS" >> "$RESULTS"
    ( cd "$WORK" && "$JAVA" -jar "$JAR" write gen gen.csv "$codec" "$REPS" ) >> "$RESULTS"
    duckdb_write "$WORK/gen.csv" "$codec" >> "$RESULTS"
  done
fi

# ---- report -------------------------------------------------------------
log "building report"
python3 benchmark/report.py < "$RESULTS" | tee "benchmark/results/report.md"
