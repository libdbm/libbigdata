#!/usr/bin/env python3
"""Aggregate RESULT lines into a markdown comparison and cross-check aggregates.

RESULT <engine> <op> <dataset> <codec> <ms> <rows> <bytes> <ints> <floats>
"""
import sys
from collections import defaultdict
from statistics import median

rows = defaultdict(list)   # (op,dataset,codec,engine) -> [ms,...]
meta = {}                  # (op,dataset,codec,engine) -> (rows,bytes,ints,floats)

for line in sys.stdin:
    p = line.split()
    if len(p) < 10 or p[0] != "RESULT":
        continue
    _, engine, op, dataset, codec, ms, nrows, nbytes, ints, floats = p[:10]
    key = (op, dataset, codec, engine)
    rows[key].append(float(ms))
    meta[key] = (int(nrows), int(nbytes), int(ints), float(floats))

ENGINES = ["libbigdata", "duckdb", "parquet-mr"]


def med(ms):
    s = sorted(ms)
    if len(s) > 1:
        s = s[1:]  # drop warm-up
    return median(s)


def cell_groups():
    groups = defaultdict(dict)  # (op,dataset,codec) -> engine -> stats
    for (op, dataset, codec, engine), ms in rows.items():
        groups[(op, dataset, codec)][engine] = (med(ms), meta[(op, dataset, codec, engine)])
    return groups


groups = cell_groups()
ok = True
print("# Parquet benchmark: libbigdata vs DuckDB vs parquet-mr\n")

for op in ("read", "write"):
    keys = sorted(k for k in groups if k[0] == op)
    if not keys:
        continue
    print(f"## {op.upper()}\n")
    print("| dataset | codec | engine | median ms | rows/s | MB/s | bytes |")
    print("|---|---|---|--:|--:|--:|--:|")
    for (o, dataset, codec) in keys:
        per = groups[(o, dataset, codec)]
        # fastest for relative annotation
        best = min((s[0] for s in per.values()), default=0)
        for engine in ENGINES:
            if engine not in per:
                continue
            ms, (nrows, nbytes, ints, floats) = per[engine]
            rps = nrows / (ms / 1000) if ms else 0
            mbps = (nbytes / 1e6) / (ms / 1000) if ms else 0
            rel = f" ({ms/best:.1f}x)" if best and ms > best * 1.05 else ""
            print(f"| {dataset} | {codec} | {engine} | {ms:.1f}{rel} | "
                  f"{rps:,.0f} | {mbps:,.1f} | {nbytes:,} |")
    print()

# Cross-check read aggregates across engines.
print("## Correctness (read aggregates match across engines)\n")
for (op, dataset, codec) in sorted(groups):
    if op != "read":
        continue
    per = groups[(op, dataset, codec)]
    ints = {e: s[1][2] for e, s in per.items()}
    floats = {e: s[1][3] for e, s in per.items()}
    iset = set(ints.values())
    fvals = list(floats.values())
    fok = (max(fvals) - min(fvals)) <= max(1.0, abs(max(fvals)) * 1e-4) if fvals else True
    good = (len(iset) == 1) and fok
    ok = ok and good
    status = "PASS" if good else "FAIL"
    print(f"- {dataset}: {status} ints={ints} floats~={ {e: round(v,1) for e,v in floats.items()} }")

print()
print("RESULT_CHECK", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
