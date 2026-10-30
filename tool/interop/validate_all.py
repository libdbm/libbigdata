#!/usr/bin/env python3
"""Read the libbigdata-written interop fixtures with external engines and emit a
single `RESULT <json>` line that test/interop/spark_interop_test.dart asserts on.
Each fixture is validated in its own try/except so one engine's failure surfaces
as {ok: false, error: ...} for that key rather than aborting the whole run.

Usage: python3 tool/interop/validate_all.py <dir>
"""
import sys
import os
import glob
import json
import traceback

D = sys.argv[1]
results = {}


def check(name, fn):
    try:
        results[name] = {"ok": True, **fn()}
    except Exception as e:
        results[name] = {"ok": False, "error": f"{e}\n{traceback.format_exc()}"}


def sstr(v):
    """JSON-safe scalar (Decimal/date/bytes -> str)."""
    import decimal
    import datetime
    if isinstance(v, (decimal.Decimal, datetime.date, datetime.datetime, bytes)):
        return str(v)
    return v


# ---- pyarrow: int32 DELTA -----------------------------------------------------
def v_int32_delta():
    import pyarrow.parquet as pq
    t = pq.read_table(f"{D}/int32delta.parquet")
    return {"type": str(t.schema.field("v").type), "v": t.column("v").to_pylist()}


check("parquet_int32_delta", v_int32_delta)


# ---- delta-rs -----------------------------------------------------------------
def v_delta_partitioned():
    from deltalake import DeltaTable
    t = DeltaTable(f"{D}/delta_part").to_pyarrow_table()
    return {"cols": t.column_names, "rows": t.num_rows}


def v_delta():
    from deltalake import DeltaTable
    t = DeltaTable(f"{D}/delta_dec").to_pyarrow_table()
    return {
        "dec_type": str(t.schema.field("dec").type),
        "dec": [str(x) for x in t.column("dec").to_pylist()],
    }


def v_delta_partition_values():
    from deltalake import DeltaTable
    import pyarrow.compute as pc
    t = DeltaTable(f"{D}/delta_pv").to_pyarrow_table()
    t = t.sort_by("id")
    return {
        "region": t.column("region").to_pylist(),
        "d": [str(x) for x in t.column("d").to_pylist()],
    }


def v_delta_nested():
    from deltalake import DeltaTable
    table = DeltaTable(f"{D}/delta_nested")
    rows = sorted(table.to_pyarrow_table().to_pylist(), key=lambda r: r["id"])
    for r in rows:
        r["props"] = dict(r["props"])
    return {"schema": json.loads(table.schema().to_json()), "rows": rows}


check("delta_partitioned", v_delta_partitioned)
check("delta_nested", v_delta_nested)
check("delta", v_delta)
check("delta_partition_values", v_delta_partition_values)


# ---- pyiceberg ----------------------------------------------------------------
def _ice(sub):
    from pyiceberg.table import StaticTable
    metas = sorted(glob.glob(f"{D}/{sub}/t/metadata/*.metadata.json"))
    return StaticTable.from_metadata(metas[-1])


def v_iceberg():
    t = _ice("ice_dec")
    arr = t.scan().to_arrow()
    return {"dec_type": str(arr.schema.field("dec").type), "rows": arr.num_rows}


def v_iceberg_expired():
    t = _ice("ice_expired")
    arr = t.scan().to_arrow()
    return {"nsnapshots": len(t.metadata.snapshots), "rows": arr.num_rows}


def v_iceberg_history():
    t = _ice("ice_history")
    snapshots = t.metadata.snapshots
    return {
        "metadata_log": len(t.metadata.metadata_log),
        "history": len(t.history()),
        "parents_linked": all(
            s.parent_snapshot_id == snapshots[i - 1].snapshot_id
            for i, s in enumerate(snapshots)
            if i > 0
        ),
        "first_parent": snapshots[0].parent_snapshot_id,
    }


def v_iceberg_repartitioned():
    t = _ice("ice_repart")
    arr = t.scan().to_arrow()
    country = arr.column("country").to_pylist()
    region = arr.column("region").to_pylist()
    # order-independent: the test asserts the SET/order of country; sort by id.
    order = sorted(range(arr.num_rows), key=lambda i: arr.column("id").to_pylist()[i])
    country = [country[i] for i in order]
    return {
        "nspecs": len(t.metadata.partition_specs),
        "default_spec": t.metadata.default_spec_id,
        "rows": arr.num_rows,
        "country": country,
        "region_eu": sum(1 for r in region if r == "eu"),
    }


def v_iceberg_partitioned():
    t = _ice("ice_part")
    arr = t.scan().to_arrow()
    spec = t.metadata.partition_specs[t.metadata.default_spec_id]
    return {"nparts": len(spec.fields), "rows": arr.num_rows}


check("iceberg", v_iceberg)
check("iceberg_expired", v_iceberg_expired)
check("iceberg_history", v_iceberg_history)
check("iceberg_repartitioned", v_iceberg_repartitioned)
check("iceberg_partitioned", v_iceberg_partitioned)


# ---- Spark (one session for all its reads) ------------------------------------
def run_spark():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("validate")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        def rows_of(df):
            return [
                {k: sstr(v) for k, v in r.asDict(recursive=True).items()}
                for r in df.collect()
            ]

        def flat():
            df = spark.read.parquet(f"{D}/flat.parquet")
            return {"rows": rows_of(df)}

        def nested():
            df = spark.read.parquet(f"{D}/nested.parquet")
            return {"schema": df.schema.simpleString()}

        def deep():
            df = spark.read.parquet(f"{D}/deep.parquet")
            rs = [
                {"id": r["id"], "matrix": r["matrix"]}
                for r in df.collect()
            ]
            return {"schema": df.schema.simpleString(), "rows": rs}

        def required_nested():
            df = spark.read.parquet(f"{D}/required_nested.parquet")
            a = [r["s"]["a"] for r in df.collect()]
            return {"a": a}

        def csv():
            df = spark.read.option("header", True).csv(f"{D}/data.csv")
            return {"rows": rows_of(df)}

        def json_lines():
            df = spark.read.json(f"{D}/data.jsonl")
            return {"count": df.count()}

        check("parquet_flat", flat)
        check("parquet_nested", nested)
        check("parquet_deep", deep)
        check("parquet_required_nested", required_nested)
        check("csv", csv)
        check("json", json_lines)
    finally:
        spark.stop()


try:
    run_spark()
except Exception as e:
    for k in ["parquet_flat", "parquet_nested", "parquet_deep",
              "parquet_required_nested", "csv", "json"]:
        results.setdefault(k, {"ok": False, "error": f"spark unavailable: {e}"})


print("RESULT " + json.dumps(results))
