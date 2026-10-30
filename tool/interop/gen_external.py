#!/usr/bin/env python3
"""Generate external-engine-written fixtures for test/interop/external_read_test.dart.

Each engine writes a format that libbigdata then READS and asserts. Every fixture
is generated in its own try/except: an engine that isn't installed (or fails, e.g.
Spark on an unsupported JDK) simply omits its `WROTE <name>` line, and the matching
Dart test skips via need(). Always prints DONE last so setUpAll proceeds.

Usage: python3 tool/interop/gen_external.py <output-dir>
"""
import sys
import os
import traceback

OUT = sys.argv[1]
os.makedirs(OUT, exist_ok=True)


def wrote(name):
    print(f"WROTE {name}")


def attempt(name, fn):
    try:
        fn()
        wrote(name)
    except Exception:
        # Diagnostic on stderr; the fixture just won't be marked WROTE.
        sys.stderr.write(f"[skip {name}] {traceback.format_exc()}\n")


# --- pyarrow: nested (list/struct/map), BYTE_STREAM_SPLIT, unsigned ints -------
def gen_pa_nested():
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "tags": pa.array([[1, 2], [], [9]], type=pa.list_(pa.int64())),
        "info": pa.array(
            [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}],
            type=pa.struct([("a", pa.int64()), ("b", pa.string())]),
        ),
        "props": pa.array(
            [[("k1", 1)], [("k2", 2)], [("k3", 3)]],
            type=pa.map_(pa.string(), pa.int64()),
        ),
    })
    pq.write_table(table, f"{OUT}/pa_nested.parquet")


def gen_pa_bss():
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "f": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
        "g": pa.array([1.25, 2.5, 3.75], type=pa.float64()),
        "h": pa.array([1.5, None, 3.5], type=pa.float32()),
    })
    pq.write_table(
        table,
        f"{OUT}/pa_bss.parquet",
        use_dictionary=False,
        column_encoding={
            "f": "BYTE_STREAM_SPLIT",
            "g": "BYTE_STREAM_SPLIT",
            "h": "BYTE_STREAM_SPLIT",
        },
    )


def gen_pa_uint():
    import pyarrow as pa
    import pyarrow.parquet as pq

    # High-bit uint32 values (> 2^31) across multiple row groups so a stats-pruned
    # read must decode UINT_32 min/max as UNSIGNED (a signed decode reads them
    # negative and wrongly prunes the high row group).
    table = pa.table({
        "u32": pa.array(
            [4000000000, 4000000001, 3000000000, 100],
            type=pa.uint32(),
        ),
    })
    pq.write_table(table, f"{OUT}/pa_uint.parquet", row_group_size=2)


# --- delta-rs: partitioned Delta ----------------------------------------------
def gen_drs_delta():
    import pyarrow as pa
    from deltalake import write_deltalake

    table = pa.table({
        "id": pa.array([1, 2, 3, 4], type=pa.int64()),
        "region": pa.array(["us", "eu", "us", "eu"]),
    })
    write_deltalake(f"{OUT}/drs_delta", table, partition_by=["region"])


# --- pyiceberg: typed (decimal + timestamp) -----------------------------------
def gen_pyice():
    import decimal
    import datetime
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = f"{OUT}/pyice"
    os.makedirs(warehouse, exist_ok=True)
    catalog = SqlCatalog(
        "ice",
        **{
            "uri": f"sqlite:///{warehouse}/catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    schema = pa.schema([
        ("amt", pa.decimal128(5, 2)),
        ("ts", pa.timestamp("us")),
    ])
    data = pa.table(
        {
            "amt": pa.array([decimal.Decimal("123.45")], type=pa.decimal128(5, 2)),
            "ts": pa.array(
                [datetime.datetime(2021, 1, 2, 3, 4, 5)], type=pa.timestamp("us")
            ),
        },
        schema=schema,
    )
    catalog.create_namespace_if_not_exists("db")
    table = catalog.create_table("db.t", schema=schema, location=f"{warehouse}/t")
    table.append(data)


# --- pyiceberg: a column dropped and re-added under the same name ------------
def gen_pyice_readd():
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.types import StringType

    warehouse = f"{OUT}/pyice_readd"
    os.makedirs(warehouse, exist_ok=True)
    catalog = SqlCatalog(
        "ice",
        **{
            "uri": f"sqlite:///{warehouse}/catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    old = pa.table({
        "id": pa.array([1, 2], pa.int64()),
        "a": pa.array(["old1", "old2"]),
    })
    catalog.create_namespace_if_not_exists("db")
    table = catalog.create_table("db.t", schema=old.schema, location=f"{warehouse}/t")
    table.append(old)
    with table.update_schema() as update:
        update.delete_column("a")
    with table.update_schema() as update:
        update.add_column("a", StringType())
    table = catalog.load_table("db.t")
    table.append(pa.table({
        "id": pa.array([3], pa.int64()),
        "a": pa.array(["new3"]),
    }))


# --- pyiceberg: a tag on the first of three snapshots ------------------------
def gen_pyice_tag():
    import time
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = f"{OUT}/pyice_tag"
    os.makedirs(warehouse, exist_ok=True)
    catalog = SqlCatalog(
        "ice",
        **{
            "uri": f"sqlite:///{warehouse}/catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    first = pa.table({"id": pa.array([1], pa.int64())})
    catalog.create_namespace_if_not_exists("db")
    table = catalog.create_table("db.t", schema=first.schema, location=f"{warehouse}/t")
    table.append(first)
    tagged = table.current_snapshot().snapshot_id
    table.manage_snapshots().create_tag(tagged, "v1").commit()
    table = catalog.load_table("db.t")
    time.sleep(0.02)
    table.overwrite(pa.table({"id": pa.array([2], pa.int64())}))
    time.sleep(0.02)
    table.append(pa.table({"id": pa.array([3], pa.int64())}))


# --- pyiceberg: a plain Parquet file added, then a column renamed ------------
def gen_pyice_mapped():
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = f"{OUT}/pyice_mapped"
    os.makedirs(f"{warehouse}/raw", exist_ok=True)
    catalog = SqlCatalog(
        "ice",
        **{
            "uri": f"sqlite:///{warehouse}/catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    data = pa.table({
        "id": pa.array([1, 2], pa.int64()),
        "a": pa.array(["x", "y"]),
    })
    # Written by pyarrow, so the file carries no Iceberg field ids.
    pq.write_table(data, f"{warehouse}/raw/f.parquet")
    catalog.create_namespace_if_not_exists("db")
    table = catalog.create_table("db.t", schema=data.schema, location=f"{warehouse}/t")
    table.add_files([f"{warehouse}/raw/f.parquet"])
    table = catalog.load_table("db.t")
    # pyiceberg records the old name in schema.name-mapping.default.
    with table.update_schema() as update:
        update.rename_column("a", "b")


# --- fastavro: logical types (date, timestamp, decimal) -----------------------
def gen_avro():
    import datetime
    import decimal
    import fastavro

    schema = {
        "type": "record",
        "name": "L",
        "fields": [
            {"name": "d", "type": {"type": "int", "logicalType": "date"}},
            {
                "name": "ts",
                "type": {"type": "long", "logicalType": "timestamp-micros"},
            },
            {
                "name": "amt",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 5,
                    "scale": 2,
                },
            },
        ],
    }
    records = [{
        "d": datetime.date(2021, 1, 2),
        "ts": datetime.datetime(2021, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "amt": decimal.Decimal("123.45"),
    }]
    with open(f"{OUT}/logical.avro", "wb") as fh:
        fastavro.writer(fh, fastavro.parse_schema(schema), records)


# --- Spark: nested + INT96 timestamps -----------------------------------------
def gen_spark_nested():
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    import datetime

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("interop")
        .config("spark.sql.parquet.outputTimestampType", "INT96")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        rows = [
            Row(id=1, tags=[1, 2], info=Row(a=1, b="x"),
                ts=datetime.datetime(2021, 1, 2, 3, 4, 5)),
            Row(id=2, tags=[3], info=Row(a=2, b="y"),
                ts=datetime.datetime(2021, 6, 7, 8, 9, 10)),
        ]
        spark.createDataFrame(rows).write.mode("overwrite").parquet(
            f"{OUT}/spark_nested"
        )
    finally:
        spark.stop()


attempt("pa_nested", gen_pa_nested)
attempt("pa_bss", gen_pa_bss)
attempt("pa_uint", gen_pa_uint)
attempt("drs_delta", gen_drs_delta)
attempt("pyice", gen_pyice)
attempt("pyice_readd", gen_pyice_readd)
attempt("pyice_tag", gen_pyice_tag)
attempt("pyice_mapped", gen_pyice_mapped)
attempt("avro", gen_avro)
attempt("spark_nested", gen_spark_nested)

print("DONE")
