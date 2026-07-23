# Iceberg regression fixtures

`pyiceberg_written/` is an Apache Iceberg table (format-version 2) authored by
**pyiceberg** (two appends → ids a..e), used by `test/iceberg_test.dart` to guard
read compatibility with another implementation's on-disk format (metadata.json,
Avro manifest list + manifests, Parquet data) without needing Python/Docker in CI.

It is read via path **re-rooting**: the manifest/metadata embed absolute
`file://` paths, so the reader (`IcebergTable._resolve`) re-bases them under the
fixture directory.

## Regenerating / cross-validating

This library's `IcebergWriter` output was cross-validated both directions:
- **pyiceberg** and **DuckDB** read a libbigdata-written table correctly.
- libbigdata reads pyiceberg's table (this fixture).

```python
# generate (pyiceberg, no Spark/JVM needed)
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
cat = SqlCatalog("d", uri="sqlite:///cat.db", warehouse="file:///tmp/wh")
cat.create_namespace("ns")
data = pa.table({"id": pa.array([1,2,3], pa.int64()), "name": pa.array(["a","b","c"])})
t = cat.create_table("ns.t", schema=data.schema); t.append(data)
t.append(pa.table({"id": pa.array([4,5], pa.int64()), "name": pa.array(["d","e"])}))
```

```sql
-- DuckDB reads a libbigdata-written table:
LOAD iceberg; SELECT * FROM iceberg_scan('<table>/metadata/<latest>.metadata.json');
```

Note: libbigdata's Parquet writer does not emit Iceberg field-ids, so the writer
declares `schema.name-mapping.default` in table properties (readers map columns by
name). Unpartitioned tables only.
