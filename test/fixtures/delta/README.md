# Delta Lake regression fixtures

Checked-in Delta tables used by `test/delta_test.dart` (the `cross-engine fixtures` group) to guard
deletion-vector **read** compatibility across engines without needing
Docker/Spark/DuckDB in CI. Each table deletes rows via a merge-on-read deletion
vector (no data-file rewrite), so reading them exercises the DV path.

| Fixture | Author | Expected live `id`s |
|---|---|---|
| `dart_written` | this library | `1,3,5,7,9,100..104` |
| `spark_written` | Apache Spark 3.5.3 + delta-spark 3.2.0 | `0,1,2,3,4` |
| `spark4_written` | Apache Spark 4.0.0 + delta-spark 4.0.0 | `0,1,2,3,4` |
| `spark4_row_groups` | `spark4_written`, data file rewritten by pyarrow | `0,1,2,3,4` |
| `rs_checkpoint` | delta-rs (`deltalake` 1.6.0 for Python) | `0..2, 4..17, 100..103` |
| `rs_multipart` | `rs_checkpoint`, checkpoint split by pyarrow | `0..2, 4..17, 100..103` |
| `rs_cleaned` | delta-rs (`deltalake` 1.6.0 for Python) | `0..19` |
| `rs_nested` | delta-rs (`deltalake` 1.6.0 for Python) | `1,2,3` |

`rs_checkpoint` exercises the Delta-standard checkpoint rather than deletion
vectors: commits 0 to 8 append two rows each (partitioned by `part`), commit 9
deletes `id = 3`, `create_checkpoint()` writes `_last_checkpoint` and
`00000000000000000009.checkpoint.parquet`, and commits 10 and 11 append
`100..103`. It was generated with:

```python
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

def batch(start):
    ids = list(range(start, start + 2))
    return pa.table({'id': pa.array(ids, pa.int64()),
                     'name': pa.array([f'n{i}' for i in ids]),
                     'part': pa.array(['even' if i % 2 == 0 else 'odd' for i in ids])})

write_deltalake(path, batch(0), partition_by=['part'])
for v in range(1, 9):
    write_deltalake(path, batch(2 * v), mode='append')
DeltaTable(path).delete('id = 3')
DeltaTable(path).create_checkpoint()
write_deltalake(path, batch(100), mode='append')
write_deltalake(path, batch(102), mode='append')
```

The tables are relocatable (relative `add.path`, `storageType: 'u'` deletion
vectors), so they read correctly from this directory.

## Regenerating / cross-validating

The `dart_written` fixture is checked in. It was cross-validated by
DuckDB's delta extension and by both Spark versions reading it:

```
duckdb -c "INSTALL delta; LOAD delta; SELECT * FROM delta_scan('<path>')"
```

The Spark fixtures were produced in a container (Spark range(0,10) →
`repartition(1)` so a single file survives the delete → `DELETE WHERE id >= 5`
with `delta.enableDeletionVectors=true`):

```
docker run --rm -v $PWD:/data eclipse-temurin:17-jdk-jammy bash -c '
  apt-get update -qq && apt-get install -y -qq python3 python3-pip
  pip3 install -q pyspark==4.0.0 delta-spark==4.0.0     # or 3.5.3 / 3.2.0
  python3 /data/run_interop4.py'
```

`rs_multipart` is `rs_checkpoint` with its checkpoint split into the standard
two-part form (`00000000000000000009.checkpoint.0000000001.0000000002.parquet`
and `...0000000002.0000000002.parquet`, ten rows each, written by pyarrow) and
`"parts": 2` added to `_last_checkpoint`. delta-rs reads it back with the same
live ids. pyarrow writes the all-null columns of each part as zero-byte pages.

`spark4_row_groups` is `spark4_written` with its one data file rewritten as five
row groups of two rows, same rows in the same order, so a scan that pruned row
groups inside a file with a deletion vector would apply the deleted positions
to the wrong rows. The log, its checksums and the deletion vector are
unchanged. It was generated with:

```python
import pyarrow.parquet as pq
pq.write_table(pq.read_table(path), path, row_group_size=2, compression='snappy')
```

`rs_cleaned` has two checkpoints and a cleaned log: commits 0 to 9 each append
ids `2v` and `2v+1`, `create_checkpoint()` runs after commits 7 and 9, and the
JSON commits 0 to 6 are then deleted. Version 7 reads 16 rows only from the
version-7 checkpoint, which `_last_checkpoint` (version 9) does not name. It
was generated with:

```python
import os
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

def batch(start):
    ids = list(range(start, start + 2))
    return pa.table({'id': pa.array(ids, pa.int64()),
                     'name': pa.array([f'n{i}' for i in ids])})

write_deltalake(path, batch(0))
for v in range(1, 8):
    write_deltalake(path, batch(2 * v), mode='append')
DeltaTable(path).create_checkpoint()
write_deltalake(path, batch(16), mode='append')
write_deltalake(path, batch(18), mode='append')
DeltaTable(path).create_checkpoint()
for v in range(0, 7):
    os.remove(f'{path}/_delta_log/{v:020d}.json')
```

`rs_nested` holds struct, array and map columns with nulls at each level. It
was generated with:

```python
import pyarrow as pa
from deltalake import write_deltalake

write_deltalake(path, pa.table({
    'id': pa.array([1, 2, 3], pa.int64()),
    'info': pa.array([{'a': 1, 'b': 'x'}, None, {'a': 3, 'b': None}],
                     pa.struct([('a', pa.int32()), ('b', pa.string())])),
    'tags': pa.array([[1, 2], [], None], pa.list_(pa.int64())),
    'props': pa.array([[('k1', 1.5)], None, []],
                      pa.map_(pa.string(), pa.float64())),
}))
```
