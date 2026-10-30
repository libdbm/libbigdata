# Parquet Support

libbigdata reads and writes Apache Parquet files in pure Dart, with no native
dependencies. This document describes the supported format features.

## Reading and writing

```dart
import 'package:libbigdata/libbigdata.dart';

// Read a Parquet file into a DataFrame. Reads and writes return a Result.
final df = await DataFrameIO.readParquet('data.parquet');

// Read with column projection and a predicate pushed down to the reader.
final filtered = await DataFrameIO.readParquet(
  'data.parquet',
  columns: ['id', 'name', 'amount'],
  filter: RowFilter.compare('amount', FilterOp.gt, 1000),
);
if (filtered case Err(:final error)) print('${error.kind}: ${error.message}');

// Write a Table; an Err (Kind.io, Kind.invalid, ...) leaves no partial file.
final written = await ParquetWriter('out.parquet').write(table);

// Write a large result out-of-core, one row group per batch.
final streamed = await DataFrameIO.writeParquetStream('out.parquet', batches);
```

Files can also be queried directly through the SQL engine, which pushes column
projection and simple `WHERE` predicates down into the reader:

```dart
final result = await sql(
  "SELECT id, amount FROM 'data.parquet' WHERE amount > 1000",
);
```

## Format coverage

### File structure

- PAR1 magic-byte validation on header and footer.
- Full `FileMetaData` parsing (schema, row groups, column chunks, key/value
  metadata, sorting columns).
- Parquet format versions 1 and 2.
- Column and predicate pushdown: unneeded columns are skipped, and row groups
  whose statistics cannot satisfy the predicate are pruned. Pruning applies to
  comparisons of numbers, strings and booleans with literals; a date or
  timestamp predicate is still applied exactly, but does not prune.
- In SQL, each scan of a file prunes with its own `WHERE`: two `UNION ALL`
  branches over the same file each skip the row groups their own predicate
  rules out, and neither filters the other.

### Page types

| Page type       | Read | Write |
|-----------------|:----:|:-----:|
| Data page v1    | Yes  | Yes   |
| Data page v2    | Yes  | No    |
| Dictionary page | Yes  | Yes   |
| Index page      | Metadata only | No |

### Compression codecs

| Codec         | Read | Write |
|---------------|:----:|:-----:|
| Uncompressed  | Yes  | Yes   |
| Snappy        | Yes  | Yes   |
| GZIP          | Yes  | Yes   |
| ZSTD          | Yes  | Yes   |
| LZ4_RAW       | Yes  | Yes   |
| LZ4 (Hadoop)  | Yes  | No    |
| Brotli        | No   | No    |
| LZO           | No   | No    |

The modern `LZ4_RAW` codec is read and written. The legacy Hadoop-framed `LZ4`
codec is read-only for backward compatibility. Brotli and LZO are not supported.
A writer chooses its codec with `ParquetWriterOptions(compression:
ParquetCompression.zstd)`; `ParquetCompression` lists exactly the writable
codecs (`none`, `snappy`, the default, `gzip`, `zstd` and `lz4Raw`).

### Encodings

Read:

- PLAIN
- PLAIN_DICTIONARY and RLE_DICTIONARY
- RLE / bit-packing (definition and repetition levels)
- DELTA_BINARY_PACKED
- DELTA_LENGTH_BYTE_ARRAY
- DELTA_BYTE_ARRAY
- BYTE_STREAM_SPLIT

Write:

- PLAIN
- Dictionary (PLAIN_DICTIONARY / RLE_DICTIONARY)
- DELTA_BINARY_PACKED

### Data types

Physical types: `INT32`, `INT64`, `INT96`, `FLOAT`, `DOUBLE`, `BOOLEAN`,
`BYTE_ARRAY`, and `FIXED_LEN_BYTE_ARRAY`.

Logical/converted types: UTF-8 strings, `DECIMAL` (fixed-length and byte-array
backed), `DATE`, `TIME`, `TIMESTAMP`, `ENUM`, `JSON`, `BSON`, `UUID`, and
`FLOAT16` (half precision). Written files carry both the modern logical type
and the legacy converted type for strings, decimals, dates and timestamps;
timestamps are microseconds adjusted to UTC. Unsigned integer types are widened losslessly, and
`uint64` maps to `DECIMAL(20,0)` to avoid overflow.

### Nested structures

Nested schemas are handled with a recursive Dremel shred/assemble
implementation, including definition and repetition levels:

- Lists (including nested lists)
- Maps
- Structs
- Arbitrary combinations of the above

### Integrity and statistics

- Column statistics (min/max/null count) are written and used for
  predicate pushdown on read.
- Optional page-level CRC32 checksums are written and verified.
- Field IDs are preserved for interoperability with schema-tracking readers.

### Untrusted files

A corrupt or hostile file is reported as an `Err` (`Kind.corrupt`, or
`Kind.limit` past a cap) rather than a raw `RangeError`, a stack overflow, or an
unbounded allocation:

- The footer is at most 128 MiB, and its schema at most 100 levels deep and
  1,048,576 elements, with child counts that must match the element list.
- Each page decodes at most the value count its header declares and at most
  256 MiB, whatever its run lengths or delta streams claim. V2 page null counts
  and level lengths are range-checked before allocation.
- Unknown enum values and missing required header fields are rejected.
- A Thrift list, set or map in the footer or a page header may declare no more
  elements than there are bytes left to read them from.

A mutation test feeds 10,000 corrupted copies of valid files to the reader:

```bash
dart test -t fuzz --run-skipped
```

## Interoperability

The reader is exercised against Parquet files produced by DuckDB, Apache Arrow
(C++/Rust), Parquet-MR, Spark, and the files in the Apache `parquet-testing`
corpus. Round-trip compatibility with external engines (pyarrow, pyspark,
parquet-rs) is checked by an opt-in interoperability test suite:

```bash
dart test -t interop --run-skipped
```

## Streaming

Both reading and writing support out-of-core operation for files larger than
memory:

- `ParquetReader.readBatches({int batchSize})`, and the lazy frame
  `DataFrameIO.readParquet` returns, stream row groups without materializing
  the whole file.
- `ParquetWriter.writeBatches(Stream<Table>)` and
  `DataFrameIO.writeParquetStream` accrue row groups incrementally.
