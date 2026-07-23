# SQL CLI

A native command-line interface for the libbigdata SQL engine, written in pure
Dart and compiled to a native executable.

## Features

- **Native executable**: compiled ahead-of-time for the host platform
- **Interactive REPL**: SQL shell with multi-line input
- **File support**: load CSV and Parquet files directly
- **Output formats**: table, JSON, and CSV
- **Query timing**: optional per-query execution time
- **Cross-platform**: macOS, Linux, and Windows

## Installation

### Option 1: Install from pub.dev
```bash
dart pub global activate libbigdata
```

This puts a `libbigdata` command on your path:

```bash
libbigdata --interactive
libbigdata -e "SELECT * FROM 'data.csv' LIMIT 5"
```

### Option 2: Build from Source
```bash
# Clone repository
git clone https://github.com/libdbm/libbigdata.git
cd libbigdata

# Build native executable
make build

# Install system-wide (optional)
make install
```

## Usage

### Command Line Options

```bash
sql [options] [SQL statement]

Options:
  -h, --help           Show help message
  -v, --version        Show version information
  -i, --interactive    Start interactive REPL mode
  -f, --file FILE      Execute SQL from file
  -e, --execute SQL    Execute SQL statement directly
  -c, --csv NAME:PATH  Load CSV file as table
  -p, --parquet NAME:PATH Load Parquet file as table  
  -o, --output FORMAT  Output format (table, json, csv)
  -t, --timing         Show query execution time
  --verbose            Show verbose output including query plan
```

### Examples

#### Execute Single Query
```bash
# Simple query
sql -e "SELECT 1 + 1 as result"

# Load CSV and query
sql -c employees:data.csv -e "SELECT COUNT(*) FROM employees"

# With timing
sql -t -c sales:sales.csv -e "SELECT SUM(amount) FROM sales"
```

#### Interactive Mode
```bash
# Start interactive shell
sql -i

# Or with pre-loaded data
sql -c employees:data.csv -i
```

#### Output Formats
```bash
# Table format (default)
sql -c data:file.csv -e "SELECT * FROM data LIMIT 5"

# JSON output
sql -o json -c data:file.csv -e "SELECT * FROM data LIMIT 5"

# CSV output  
sql -o csv -c data:file.csv -e "SELECT * FROM data LIMIT 5"
```

#### File Execution
```bash
# Execute SQL script
sql -f queries.sql

# With data loading
sql -c employees:data.csv -f analysis.sql
```

## Interactive Mode Commands

Once in interactive mode (`sql -i`), you can use these special commands:

- `.help` - Show help
- `.tables` - List loaded tables
- `.schema TABLE` - Show table schema
- `.load csv NAME:PATH` - Load CSV file
- `.load parquet NAME:PATH` - Load Parquet file
- `.output FORMAT` - Set output format (table, json, csv)
- `.timing on|off` - Toggle query timing
- `.verbose on|off` - Toggle verbose output
- `.quit` - Exit

### Interactive Example Session
```sql
sql> .load csv employees:data.csv
Loading CSV file: data.csv as table "employees"...
Successfully loaded CSV file as table "employees"

sql> .tables
Tables:
  employees (1000 rows)

sql> SELECT department, AVG(salary) as avg_sal 
   > FROM employees 
   > GROUP BY department 
   > ORDER BY avg_sal DESC;

department | avg_sal
----------------------
Engineering| 85000.0
Marketing  | 78000.0
Sales      | 72000.0
HR         | 65000.0

(4 rows)

sql> .timing on
Timing enabled

sql> SELECT COUNT(*) FROM employees WHERE salary > 80000;
Execution time: 2ms

count
-----
234

(1 rows)

sql> .quit
Goodbye!
```

## Supported SQL Features

The CLI runs the same SQL engine as the library. For the full list of supported
and unsupported syntax, see [SQL.md](SQL.md).

## Performance

The engine uses columnar, batch-oriented execution. In practice:

- Scans, filters, and aggregations are the fastest operations.
- `ORDER BY` requires a full sort and is the most expensive operation.
- Results larger than memory are processed in streaming batches, and operators
  that exceed the memory budget spill to disk rather than failing.

### Performance tips

1. Use `LIMIT` for large result sets.
2. Project only the columns you need.
3. Filter early with `WHERE`.
4. Avoid `ORDER BY` unless the ordering is required.

## File Format Support

### CSV Files
```bash
# Load CSV with header row
sql -c table_name:file.csv -e "SELECT * FROM table_name"

# The first row is automatically treated as column headers
```

### Parquet Files
```bash
# Load Parquet file
sql -p table_name:file.parquet -e "SELECT * FROM table_name"

# Preserves original data types and schema
```

## Build System

### Available Make Targets
```bash
make help          # Show all available commands
make build         # Build release executable
make build-debug   # Build debug executable  
make build-all     # Build for all platforms
make install       # Install to /usr/local/bin
make clean         # Clean build artifacts
make test          # Run tests
make run-example   # Run example queries
```

### Cross-Platform Builds
```bash
# Build for specific platform
./build.sh --platform linux-x64

# Build for all platforms
./build.sh --all-platforms

# Supported platforms:
# - macos-x64, macos-arm64
# - linux-x64, linux-arm64  
# - windows-x64
```

## Development

### Running from Source
```bash
# Run directly with Dart VM
dart run bin/sql.dart --help

# Or use make target
make dev-run
```

### File Structure
```
bin/sql.dart           # Main CLI application
build.sh               # Native build script
Makefile               # Build automation
lib/                   # SQL engine library
  src/sql/             # SQL parser and execution engine
  src/core/            # Core data structures
  src/formats/         # File format readers
```

## Troubleshooting

### Common Issues

1. **Build fails**: Ensure Dart SDK 3.8+ is installed
2. **CSV loading fails**: Check file path and format
3. **Query syntax error**: Verify SQL syntax matches supported dialect
4. **Memory issues**: Use `LIMIT` for large datasets

### Getting Help
- Run `sql --help` for command options
- Use `.help` in interactive mode
- Check query syntax against SQL documentation
- Report issues on the project repository

## License

This CLI tool is part of the BigData library project.