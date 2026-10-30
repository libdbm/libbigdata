#!/usr/bin/env python3
"""Check the prerequisites of the interop suite before `make interop` runs it.

Prints one line per prerequisite and exits 1 if any is missing, so a missing
engine reads as a sentence with an install hint rather than a stack trace from
deep inside a test.

Usage: python3 tool/interop/check.py
"""
import importlib.util
import shutil
import subprocess
import sys

# (module, what uses it, how to install)
MODULES = [
    ("pyarrow", "Parquet reads and fixtures", "pip install pyarrow"),
    ("deltalake", "delta-rs Delta reads and fixtures", "pip install deltalake"),
    ("pyiceberg", "Iceberg reads and fixtures", "pip install 'pyiceberg[sql-sqlite]'"),
    ("fastavro", "Avro fixtures", "pip install fastavro"),
    ("pyspark", "Spark reads and fixtures", "pip install pyspark"),
]

# (command, what uses it, how to install)
COMMANDS = [
    ("dart", "the test runner", "https://dart.dev/get-dart"),
    ("duckdb", "DuckDB reads", "brew install duckdb, or https://duckdb.org"),
    ("java", "Spark (needs a JDK 17 or 21)", "install a JDK and put java on PATH"),
]


def main():
    missing = []
    for name, purpose, hint in MODULES:
        found = importlib.util.find_spec(name) is not None
        report(f"python module {name}", purpose, found, hint, missing)
    for name, purpose, hint in COMMANDS:
        found = shutil.which(name) is not None
        if found and name == "java":
            # `java` may be a shim with no JDK behind it.
            found = subprocess.run(
                ["java", "-version"], capture_output=True
            ).returncode == 0
        report(f"command {name}", purpose, found, hint, missing)
    if missing:
        print(
            f"\nmake interop: {len(missing)} prerequisite(s) missing: "
            f"{', '.join(missing)}. See tool/interop/README.md."
        )
        sys.exit(1)
    print("\nAll interop prerequisites found.")


def report(label, purpose, found, hint, missing):
    if found:
        print(f"  found    {label} ({purpose})")
    else:
        print(f"  MISSING  {label} ({purpose}): {hint}")
        missing.append(label)


if __name__ == "__main__":
    main()
