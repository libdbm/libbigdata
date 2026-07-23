# Single image with all three engines for an apples-to-apples Parquet benchmark.
#   docker build -f benchmark/Dockerfile -t libbigdata-bench .
#   docker run --rm -v "$PWD/benchmark/results:/app/benchmark/results" libbigdata-bench
FROM dart:stable

ARG DUCKDB_VERSION=1.1.3

# Toolchain: JDK + Maven (parquet-mr), DuckDB CLI, python3 + bc (report/timing).
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-21-jdk-headless maven python3 bc unzip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# DuckDB CLI (arch-matched).
RUN arch="$(dpkg --print-architecture)"; \
    case "$arch" in arm64) d=aarch64;; amd64) d=amd64;; *) echo "unsupported $arch"; exit 1;; esac; \
    curl -fsSL -o /tmp/duckdb.zip \
      "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${d}.zip" \
    && unzip /tmp/duckdb.zip -d /usr/local/bin && rm /tmp/duckdb.zip && duckdb --version

WORKDIR /app

# 1) Maven deps in their own layer (cached unless pom.xml changes).
COPY benchmark/java/pom.xml benchmark/java/pom.xml
RUN cd benchmark/java && mvn -q -B dependency:go-offline

# 2) Dart deps in their own layer (cached unless pubspec changes).
COPY pubspec.yaml pubspec.lock ./
RUN dart pub get

# 3) Sources, then build artifacts.
COPY . .
RUN cd benchmark/java && mvn -q -B package -DskipTests
RUN dart pub get && mkdir -p benchmark/bin && dart compile exe benchmark/dart/bench.dart -o benchmark/bin/bench

ENV DART_BIN=/app/benchmark/bin/bench
ENV JAR=/app/benchmark/java/target/parquet-bench.jar
ENV JAVA=java

ENTRYPOINT ["bash", "benchmark/run.sh"]
