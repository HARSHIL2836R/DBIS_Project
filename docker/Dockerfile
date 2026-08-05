# parquet-gsi reproducible benchmark image
# PostgreSQL 16 + Apache Arrow/Parquet + the modified parquet_fdw carrying the
# GSI planner/executor integration, plus the Python toolchain used by the
# data generator and the one-shot lake indexer.
FROM postgres:16-bookworm

ENV LANG=C.UTF-8
ENV PG_MAJOR=16
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates lsb-release wget gcc g++ make cmake pkg-config git \
        postgresql-server-dev-16 \
        python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Apache Arrow + Parquet C++ from the official apt source (pinned by distro
# codename, which is fixed by the base image tag above).
#
# ARROW_VERSION is pinned deliberately. The published numbers were produced
# against 24.0.0-1, which is the version parquet_impl.cpp is known to compile
# against under -std=c++20. Leaving it unpinned means a build six months from
# now silently picks up a newer Arrow whose API may have moved, and the
# stranger reproducing this gets a compile error instead of a benchmark.
# To move the pin: bump it, rebuild, and re-run ./run_bench.sh. Do not bump it
# and keep the old numbers.
ENV ARROW_VERSION=24.0.0-1
RUN wget -q "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb" \
        -O /tmp/arrow.deb \
    && apt-get update \
    && apt-get install -y -V /tmp/arrow.deb \
    && apt-get update \
    && apt-get install -y -V \
        "libarrow-dev=${ARROW_VERSION}" \
        "libparquet-dev=${ARROW_VERSION}" \
    && rm -f /tmp/arrow.deb && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages \
        "numpy==2.1.3" "pandas==2.2.3" "pyarrow==19.0.1" "psycopg2-binary==2.9.10" \
        "matplotlib==3.9.2"

# Build and install the modified parquet_fdw extension.
COPY modified_parquet_fdw /build/modified_parquet_fdw
WORKDIR /build/modified_parquet_fdw
RUN make PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config -j"$(nproc)" \
    && make PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config install \
    && make clean

# Build the standalone Arrow/Parquet extraction engine (not on the query path;
# built here so CI proves it still compiles).
COPY parquet_extraction_engine /build/parquet_extraction_engine
WORKDIR /build/parquet_extraction_engine
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j"$(nproc)" \
    && install -m 0755 build/extractor /usr/local/bin/parquet-extractor

WORKDIR /work
