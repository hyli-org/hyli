> `main` is the development branch.
> When building applications or running examples, use the [latest release](https://github.com/hyli-org/hyli/releases) instead.

# Hyli

[![Telegram Chat][tg-badge]][tg-url]
[![Build Status][actions-badge]][actions-url]
[![Code Coverage][codecov-badge]][codecov-url]
[![Twitter][twitter-badge]][twitter-url]

_Hyli is the new proof-powered L1 to build the next generation of apps onchain._

This repository hosts the **work-in-progress Rust client** for the [Hyli](https://hyli.org) chain.

**Current Status**: 🚧 Work in Progress (WIP)

---

## 📎 Useful Links

- 🌐 [Hyli Website](https://www.hyli.org/)
- 📚 [Hyli Documentation](https://docs.hyli.org)

---

## 🚀 Getting Started

### With Cargo

#### Start a Single-Node Devnet

To launch a single-node devnet (consensus disabled) for building and debugging smart contracts:

```bash
cargo build
HYLE_RUN_INDEXER=false cargo run
```

Note: if you need sp1 verifier, enable the feature: `sp1`

```sh
cargo run -F sp1
```

#### Run with Indexer

To run the indexer, you can use the `--pg` node argument:

```sh
cargo run -- --pg
```

It will start a postgres server for you, and will close it (with all its data) whenever you stop the node.
This is useful during development.

If you want data persistence, you can run the PostgreSQL server:

```bash
# Start PostgreSQL with default configuration:
docker run -d --rm --name pg_hyli -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres
```

and then in the `hyli` root:

```sh
cargo run
```

### Configuration

You can configure Hyli using environment variables or a configuration file:

#### Using a Configuration File

Copy the default configuration file to the directory where the node will run:

```bash
cp ./src/utils/conf_defaults.toml config.toml
```

If a file named `config.toml` is present, it will be automatically loaded at node startup.

#### Using Environment Variables

Examples of configuration via environment variables:

```bash
HYLE_RUN_INDEXER=false
HYLE_CONSENSUS__SLOT_DURATION=100
```

---

## 🐳 Getting Started with Docker

### Build Locally

```bash
# Build the dependency image, this is a cache layer for faster iteration builds
docker build . -t hyle-dep -f Dockerfile.dependencies
# Build the node image
docker build . -t hyle
```

### Run Locally with Docker

```bash
docker run -v ./db:/hyle/data -e HYLE_RUN_INDEXER=false -p 4321:4321 -p 1234:1234 hyle
```

> 🛠️ **Note**: If you encounter permission issues with the `/hyle/data` volume, add the `--privileged` flag.

---

## 📊 Monitoring with Grafana and Prometheus

### Starting Services

To start the monitoring stack:

```bash
docker compose -f tools/docker-compose.yml up -d
```

### Access Grafana

Grafana is accessible at: [http://localhost:3000](http://localhost:3000)

### Stopping Services

To stop the monitoring stack:

```bash
docker compose -f tools/docker-compose.yml down
```

---

## 🛠️ Profiling and Debugging

### Profiling Build

Run the following command to enable the `profiling` profile, which is optimised but retains debug symbols:

```bash
cargo run --profile profiling
```

### CPU Profiling

- For advanced analysis, we recommend [Samply](https://github.com/mstange/samply).

### Memory Profiling

Hyli includes built-in support for the `dhat` crate, which uses the Valgrind DHAT viewer for memory profiling.  
To enable this feature, add the `dhat` feature flag. Use it selectively, as it has a runtime performance cost.

[actions-badge]: https://img.shields.io/github/actions/workflow/status/hyli-org/hyli/ci.yml?branch=main
[actions-url]: https://github.com/hyli-org/hyli/actions?query=workflow%3ATests+branch%3Amain
[codecov-badge]: https://codecov.io/gh/hyli-org/hyli/graph/badge.svg?token=S87GT99Q62
[codecov-url]: https://codecov.io/gh/hyli-org/hyli
[twitter-badge]: https://img.shields.io/twitter/follow/hyli_org
[twitter-url]: https://x.com/hyli_org
[tg-badge]: https://img.shields.io/endpoint?url=https%3A%2F%2Ftg.sumanjay.workers.dev%2Fhyli_org%2F&logo=telegram&label=chat&color=neon
[tg-url]: https://t.me/hyli_org
