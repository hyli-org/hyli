# Hyli

<div align="center">
  <a href="https://hyli.org/">
    <img src="https://github.com/hyli-org/hyli-assets/blob/main/Logos/Logo/HYLI_WORDMARK_ORANGE.png?raw=true" width="320" alt="Hyli">
  </a>

  _**Hyli** is the new proof-powered L1 to build the next generation of apps onchain._

  This repository hosts the **Rust client** for the [Hyli](https://hyli.org) chain.

  [![Build Status][actions-badge]][actions-url]
  [![Code Coverage][codecov-badge]][codecov-url]
  
  [![Telegram Chat][tg-badge]][tg-url]
  [![Twitter][twitter-badge]][twitter-url]
</div>

> [!IMPORTANT]
> `main` is the development branch.
> When building applications or running examples, use the [latest release](https://github.com/hyli-org/hyli/releases) instead.

## :tangerine: What is Hyli?

With Hyli, developers can build fast, composable, and verifiable apps without dealing with the usual pains of blockchain.

On Hyli, instead of executing transactions onchain, you run your app logic anywhere off-chain, in Rust, Noir, or even multiple languages at once. You only need to send a proof for onchain settlement.

<div align="center">
    <p>
        📚 <a href="https://hyli.org/">Website</a> | <a href="https://docs.hyli.org">Docs</a> | <a href="https://docs.hyli.org/guide/">Hyli Guide</a> | <a href="https://docs.hyli.org/quickstart/">Quickstart</a> | <a href="https://docs.hyli.org/tooling/">Tooling</a>
    </p>
    <p>
        Follow <a href="https://twitter.com/hyli_org">on X</a> | <a href="https://www.linkedin.com/company/hyli-org">LinkedIn</a> | <a href="https://t.me/hyli_org">Telegram</a> | <a href="https://www.youtube.com/@hyli-org">YouTube</a> | <a href="https://blog.hyli.org/">Blog &amp; Newsletter</a>
    </p>
</div>

## 🚀 Getting Started

### Recommended: Quickstart with Cargo

Clone this repository.

Run:
```sh
git checkout v0.13.1
rm -rf data_node && RISC0_DEV_MODE=true SP1_PROVER=mock cargo run -- --pg
```

You can now use the [Hyli explorer](https://explorer.hyli.org/). Select `localhost` in the upper-right corner.

Use [our quickstart guide](https://docs.hyli.org/quickstart/run/) to start building!

#### Options

To launch a local node for building and debugging smart contracts, without indexer:

```bash
cargo build
HYLE_RUN_INDEXER=false cargo run
```

If you need sp1 verifier, enable the feature: `sp1`

```sh
cargo run -F sp1
```

To run the indexer, you can use the `--pg` node argument:

```sh
cargo run -- --pg
```

It will start a postgres server for you, and will close it (with all its data) whenever you stop the node.

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

You can configure Hyli using environment variables or a configuration file.

Read the [configuration files and environment variables reference in our docs](https://docs.hyli.org/reference/local-node/#configuration).

## 🐳 Alternative: Getting Started with Docker
<details>
<summary>Click to open Docker instructions.</summary>

### Build Locally

```bash
# Build the dependency image, this is a cache layer for faster iteration builds
docker build -f Dockerfile.dependencies -t hyle-dep .
# Build the node image
docker build -t hyle .
```

### Build Locally on MacOS (Apple Silicon)

##### 🧰 Requirements for buildx users
If you are building for an architecture different than your host machine (e.g., building arm64 on an amd64 host), make sure to set up your environment accordingly:

```bash
# 1. Enable Docker BuildKit (recommended)
export DOCKER_BUILDKIT=1

# 2. Create and use a buildx builder (only needed once)
docker buildx create --use --name hyle-builder
docker buildx inspect --bootstrap

# 3. Install QEMU for cross-platform builds
docker run --privileged --rm tonistiigi/binfmt --install all
```

```bash
# Build the dependency image, this is a cache layer for faster iteration builds
docker buildx build --platform linux/arm64 -f Dockerfile.dependencies -t hyle-dep .
# Build the node image
docker buildx build --platform linux/arm64 -t hyle .
```

### Run Locally with Docker

```bash
docker run -v ./db:/hyle/data -e HYLE_RUN_INDEXER=false -p 4321:4321 -p 1234:1234 hyle
```

> 🛠️ **Note**: If you build on MacOS (Apple Silicon), add `--platform linux/arm64` to run script.
> 🛠️ **Note**: If you encounter permission issues with the `/hyle/data` volume, add the `--privileged` flag.

</details>

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
