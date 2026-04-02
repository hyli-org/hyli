# Jolt Verifier

The Jolt verifier runs as an external worker process alongside the node. This document explains how to set it up.

## Prerequisites

Build the `hyli-jolt-verifier` binary from the `hyli-jolt-verifier-worker` crate. A release build is recommended for performance:

```sh
cargo build -p hyli-jolt-verifier-worker --release
```

This produces the `hyli-jolt-verifier` binary in `target/release/`.

## Running the node with Jolt enabled

Pass the `--jolt` flag when starting the node:

```sh
hyli --jolt
```

This enables the `jolt` backend in the verifier workers configuration, which handles `jolt-0.1` proofs.

## Binary resolution

The node locates the `hyli-jolt-verifier` binary via the `resolve_worker_command` function (`src/verifier_workers.rs`), which searches in the following order:

1. **Absolute or relative path** — if the configured command contains a path separator, it is used as-is.
2. **`PATH`** — the system `PATH` is searched for the binary name.
3. **Cargo target directory** (debug builds only) — falls back to `target/debug/` and `target/release/` within the Cargo target directory (resolved via `CARGO_TARGET_DIR` env var or `cargo metadata`).

For production deployments, the simplest approach is to install the binary into a directory on your `PATH`, or configure an absolute path in the node's configuration under `verifier_workers.backends.jolt.command`.
