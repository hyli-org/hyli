#!/bin/bash
set -e

# Script to copy Cargo cache from shared storage to local workspace
# Usage: copy-cache.sh <CACHE_NAME>
#
# Environment variables required:
#   SHARED_CACHE_DIR: Path to shared cache directory (e.g., /home/runner/.cache/node-cache/rust)
#   CARGO_HOME: Local path for CARGO_HOME
#   CARGO_TARGET_DIR: Local path for CARGO_TARGET_DIR
#
# Arguments:
#   CACHE_NAME: Name of the cache subdirectory (e.g., "hyli" or "hyli-my-crate")

CACHE_NAME="$1"

if [ -z "$CACHE_NAME" ]; then
  echo "Error: Missing required argument CACHE_NAME"
  echo "Usage: $0 <CACHE_NAME>"
  exit 1
fi

if [ -z "$SHARED_CACHE_DIR" ] || [ -z "$CARGO_HOME" ] || [ -z "$CARGO_TARGET_DIR" ]; then
  echo "Error: Required environment variables not set"
  echo "Required: SHARED_CACHE_DIR, CARGO_HOME, CARGO_TARGET_DIR"
  exit 1
fi

echo "Copying cache from shared storage..."
echo "  Shared cache: $SHARED_CACHE_DIR"
echo "  Local CARGO_HOME: $CARGO_HOME"
echo "  Local CARGO_TARGET_DIR: $CARGO_TARGET_DIR"
echo "  Cache name: $CACHE_NAME"

# Create local cache directories
mkdir -p "$CARGO_HOME"
mkdir -p "$CARGO_TARGET_DIR"

# Copy CARGO_HOME components if they exist
if [ -d "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/registry" ]; then
  echo "  Copying registry..."
  cp -r "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/registry" "$CARGO_HOME/"
fi

if [ -d "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/git" ]; then
  echo "  Copying git..."
  cp -r "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/git" "$CARGO_HOME/"
fi

if [ -d "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/bin" ]; then
  echo "  Copying bin..."
  cp -r "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/bin" "$CARGO_HOME/"
fi

if [ -f "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/.crates.toml" ]; then
  echo "  Copying .crates.toml..."
  cp "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/.crates.toml" "$CARGO_HOME/"
fi

if [ -f "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/.crates2.json" ]; then
  echo "  Copying .crates2.json..."
  cp "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/.crates2.json" "$CARGO_HOME/"
fi

# Copy target directory if it exists
if [ -d "$SHARED_CACHE_DIR/$CACHE_NAME/target" ]; then
  echo "  Copying target directory..."
  cp -r "$SHARED_CACHE_DIR/$CACHE_NAME/target" "$CARGO_TARGET_DIR/"
fi

echo "Cache copy completed!"
