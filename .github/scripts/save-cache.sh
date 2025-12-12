#!/bin/bash
set -e

# Script to save Cargo cache from local workspace to shared storage
# Usage: save-cache.sh <CACHE_NAME>
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

echo "Saving cache to shared storage..."
echo "  Shared cache: $SHARED_CACHE_DIR"
echo "  Local CARGO_HOME: $CARGO_HOME"
echo "  Local CARGO_TARGET_DIR: $CARGO_TARGET_DIR"
echo "  Cache name: $CACHE_NAME"

# Create shared cache directories
mkdir -p "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home"

set +e

# Save CARGO_HOME components
if [ -d "$CARGO_HOME/registry" ]; then
  echo "  Saving registry..."
  cp -a "$CARGO_HOME/registry" "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/"
fi

if [ -d "$CARGO_HOME/git" ]; then
  echo "  Saving git..."
  cp -a "$CARGO_HOME/git" "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/"
fi

if [ -d "$CARGO_HOME/bin" ]; then
  echo "  Saving bin..."
  cp -a "$CARGO_HOME/bin" "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/"
fi

if [ -f "$CARGO_HOME/.crates.toml" ]; then
  echo "  Saving .crates.toml..."
  cp "$CARGO_HOME/.crates.toml" "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/"
fi

if [ -f "$CARGO_HOME/.crates2.json" ]; then
  echo "  Saving .crates2.json..."
  cp "$CARGO_HOME/.crates2.json" "$SHARED_CACHE_DIR/$CACHE_NAME/cargo-home/"
fi

# Save target directory
if [ -d "$CARGO_TARGET_DIR" ]; then
  echo "  Saving target directory..."
  cp -a "$CARGO_TARGET_DIR" "$SHARED_CACHE_DIR/$CACHE_NAME/"
fi

echo "Cache save completed!"
