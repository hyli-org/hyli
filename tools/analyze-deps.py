#!/usr/bin/env python3
"""Analyze cargo dependency tree to find the heaviest dependencies.

Computes the *exclusive* dependency count for each direct dependency:
how many crates would be removed from the build if you dropped that dep.
This is the actionable metric — not the total transitive count.

Usage: python3 tools/analyze-deps.py [--top N] [--no-workspace] [--json]

Run from the project root (where Cargo.toml lives).
Requires: cargo
"""

import argparse
import json
import re
import subprocess
import sys
from collections import defaultdict


def run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"Error running: {' '.join(cmd)}", file=sys.stderr)
        print(r.stderr, file=sys.stderr)
        sys.exit(1)
    return r.stdout


def get_workspace_members():
    meta = json.loads(run(["cargo", "metadata", "--no-deps", "--format-version=1"]))
    return {p["name"] for p in meta["packages"]}


def parse_tree_output(output):
    """Extract unique crate names from `cargo tree --prefix none` output."""
    crates = set()
    for line in output.strip().split("\n")[1:]:
        line = line.strip()
        if line.startswith("["):
            continue
        m = re.match(r"^(\S+) v\S+", line)
        if m:
            crates.add(m.group(1))
    return crates


def get_all_deps_from_tree():
    """Parse the full cargo tree once and return (direct_deps, dep_sets).

    direct_deps: list of (name, full_spec, is_first_occurrence)
    dep_sets: dict name -> set of transitive dep names

    This parses the single `cargo tree` output by tracking indentation depth,
    which avoids running cargo tree N times (one per dep).
    """
    output = run(["cargo", "tree", "--prefix", "depth", "-e", "normal"])
    # --prefix depth outputs lines like "0hyli v0.15.0", "1anyhow v1.0.102", "2foo v0.1.0"

    direct_deps = []  # (name, full_spec)
    seen_direct = set()

    # For each direct dep, collect all transitive deps by tracking the subtree
    dep_sets = {}
    current_direct = None
    # Stack to track which direct dep "owns" deeper entries
    # We only care about depth >= 1 entries that fall under a depth-1 entry

    for line in output.strip().split("\n"):
        m = re.match(r"^(\d+)(\S+) (v\S+)", line)
        if not m:
            continue
        depth = int(m.group(1))
        name = m.group(2)
        ver = m.group(3)

        if depth == 0:
            continue  # root crate

        if depth == 1:
            current_direct = name
            if name not in seen_direct:
                seen_direct.add(name)
                direct_deps.append((name, f"{name} {ver}"))
                dep_sets[name] = set()
        elif current_direct and current_direct in dep_sets:
            dep_sets[current_direct].add(name)

    return direct_deps, dep_sets


def count_duplicated_versions():
    """Find crates that appear with multiple versions."""
    output = run(["cargo", "tree", "--depth", "0", "--prefix", "none", "-e", "normal", "--duplicates"])
    versions = defaultdict(set)
    for line in output.strip().split("\n"):
        m = re.match(r"^(\S+) (v\S+)", line.strip())
        if m:
            versions[m.group(1)].add(m.group(2))
    return {k: sorted(v) for k, v in versions.items() if len(v) > 1}


def main():
    parser = argparse.ArgumentParser(description="Analyze cargo dependency weight")
    parser.add_argument("--top", type=int, default=20, help="Number of deps to show (default: 20)")
    parser.add_argument("--no-workspace", action="store_true", help="Hide workspace crates")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    ws_names = get_workspace_members()

    print("Parsing dependency tree...", file=sys.stderr)
    direct_deps, dep_sets = get_all_deps_from_tree()
    print(f"Found {len(direct_deps)} direct dependencies.", file=sys.stderr)

    # Count how many direct deps pull in each transitive dep
    # A dep is "exclusive" to crate X if no other direct dep also needs it
    dep_users = defaultdict(set)  # transitive_dep -> set of direct deps that need it
    for direct_name, trans_deps in dep_sets.items():
        for t in trans_deps:
            dep_users[t].add(direct_name)

    # For each direct dep, count exclusive deps (only pulled by this one)
    results = []
    all_transitive = set()
    for name, full in direct_deps:
        trans = dep_sets.get(name, set())
        all_transitive |= trans
        exclusive = {t for t in trans if len(dep_users[t]) == 1}
        ext_trans = {t for t in trans if t not in ws_names}
        ext_exclusive = {t for t in exclusive if t not in ws_names}
        results.append({
            "name": name,
            "full": full,
            "is_workspace": name in ws_names,
            "total_deps": len(trans),
            "external_deps": len(ext_trans),
            "exclusive_deps": len(exclusive),
            "exclusive_external": len(ext_exclusive),
            "exclusive_list": sorted(ext_exclusive),
        })

    external = [r for r in results if not r["is_workspace"]]
    workspace = [r for r in results if r["is_workspace"]]

    external.sort(key=lambda x: -x["exclusive_external"])
    workspace.sort(key=lambda x: -x["exclusive_external"])

    duplicates = count_duplicated_versions()

    if args.json:
        print(json.dumps({
            "external": external[:args.top],
            "workspace": [] if args.no_workspace else workspace[:args.top],
            "duplicated_versions": duplicates,
            "summary": {
                "direct_deps": len(direct_deps),
                "total_unique_transitive": len(all_transitive),
                "external_count": len(external),
                "workspace_count": len(workspace),
                "duplicated_crates": len(duplicates),
            },
        }, indent=2))
        return

    # --- Pretty print ---

    print()
    print("=" * 72)
    print("  CARGO DEPENDENCY ANALYSIS")
    print(f"  Direct deps: {len(direct_deps)}  |  "
          f"Unique transitive: {len(all_transitive)}  |  "
          f"Duplicated crates: {len(duplicates)}")
    print("=" * 72)

    print()
    print(f"  EXTERNAL DEPENDENCIES (top {args.top})")
    print(f"  {'Crate':<30} {'Total':>6} {'Exclusive':>10}  Exclusive deps")
    print("  " + "-" * 68)
    for e in external[:args.top]:
        bar = "█" * e["exclusive_external"]
        extras = ""
        if e["exclusive_list"]:
            shown = e["exclusive_list"][:5]
            extras = ", ".join(shown)
            if len(e["exclusive_list"]) > 5:
                extras += f", +{len(e['exclusive_list']) - 5} more"
        print(f"  {e['full']:<30} {e['external_deps']:>6} {e['exclusive_external']:>10}  {bar}")
        if extras:
            print(f"  {'':>48}  └ {extras}")

    if not args.no_workspace:
        print()
        print(f"  WORKSPACE CRATES (top {args.top})")
        print(f"  {'Crate':<30} {'Total':>6} {'Exclusive':>10}")
        print("  " + "-" * 48)
        for e in workspace[:args.top]:
            bar = "█" * e["exclusive_external"]
            print(f"  {e['full']:<30} {e['total_deps']:>6} {e['exclusive_external']:>10}  {bar}")

    if duplicates:
        print()
        print(f"  DUPLICATED CRATES ({len(duplicates)} crates with multiple versions)")
        print("  " + "-" * 52)
        for name, versions in sorted(duplicates.items()):
            print(f"  {name:<30} {', '.join(versions)}")

    print()


if __name__ == "__main__":
    main()
