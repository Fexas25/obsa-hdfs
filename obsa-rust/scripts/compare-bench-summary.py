#!/usr/bin/env python3
"""Summarize compare-bench.sh results: per workload and metric, the median of each
implementation over all runs, the Rust/Java ratio, and which side is better.

    python3 scripts/compare-bench-summary.py results.jsonl [--markdown]

Failed runs are counted and excluded. Medians over few runs are noisy: report the run
count with every number, and do not quote a ratio whose runs disagree in direction.
"""
import json
import statistics
import sys
from collections import defaultdict

# metric path -> True if higher is better
METRICS = {
    "write_mib_s": True, "read_mib_s": True,
    "write_file_ms.p50": False, "read_file_ms.p50": False,
    "reads_per_s": True, "read_us.p50": False, "read_us.p95": False, "read_us.p99": False,
    "create_per_s": True, "stat_per_s": True, "delete_per_s": True, "list_ms": False,
    "create_us.p99": False, "stat_us.p99": False, "delete_us.p99": False,
    "rename_ms": False,
    "wall_ms": False, "cpu_ms": False, "gc_ms": False, "peak_rss_mb": False,
}


def get(record, path):
    value = record
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value if isinstance(value, (int, float)) and value >= 0 else None


def main():
    path = sys.argv[1]
    markdown = "--markdown" in sys.argv
    runs = defaultdict(lambda: defaultdict(list))  # (workload, args) -> impl -> records
    failures = defaultdict(int)
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            key = (r.get("workload"), r.get("args", ""))
            if not r.get("ok"):
                failures[(key, r.get("impl"))] += 1
                continue
            runs[key][r["impl"]].append(r)
    rows = []
    for key in sorted(runs, key=str):
        java, rust = runs[key].get("java", []), runs[key].get("rust", [])
        for metric, higher in METRICS.items():
            jv = [v for v in (get(r, metric) for r in java) if v is not None]
            rv = [v for v in (get(r, metric) for r in rust) if v is not None]
            if not jv or not rv:
                continue
            jm, rm = statistics.median(jv), statistics.median(rv)
            ratio = rm / jm if jm else float("inf")
            better = "rust" if (ratio > 1) == higher and ratio != 1 else ("java" if ratio != 1 else "=")
            # Direction agrees across runs only if every Rust run beats every Java run or vice versa.
            consistent = (min(rv) > max(jv) or max(rv) < min(jv))
            rows.append((f"{key[0]} {key[1]}".strip(), metric, len(jv), jm, len(rv), rm, ratio, better,
                         "yes" if consistent else "no"))
    header = ("workload", "metric", "n_java", "java", "n_rust", "rust", "rust/java", "better", "consistent")
    if markdown:
        print("| " + " | ".join(header) + " |")
        print("|" + "---|" * len(header))
        for r in rows:
            print("| " + " | ".join(fmt(x) for x in r) + " |")
    else:
        for r in rows:
            print("\t".join(fmt(x) for x in r))
    for (key, impl), n in sorted(failures.items(), key=str):
        print(f"FAILED {impl} {key[0]} {key[1]}: {n} run(s)", file=sys.stderr)


def fmt(x):
    if isinstance(x, float):
        return f"{x:.3g}" if abs(x) < 1000 else f"{x:.0f}"
    return str(x)


if __name__ == "__main__":
    main()
