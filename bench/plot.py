#!/usr/bin/env python3
"""Regenerate README figures and the results table from results/results.csv.

Reads only the CSV. If the CSV is absent this exits non-zero rather than
inventing a figure.
"""
from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def load(path: Path) -> list[dict]:
    if not path.exists():
        raise SystemExit(f"{path} not found. Run bench/run_bench.py first.")
    with path.open() as fh:
        return list(csv.DictReader(fh))


def summarise(rows: list[dict]) -> dict:
    agg = defaultdict(list)
    for r in rows:
        agg[(r["query_id"], r["cardinality"], r["arm"], r["cache"])].append(r)
    out = {}
    for key, vals in agg.items():
        ex = [float(v["execution_ms"]) for v in vals]
        pl = [float(v["planning_ms"]) for v in vals]
        out[key] = {
            "median_ms": statistics.median(ex),
            "min_ms": min(ex),
            "max_ms": max(ex),
            "stdev_ms": statistics.pstdev(ex) if len(ex) > 1 else 0.0,
            "median_planning_ms": statistics.median(pl),
            "rows": int(vals[0]["actual_rows"] or 0),
            "row_groups": int(vals[0]["row_groups_read"] or 0),
            "gsi_active": vals[0]["gsi_active"] == "True",
            "n": len(ex),
        }
    return out


def check_ab(s: dict) -> list[str]:
    """The CSV must show the index off in the off arm and on in the on arm.

    plot.py is the last thing to touch the data before it reaches the README,
    so it refuses to render a table it cannot vouch for.
    """
    problems = []
    for (qid, card, arm, cache), v in sorted(s.items()):
        if arm == "gsi_off" and v["gsi_active"]:
            problems.append(f"{qid}/{cache}: gsi_off arm reports the index active")
        if arm == "gsi_on" and not v["gsi_active"]:
            problems.append(f"{qid}/{cache}: gsi_on arm reports the index inactive")
    return problems


def markdown_table(s: dict) -> str:
    qids = sorted({k[0] for k in s})
    lines = [
        "| Query | Card. | Cache | No GSI (median ms) | GSI (median ms) | Speedup | "
        "Row groups read (no GSI -> GSI) | Rows returned | n |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for qid in qids:
        card = next(k[1] for k in s if k[0] == qid)
        for cache in ("cold", "warm"):
            off = s.get((qid, card, "gsi_off", cache))
            on = s.get((qid, card, "gsi_on", cache))
            if not off or not on:
                continue
            sp = off["median_ms"] / on["median_ms"] if on["median_ms"] else float("nan")
            # Only call something a regression when the two arms' observed
            # ranges do not overlap. A 0.04 ms gap between two cells that each
            # span 15 ms is not a regression, and labelling it as one is just
            # as dishonest as hiding it. The row-groups column below still
            # shows the index pruned nothing, which is the real finding.
            overlap = (off["min_ms"] <= on["max_ms"] and on["min_ms"] <= off["max_ms"])
            if overlap:
                marker = " (within run-to-run spread)"
            elif sp < 1.0:
                marker = " **regression**"
            else:
                marker = ""
            lines.append(
                f"| `{qid}` | {card} | {cache} | {off['median_ms']:.1f} | "
                f"{on['median_ms']:.1f} | {sp:.2f}x{marker} | "
                f"{off['row_groups']} -> {on['row_groups']} | "
                f"{on['rows']} | {on['n']} |")
    return "\n".join(lines)


def plot(s: dict, out: Path) -> None:
    qids = sorted({k[0] for k in s})
    cards = {k[0]: k[1] for k in s}
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for ax, cache in zip(axes, ("cold", "warm")):
        labels, off_v, on_v = [], [], []
        for qid in qids:
            off = s.get((qid, cards[qid], "gsi_off", cache))
            on = s.get((qid, cards[qid], "gsi_on", cache))
            if not off or not on:
                continue
            labels.append(f"{qid}\n({cards[qid]} card.)")
            off_v.append(off["median_ms"])
            on_v.append(on["median_ms"])
        x = range(len(labels))
        ax.bar([i - 0.2 for i in x], off_v, width=0.4, label="no GSI")
        ax.bar([i + 0.2 for i in x], on_v, width=0.4, label="GSI")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_ylabel("median execution time (ms)")
        ax.set_title(f"{cache} cache")
        ax.legend()
    fig.suptitle("parquet-gsi: selective lookups with and without the global secondary index")
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="results/results.csv")
    ap.add_argument("--fig", default="results/latency.png")
    ap.add_argument("--table", default="results/results_table.md")
    args = ap.parse_args()
    s = summarise(load(Path(args.csv)))
    problems = check_ab(s)
    if problems:
        raise SystemExit("results.csv contradicts the A/B design:\n  "
                         + "\n  ".join(problems)
                         + "\nRefusing to produce a table from it.")
    plot(s, Path(args.fig))
    md = markdown_table(s)
    Path(args.table).write_text(md + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
