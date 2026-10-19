"""Compares out/celery.json and out/tributary.json; writes out/report.md and charts."""

import json
import os
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out")


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round(p / 100 * (len(xs) - 1)))))
    return xs[k]


def summarize(run):
    calls = run["calls"]
    ok = [c for c in calls if c["status"] == 200]
    billed = Counter((c["job"], c["step"]) for c in ok)
    tokens = {(c["job"], c["step"]): c["tokens"] for c in ok}
    dup = sum(n - 1 for n in billed.values() if n > 1)
    wasted = sum((n - 1) * tokens[k] for k, n in billed.items() if n > 1)
    inter = [j for j in run["jobs"] if j["kind"] == "interactive"]
    lat = [run["done"][j["id"]] - run["enqueued"][j["id"]] for j in inter if j["id"] in run["done"]]
    last = max(run["done"].values()) if run["done"] else run["end"]
    return {
        "system": run["system"],
        "429s": sum(1 for c in calls if c["status"] == 429),
        "duplicate paid calls": dup,
        "wasted tokens": wasted,
        "billed tokens": sum(c["tokens"] for c in ok),
        "interactive p50 (s)": pct(lat, 50),
        "interactive p95 (s)": pct(lat, 95),
        "completed jobs": f'{len(run["done"])}/{len(run["jobs"])}',
        "total time (s)": last - run["start"],
        "_lat": lat,
        "_inter_done": sorted((run["enqueued"][j["id"]] - run["start"], run["done"][j["id"]] - run["enqueued"][j["id"]])
                              for j in inter if j["id"] in run["done"]),
    }


def fmt(v):
    if isinstance(v, float):
        return f"{v:.1f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def main():
    runs = []
    for name in ("celery", "tributary"):
        path = os.path.join(OUT, f"{name}.json")
        if os.path.exists(path):
            with open(path) as f:
                runs.append(summarize(json.load(f)))
    if not runs:
        raise SystemExit("no runs in demo/out")
    rows = [k for k in runs[0] if not k.startswith("_") and k != "system"]
    lines = ["| | " + " | ".join(r["system"] for r in runs) + " |", "|---" * (len(runs) + 1) + "|"]
    for k in rows:
        lines.append(f"| {k} | " + " | ".join(fmt(r[k]) for r in runs) + " |")
    table = "\n".join(lines)
    with open(os.path.join(OUT, "report.md"), "w") as f:
        f.write("# Demo report\n\n" + table + "\n")
    print(table)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed: skipping charts")
        return
    colors = {"Celery 5 + Redis": "#d9534f", "Tributary v0": "#2a7ab9"}
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for r in runs:
        xs = [x for x, _ in r["_inter_done"]]
        ys = [y for _, y in r["_inter_done"]]
        ax.plot(xs, ys, marker="o", markersize=3, linewidth=1, label=r["system"], color=colors.get(r["system"]))
    ax.set_yscale("log")
    ax.set_xlabel("seconds since start (when the interactive request arrived)")
    ax.set_ylabel("interactive request latency (s, log)")
    ax.set_title("Interactive users wait behind 200 agent jobs")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "latency.png"), dpi=150)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    labels = ["429 responses", "duplicate paid calls"]
    width = 0.38
    for i, r in enumerate(runs):
        vals = [r["429s"], r["duplicate paid calls"]]
        ax.bar([x + i * width for x in range(len(labels))], vals, width, label=r["system"], color=colors.get(r["system"]))
    ax.set_xticks([x + width / 2 for x in range(len(labels))])
    ax.set_xticklabels(labels)
    ax.set_title("Same workload, same provider limits")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "429s.png"), dpi=150)
    print("charts: demo/out/latency.png demo/out/429s.png")


if __name__ == "__main__":
    main()
