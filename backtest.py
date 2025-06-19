#!/usr/bin/env python3
import argparse, json, itertools, sys
from collections import defaultdict
from kafka import KafkaConsumer

# Inlined from allocator.py
def compute_cost(split, venues, order_size, λo, λu, θ) -> float:
    executed = cash_spent = 0.0
    for alloc, v in zip(split, venues):
        exe = min(alloc, v["ask_sz"])
        executed += exe
        cash_spent += exe * (v["ask_px"] + v["fee"])
        maker_rebate = max(alloc - exe, 0) * v["rebate"]
        cash_spent -= maker_rebate
    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    return (
        cash_spent
        + θ * (underfill + overfill)
        + λu * underfill
        + λo * overfill
    )

def allocate(order_size, venues, λ_over, λ_under, θ_queue, step=100):
    splits = [[]]
    for v in venues:
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, v["ask_sz"])
            for q in range(0, max_v + 1, step):
                new_splits.append(alloc + [q])
        splits = new_splits

    best_cost, best_split = float("inf"), []
    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size, λ_over, λ_under, θ_queue)
        if cost < best_cost:
            best_cost, best_split = cost, alloc
    return best_split, best_cost

LAM_OVER  = [0.2, 0.4, 0.6]
LAM_UNDER = [0.2, 0.4, 0.6]
THETA_Q   = [0.1, 0.3, 0.5]
ORDER     = 5_000

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="host.docker.internal:9092")
    p.add_argument("--topic", default="mock_l1_stream")
    p.add_argument("--timeout_ms", type=int, default=30_000)
    return p.parse_args()

def load_snapshots(topic, bootstrap, timeout):
    c = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        value_deserializer=lambda m: json.loads(m.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout,
    )
    by_ts = defaultdict(list)
    for m in c:
        by_ts[m.value["ts_event"]].append(m.value)
    if not by_ts:
        raise RuntimeError("no snapshots consumed")
    return by_ts

def run_execution(by_ts, split_fn):
    remain, cash = ORDER, 0.0
    for ts in sorted(by_ts):
        vs = [
            {"ask_px": v["ask_px_00"], "ask_sz": v["ask_sz_00"],
             "fee": 0.0002, "rebate": 0.0001}
            for v in by_ts[ts]
        ]
        if remain <= 0:
            break
        for qty, v in zip(split_fn(remain, vs), vs):
            exe = min(qty, v["ask_sz"])
            remain -= exe
            cash += exe * v["ask_px"]
    return {"total_cash": cash, "avg_fill_px": cash / ORDER}

def main():
    a = parse_args()
    s = load_snapshots(a.topic, a.bootstrap, a.timeout_ms)

    baselines = {
        "best_ask": run_execution(
            s, lambda r, vs: [r if v is min(vs, key=lambda x: x["ask_px"]) else 0 for v in vs]),
        "twap": run_execution(s, lambda r, vs: [r // len(vs)] * len(vs)),
        "vwap": run_execution(
            s, lambda r, vs: [int(r * v["ask_sz"] / sum(x["ask_sz"] for x in vs)) for v in vs]),
    }

    best_cost = float("inf")
    best_parm, best_res = None, None
    for lo, lu, th in itertools.product(LAM_OVER, LAM_UNDER, THETA_Q):
        res = run_execution(
            s, lambda r, vs, lo=lo, lu=lu, th=th: allocate(r, vs, lo, lu, th)[0])
        if res["total_cash"] < best_cost:
            best_cost, best_parm, best_res = res["total_cash"], {
                "lambda_over": lo, "lambda_under": lu, "theta_queue": th}, res

    savings = {
        k: round((b["total_cash"] - best_res["total_cash"]) / b["total_cash"] * 10_000, 1)
        for k, b in baselines.items()
    }

    out = {
        "best_parameters": best_parm,
        "optimized": best_res,
        "baselines": baselines,
        "savings_vs_baselines_bps": savings,
    }

    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    json.dump(out, sys.stdout, indent=2)

if __name__ == "__main__":
    main()
