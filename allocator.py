# allocator.py
from typing import List, Tuple, Dict

def compute_cost(split, venues, order_size, λo, λu, θ) -> float:
    executed = cash_spent = 0.0
    for alloc, v in zip(split, venues):
        exe = min(alloc, v["ask_sz"])
        executed   += exe
        cash_spent += exe * (v["ask_px"] + v["fee"])
        maker_rebate = max(alloc - exe, 0) * v["rebate"]
        cash_spent  -= maker_rebate
    underfill = max(order_size - executed, 0)
    overfill  = max(executed - order_size, 0)
    return (
        cash_spent
        + θ * (underfill + overfill)
        + λu * underfill
        + λo * overfill
    )


def allocate(
    order_size: int,
    venues: List[Dict[str, float]],
    λ_over: float,
    λ_under: float,
    θ_queue: float,
    step: int = 100,
) -> Tuple[List[int], float]:
    """Return best split list[int] & expected cost."""
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
