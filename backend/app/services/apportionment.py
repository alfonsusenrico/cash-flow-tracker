"""Exact proportional allocation of integer amounts."""

from typing import Any


def apportion(total: int, weights: dict[Any, int]) -> dict[Any, int]:
    """Split `total` over keys in proportion to their weights, exactly, by largest remainder.

    With total <= sum(weights), no key receives more than its weight. Ties go to the earlier key.
    """
    weight_sum = sum(weights.values())
    if total <= 0 or weight_sum <= 0:
        return {}
    shares = {key: total * weight // weight_sum for key, weight in weights.items()}
    remainders = sorted(weights, key=lambda key: (-(total * weights[key] % weight_sum), key))
    for key in remainders[: total - sum(shares.values())]:
        shares[key] += 1
    return {key: share for key, share in shares.items() if share}
