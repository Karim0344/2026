from __future__ import annotations


def risk_multiplier(context_score: int, pattern_score: int) -> float:
    total = context_score + pattern_score
    if total <= -10:
        return 0.5
    if total <= -4:
        return 0.75
    return 1.0


def strategy_penalty(avg_r: float, samples: int) -> int:
    if samples < 20:
        return 0
    if avg_r < -0.2:
        return -10
    if avg_r < 0:
        return -5
    return 0
