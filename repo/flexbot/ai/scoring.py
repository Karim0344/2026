from typing import Any


def confidence_score(features: dict[str, Any], is_long: bool, max_spread_points: int) -> int:
    score = 0

    if is_long:
        if features.get("trend_ok_long"):
            score += 20
        if features.get("htf_ok_long"):
            score += 20
        if features.get("pullback_ok_long"):
            score += 15
        if features.get("bullish_close"):
            score += 15
        if features.get("breakout_ok_long"):
            score += 15
    else:
        if features.get("trend_ok_short"):
            score += 20
        if features.get("htf_ok_short"):
            score += 20
        if features.get("pullback_ok_short"):
            score += 15
        if features.get("bearish_close"):
            score += 15
        if features.get("breakout_ok_short"):
            score += 15

    rsi = float(features.get("rsi", 0.0) or 0.0)
    if 40.0 < rsi < 60.0:
        score += 10
    elif 30.0 < rsi < 70.0:
        score += 5

    spread_points = int(features.get("spread_points", max_spread_points + 1))
    if spread_points <= max_spread_points:
        score += 5
    else:
        score -= 10

    return max(0, min(100, score))
