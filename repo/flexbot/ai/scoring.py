from typing import Any


def confidence_score(features: dict[str, Any], is_long: bool, max_spread: int) -> int:
    score = 0

    if features.get("trend_ok"):
        score += 20

    if features.get("htf_ok"):
        score += 20

    if features.get("pullback"):
        score += 15

    if features.get("momentum"):
        score += 15

    if features.get("breakout"):
        score += 15

    spread = int(features.get("spread_points", 999))
    if spread < max_spread:
        score += 5
    else:
        score -= 10

    return max(0, min(100, score))
