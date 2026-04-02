import json
import os
from collections import defaultdict
from typing import Any


def _safe_avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def analyze_trade_memory(path: str = "trade_memory.jsonl") -> dict[str, Any]:
    if not os.path.exists(path):
        return {
            "total_closed": 0,
            "by_strategy": {},
            "by_strategy_regime": {},
        }

    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue

    closes = [r for r in rows if r.get("event") == "close"]
    by_strategy: dict[str, list[float]] = defaultdict(list)
    by_strategy_regime: dict[str, list[float]] = defaultdict(list)

    for row in closes:
        rr = float(row.get("result_r", 0.0))
        signal_reason = str(row.get("signal_reason", "unknown"))
        features = row.get("features", {}) or {}
        regime = str(features.get("regime", "unknown"))

        by_strategy[signal_reason].append(rr)
        by_strategy_regime[f"{signal_reason}|{regime}"].append(rr)

    strategy_stats = {}
    for key, vals in by_strategy.items():
        wins = sum(1 for v in vals if v > 0)
        strategy_stats[key] = {
            "count": len(vals),
            "avg_r": round(_safe_avg(vals), 3),
            "winrate": round((wins / len(vals) * 100.0), 2) if vals else 0.0,
        }

    regime_stats = {}
    for key, vals in by_strategy_regime.items():
        wins = sum(1 for v in vals if v > 0)
        regime_stats[key] = {
            "count": len(vals),
            "avg_r": round(_safe_avg(vals), 3),
            "winrate": round((wins / len(vals) * 100.0), 2) if vals else 0.0,
        }

    return {
        "total_closed": len(closes),
        "by_strategy": strategy_stats,
        "by_strategy_regime": regime_stats,
    }


def selector_adjustment(
    signal_reason: str,
    regime: str,
    path: str = "trade_memory.jsonl",
    min_samples: int = 10,
) -> dict[str, Any]:
    analysis = analyze_trade_memory(path)
    strategy_key = signal_reason
    regime_key = f"{signal_reason}|{regime}"

    s = analysis["by_strategy"].get(strategy_key, {})
    sr = analysis["by_strategy_regime"].get(regime_key, {})

    base_bonus = 0
    block = False
    reason = "no_history"

    if sr and sr.get("count", 0) >= min_samples:
        avg_r = float(sr.get("avg_r", 0.0))
        winrate = float(sr.get("winrate", 0.0))

        if avg_r < -0.2 or winrate < 35:
            block = True
            reason = "strategy_regime_bad"
        elif avg_r > 0.75 and winrate >= 55:
            base_bonus = 15
            reason = "strategy_regime_strong"
        elif avg_r > 0.25 and winrate >= 45:
            base_bonus = 8
            reason = "strategy_regime_ok"
        else:
            reason = "strategy_regime_neutral"

        return {
            "block": block,
            "bonus": base_bonus,
            "reason": reason,
            "samples": sr.get("count", 0),
            "avg_r": sr.get("avg_r", 0.0),
            "winrate": sr.get("winrate", 0.0),
        }

    if s and s.get("count", 0) >= min_samples:
        avg_r = float(s.get("avg_r", 0.0))
        winrate = float(s.get("winrate", 0.0))

        if avg_r < -0.3 or winrate < 35:
            block = True
            reason = "strategy_bad"
        elif avg_r > 0.75 and winrate >= 55:
            base_bonus = 10
            reason = "strategy_strong"
        elif avg_r > 0.25 and winrate >= 45:
            base_bonus = 5
            reason = "strategy_ok"
        else:
            reason = "strategy_neutral"

        return {
            "block": block,
            "bonus": base_bonus,
            "reason": reason,
            "samples": s.get("count", 0),
            "avg_r": s.get("avg_r", 0.0),
            "winrate": s.get("winrate", 0.0),
        }

    return {
        "block": False,
        "bonus": 0,
        "reason": reason,
        "samples": 0,
        "avg_r": 0.0,
        "winrate": 0.0,
    }
