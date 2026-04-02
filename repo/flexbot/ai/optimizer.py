import json
import os
from collections import defaultdict


def analyze_memory(path: str = "trade_memory.jsonl") -> dict:
    if not os.path.exists(path):
        return {"total": 0, "suggestions": []}

    rows = []
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
    if not closes:
        return {"total": 0, "suggestions": []}

    total = len(closes)
    by_reason = defaultdict(list)
    by_regime = defaultdict(list)
    by_session = defaultdict(list)

    for row in closes:
        rr = float(row.get("result_r", 0.0))
        features = row.get("features", {}) or {}
        by_reason[row.get("signal_reason", "unknown")].append(rr)
        by_regime[features.get("regime", "unknown")].append(rr)
        by_session[features.get("session", "unknown")].append(rr)

    suggestions = []

    for key, vals in by_reason.items():
        avg_r = sum(vals) / len(vals)
        if len(vals) >= 10 and avg_r < 0:
            suggestions.append(f"Disable or penalize setup: {key} (avg_r={avg_r:.2f})")

    for key, vals in by_regime.items():
        avg_r = sum(vals) / len(vals)
        if len(vals) >= 10 and avg_r < 0:
            suggestions.append(f"Avoid regime: {key} (avg_r={avg_r:.2f})")

    for key, vals in by_session.items():
        avg_r = sum(vals) / len(vals)
        if len(vals) >= 10 and avg_r < 0:
            suggestions.append(f"Avoid session: {key} (avg_r={avg_r:.2f})")

    return {
        "total": total,
        "suggestions": suggestions[:10],
    }
