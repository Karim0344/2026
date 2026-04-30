from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
from flexbot.ai.storage import read_table, resolve_existing_path
from flexbot.ai.session_utils import normalize_session_name


class PatternScorer:
    def __init__(self, store_learning_path: str, weight: float = 1.0):
        self.path = Path(store_learning_path) / "pattern_edge_table.parquet"
        self.weight = float(weight)
        self._cache: pd.DataFrame | None = None

    def refresh(self) -> None:
        existing = resolve_existing_path(self.path)
        self._cache = read_table(self.path) if existing is not None else pd.DataFrame()

    def score(self, lookup: dict, min_samples: int = 20) -> tuple[int, str]:
        if self._cache is None:
            self.refresh()
        if self._cache is None or self._cache.empty:
            return 0, "pattern_table_missing"

        lk = dict(lookup)
        lk["session_name"] = normalize_session_name(lk.get("session_name", ""))
        df = self._cache.copy()
        weights = {
            "regime": 0.28, "side": 0.28, "session_name": 0.16,
            "breakout_pressure_up": 0.08, "breakout_pressure_down": 0.08,
            "compression_flag": 0.04, "three_candle_breakout": 0.04,
            "three_candle_reversal": 0.02, "rising_lows": 0.01, "falling_highs": 0.01,
        }
        sim = pd.Series(0.0, index=df.index)
        total_w = sum(weights.values())
        for k, w in weights.items():
            if k in df.columns and k in lk:
                sim += (df[k] == lk[k]).astype(float) * w
        df["match_score"] = sim / total_w
        df = df[df.get("count", 0) >= int(min_samples)]
        if df.empty:
            return 0, "pattern_too_few_samples"
        best = df.sort_values(["match_score", "count", "avg_r"], ascending=[False, False, False]).iloc[0]
        ms = float(best.get("match_score", 0.0))
        if ms < 0.55:
            return 0, "pattern_no_match"
        avg_r = float(best.get("avg_r", 0.0)); count = int(best.get("count", 0))
        raw = max(-20.0, min(20.0, avg_r * 25.0 * ms))
        score = int(round(raw * self.weight))
        logging.info("PATTERN_SCORE method=fuzzy match_score=%.2f count=%s avg_r=%.4f score=%s", ms, count, avg_r, score)
        return score, "pattern_fuzzy_match"
