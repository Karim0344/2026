from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from flexbot.ai.session_utils import normalize_session_name


class StrategyEdgeScorer:
    def __init__(self, store_learning_path: str, weight: float = 1.0):
        self.path = Path(store_learning_path) / "strategy_edge_table.csv"
        self.weight = float(weight)
        self._cache: pd.DataFrame | None = None

    def refresh(self) -> None:
        if not self.path.exists():
            self._cache = pd.DataFrame()
            return
        self._cache = pd.read_csv(self.path)

    def score(self, lookup: dict, min_samples: int = 20) -> tuple[int, str]:
        if self._cache is None:
            self.refresh()
        if self._cache is None or self._cache.empty:
            return 0, "strategy_table_missing"

        lk = dict(lookup)
        lk["session_name"] = normalize_session_name(lk.get("session_name", ""))
        levels = [
            ("strategy_name", "regime", "side", "session_name", "timeframe"),
            ("strategy_name", "regime", "side", "timeframe"),
            ("regime", "side", "session_name", "timeframe"),
            ("regime", "side"),
        ]
        for idx, keys in enumerate(levels, start=1):
            mask = pd.Series(True, index=self._cache.index)
            for k in keys:
                if k in self._cache.columns and k in lk:
                    mask &= self._cache[k] == lk[k]
            row = self._cache.loc[mask].sort_values("count", ascending=False).head(1)
            if row.empty:
                continue
            count = int(row.iloc[0].get("count", 0))
            if count < int(min_samples):
                continue
            avg_r = float(row.iloc[0].get("avg_r", 0.0))
            raw = max(-20.0, min(20.0, avg_r * 25.0))
            score = int(round(raw * self.weight))
            logging.info("STRATEGY_EDGE_SCORE method=backoff_level_%s count=%s avg_r=%.4f score=%s", idx, count, avg_r, score)
            if score < 0:
                logging.info("STRATEGY_EDGE_SCORE_NEGATIVE count=%s avg_r=%.4f score=%s reason=strategy_penalty", count, avg_r, score)
            return score, f"strategy_backoff_match_{idx}"
        return 0, "strategy_no_match"
