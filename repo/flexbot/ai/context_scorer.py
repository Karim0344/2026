from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
from flexbot.ai.storage import read_table, resolve_existing_path
from flexbot.ai.session_utils import normalize_session_name


class ContextScorer:
    def __init__(self, store_learning_path: str, weight: float = 1.0):
        self.path = Path(store_learning_path) / "context_edge_table.parquet"
        self.weight = float(weight)
        self._cache: pd.DataFrame | None = None

    def refresh(self) -> None:
        existing = resolve_existing_path(self.path)
        self._cache = read_table(self.path) if existing is not None else pd.DataFrame()

    def score(self, lookup: dict, min_samples: int = 20) -> tuple[int, str]:
        if self._cache is None:
            self.refresh()
        if self._cache is None or self._cache.empty:
            return 0, "context_table_missing"

        lookup = dict(lookup)
        lookup["session_name"] = normalize_session_name(lookup.get("session_name", ""))
        levels = [
            ("weekday", "hour", "session_name", "regime", "side", "timeframe"),
            ("hour", "session_name", "regime", "side", "timeframe"),
            ("session_name", "regime", "side", "timeframe"),
            ("regime", "side"),
        ]
        if lookup.get("strategy_name"):
            levels = [("strategy_name",) + l for l in levels] + levels

        for idx, keys in enumerate(levels, start=1):
            mask = pd.Series(True, index=self._cache.index)
            for key in keys:
                if key in lookup and key in self._cache.columns:
                    mask &= self._cache[key] == lookup[key]
            row = self._cache.loc[mask].sort_values("count", ascending=False).head(1)
            if row.empty:
                continue
            count = int(row.iloc[0].get("count", 0))
            if count < int(min_samples):
                continue
            avg_r = float(row.iloc[0].get("avg_r", 0.0))
            raw = max(-15.0, min(15.0, avg_r * 20.0))
            score = int(round(raw * self.weight))
            logging.info("CONTEXT_SCORE method=backoff_level_%s count=%s avg_r=%.4f score=%s", idx, count, avg_r, score)
            if score < 0:
                logging.info("CONTEXT_SCORE_NEGATIVE count=%s avg_r=%.4f score=%s reason=context_penalty", count, avg_r, score)
            return score, f"context_backoff_match_{idx}"

        return 0, "context_no_match"
