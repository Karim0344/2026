from __future__ import annotations

from pathlib import Path
import pandas as pd
from flexbot.ai.storage import read_table, resolve_existing_path


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

        mask = pd.Series(True, index=self._cache.index)
        for key in ("weekday", "hour", "session_name", "regime", "side", "strategy_name", "timeframe"):
            if key in lookup:
                mask &= self._cache[key] == lookup[key]

        row = self._cache.loc[mask].head(1)
        if row.empty:
            return 0, "context_no_match"

        count = int(row.iloc[0].get("count", 0))
        if count < int(min_samples):
            return 0, "context_too_few_samples"

        avg_r = float(row.iloc[0].get("avg_r", 0.0))
        raw = max(-15.0, min(15.0, avg_r * 20.0))
        return int(round(raw * self.weight)), "context_match"
