from __future__ import annotations

from pathlib import Path
import pandas as pd


class PatternScorer:
    def __init__(self, store_learning_path: str, weight: float = 1.0):
        self.path = Path(store_learning_path) / "pattern_edge_table.parquet"
        self.weight = float(weight)
        self._cache: pd.DataFrame | None = None

    def refresh(self) -> None:
        if self.path.exists():
            self._cache = pd.read_parquet(self.path)
        else:
            self._cache = pd.DataFrame()

    def score(self, lookup: dict, min_samples: int = 20) -> tuple[int, str]:
        if self._cache is None:
            self.refresh()
        if self._cache is None or self._cache.empty:
            return 0, "pattern_table_missing"

        pattern_keys = [
            "compression_flag",
            "breakout_pressure_up",
            "breakout_pressure_down",
            "three_candle_breakout",
            "three_candle_reversal",
            "rising_lows",
            "falling_highs",
            "mid_range_flag",
            "session_name",
            "regime",
            "side",
        ]
        mask = pd.Series(True, index=self._cache.index)
        for key in pattern_keys:
            if key in lookup:
                mask &= self._cache[key] == lookup[key]

        row = self._cache.loc[mask].head(1)
        if row.empty:
            return 0, "pattern_no_match"

        count = int(row.iloc[0].get("count", 0))
        if count < int(min_samples):
            return 0, "pattern_too_few_samples"

        avg_r = float(row.iloc[0].get("avg_r", 0.0))
        raw = max(-20.0, min(20.0, avg_r * 25.0))
        return int(round(raw * self.weight)), "pattern_match"
