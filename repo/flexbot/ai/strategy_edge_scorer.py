from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from flexbot.ai.session_utils import normalize_session_name
from flexbot.ai.learning_version import build_learning_version


class StrategyEdgeScorer:
    def __init__(self, store_learning_path: str, cfg=None, weight: float = 1.0):
        self.path = Path(store_learning_path) / "strategy_edge_table.csv"
        self.weight = float(weight)
        self.cfg = cfg
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
            return 0, "no_data"
        current_version = build_learning_version(self.cfg) if self.cfg is not None else ""
        if "learning_version" not in self._cache.columns:
            logging.warning("LEARNING_VERSION_MISMATCH table=strategy_edge_table status=missing_column expected=%s", current_version)
            return 0, "version_mismatch"
        if current_version and not self._cache[self._cache["learning_version"] == current_version].empty:
            self._cache = self._cache[self._cache["learning_version"] == current_version].copy()
        elif current_version:
            logging.warning("LEARNING_VERSION_MISMATCH table=strategy_edge_table expected=%s", current_version)
            return 0, "version_mismatch"
        current_symbol = str(lookup.get("symbol", ""))
        current_timeframe = str(lookup.get("timeframe", ""))
        if "symbol" in self._cache.columns and current_symbol:
            self._cache = self._cache[self._cache["symbol"] == current_symbol].copy()
            if self._cache.empty:
                return 0, "symbol_mismatch"
        if "timeframe" in self._cache.columns and current_timeframe:
            self._cache = self._cache[self._cache["timeframe"] == current_timeframe].copy()
            if self._cache.empty:
                return 0, "tf_mismatch"

        lk = dict(lookup)
        lk["session_name"] = normalize_session_name(lk.get("session_name", ""))
        levels = [
            ("strategy_name", "regime", "side", "session_name", "timeframe"),
            ("strategy_name", "regime", "side", "timeframe"),
            ("strategy_name", "regime", "side"),
            ("strategy_name", "side"),
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
            confidence = min(1.0, count / max(int(min_samples) * 3, 1))
            score = int(round(raw * confidence * self.weight))
            if count < int(min_samples) * 2:
                score = int(round(score * 0.5))
            logging.info("STRATEGY_EDGE_SCORE method=backoff_level_%s count=%s confidence=%.2f avg_r=%.4f score=%s", idx, count, confidence, avg_r, score)
            if score < 0:
                logging.info("STRATEGY_EDGE_SCORE_NEGATIVE count=%s avg_r=%.4f score=%s reason=strategy_penalty", count, avg_r, score)
            return score, f"strategy_backoff_match_{idx}"
        return 0, "no_data"
