from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
from flexbot.ai.storage import read_table, resolve_existing_path
from flexbot.ai.session_utils import normalize_session_name
from flexbot.ai.learning_version import build_learning_version


class PatternScorer:
    def __init__(self, store_learning_path: str, cfg=None, weight: float = 1.0):
        self.path = Path(store_learning_path) / "pattern_edge_table.parquet"
        self.weight = float(weight)
        self.cfg = cfg
        self._cache: pd.DataFrame | None = None

    def refresh(self) -> None:
        existing = resolve_existing_path(self.path)
        self._cache = read_table(self.path) if existing is not None else pd.DataFrame()

    def score(self, lookup: dict, min_samples: int = 20) -> tuple[int, str]:
        if self._cache is None:
            self.refresh()
        if self._cache is None or self._cache.empty:
            return 0, "no_data"
        current_version = build_learning_version(self.cfg) if self.cfg is not None else ""
        if "learning_version" not in self._cache.columns:
            logging.warning("LEARNING_VERSION_MISMATCH table=pattern_edge_table status=missing_column expected=%s", current_version)
            return 0, "version_mismatch"
        if current_version and not self._cache[self._cache["learning_version"] == current_version].empty:
            self._cache = self._cache[self._cache["learning_version"] == current_version].copy()
        elif current_version:
            logging.warning("LEARNING_VERSION_MISMATCH table=pattern_edge_table expected=%s", current_version)
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
        df = self._cache.copy()
        if "side" in df.columns and "side" in lk:
            df = df[df["side"] == lk["side"]]
        if "regime" in df.columns and "regime" in lk:
            reg = str(lk["regime"])
            if reg in ("range_breakout_pressure_up", "range_breakout_pressure_down"):
                df = df[df["regime"].isin([reg, "range"]) ]
            else:
                df = df[df["regime"] == reg]
        if df.empty:
            return 0, "no_data"
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
            return 0, "no_data"
        best = df.sort_values(["match_score", "count", "avg_r"], ascending=[False, False, False]).iloc[0]
        ms = float(best.get("match_score", 0.0))
        if ms < 0.55:
            return 0, "no_data"
        avg_r = float(best.get("avg_r", 0.0)); count = int(best.get("count", 0))
        raw = max(-20.0, min(20.0, avg_r * 25.0 * ms))
        confidence = min(1.0, count / max(int(min_samples) * 3, 1))
        score = int(round(raw * confidence * self.weight))
        logging.info("PATTERN_SCORE method=fuzzy match_score=%.2f count=%s confidence=%.2f avg_r=%.4f score=%s", ms, count, confidence, avg_r, score)
        if score < 0:
            logging.info("PATTERN_SCORE_NEGATIVE match_score=%.2f avg_r=%.4f score=%s reason=pattern_penalty", ms, avg_r, score)
        return score, "pattern_fuzzy_match"
