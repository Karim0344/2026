from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import logging

import pandas as pd

from flexbot.ai.feature_builder import build_features
from flexbot.ai.outcome_labeler import label_outcomes
from flexbot.ai.pattern_edge_engine import build_pattern_edge_table, save_pattern_edge_table
from flexbot.ai.statistical_edge_engine import build_context_edge_table, save_context_edge_table
from flexbot.ai.storage import write_table
from flexbot.ai.historical_strategy_simulator import build_strategy_edge_table
from flexbot.data.historical_data_recorder import HistoricalDataRecorder
from flexbot.reporting.learning_summary import build_learning_summary, save_learning_summary


@dataclass
class LearningRunResult:
    history_rows: int = 0
    feature_rows: int = 0
    outcome_rows: int = 0
    context_rows: int = 0
    pattern_rows: int = 0
    history_path: str = ""
    features_path: str = ""
    outcomes_path: str = ""
    context_path: str = ""
    pattern_path: str = ""
    strategy_path: str = ""
    summary_path: str = ""


class LearningPipeline:
    def __init__(self, cfg):
        self.cfg = cfg
        self.recorder = HistoricalDataRecorder(cfg)

    def run(self, symbol: str) -> LearningRunResult:
        logging.info("LEARNING_PIPELINE_START symbol=%s", symbol)
        timeframes = self._timeframes_to_refresh()
        history_frames: list[pd.DataFrame] = []

        for tf in timeframes:
            df = self.recorder.refresh_history(symbol=symbol, timeframe=tf)
            if df.empty:
                logging.info("HISTORY_REFRESHED symbol=%s tf=%s rows=0", symbol, tf)
                continue

            df = df.copy()
            df["timeframe"] = tf
            history_frames.append(df)
            logging.info("HISTORY_REFRESHED symbol=%s tf=%s rows=%s", symbol, tf, len(df))

        history_df = (
            pd.concat(history_frames, ignore_index=True).sort_values("time").reset_index(drop=True)
            if history_frames
            else pd.DataFrame()
        )
        history_path = self._save_learning_frame(history_df, "history")

        if history_df.empty:
            logging.info("HISTORY_REFRESHED rows=0 path=%s", history_path)
            logging.info("FEATURES_BUILT rows=0")
            logging.info("OUTCOMES_LABELED rows=0")
            logging.info("LEARNING_PIPELINE_END symbol=%s status=history_empty", symbol)
            return LearningRunResult(history_path=str(history_path))

        features_frames: list[pd.DataFrame] = []
        for tf in timeframes:
            tf_df = history_df.loc[history_df["timeframe"] == tf].copy()
            if tf_df.empty:
                continue
            built = build_features(
                df=tf_df,
                strategy_name="historical_learning",
                symbol=symbol,
                timeframe=tf,
            )
            built["regime"] = self._infer_regime(built)
            built["side"] = built.apply(
                lambda r: "long" if float(r.get("close", 0.0)) >= float(r.get("open", 0.0)) else "short",
                axis=1,
            )
            features_frames.append(built)

        features_df = (
            pd.concat(features_frames, ignore_index=True).sort_values("time").reset_index(drop=True)
            if features_frames
            else pd.DataFrame()
        )
        features_path = self._save_learning_frame(features_df, "features")
        logging.info("FEATURES_BUILT rows=%s path=%s", len(features_df), features_path)

        outcomes_df = label_outcomes(features_df, spread_cost_points=self.cfg.learning_spread_cost_points, slippage_points=self.cfg.learning_slippage_points, point_size=self.cfg.learning_point_size)
        outcomes_path = self._save_learning_frame(outcomes_df, "outcomes")
        logging.info("OUTCOMES_LABELED rows=%s path=%s", len(outcomes_df), outcomes_path)

        context_table = build_context_edge_table(
            outcomes_df,
            min_samples=self.cfg.min_samples_context,
        )
        context_path = save_context_edge_table(context_table, self.cfg.store_learning_path)
        logging.info("CONTEXT_TABLE_BUILT rows=%s path=%s", len(context_table), context_path)

        pattern_table = build_pattern_edge_table(
            outcomes_df,
            min_samples=self.cfg.min_samples_pattern,
        )
        pattern_path = save_pattern_edge_table(pattern_table, self.cfg.store_learning_path)
        logging.info("PATTERN_TABLE_BUILT rows=%s path=%s", len(pattern_table), pattern_path)

        strategy_table = build_strategy_edge_table(outcomes_df, min_samples=self.cfg.min_samples_context)
        strategy_path = Path(self.cfg.store_learning_path) / "strategy_edge_table.csv"
        strategy_table.to_csv(strategy_path, index=False)
        logging.info("STRATEGY_EDGE_TABLE_BUILT rows=%s path=%s", len(strategy_table), strategy_path)

        summary = build_learning_summary(context_table=context_table, pattern_table=pattern_table)
        summary_path = save_learning_summary(summary=summary, report_dir=self.cfg.store_reports_path)
        logging.info("LEARNING_SUMMARY_SAVED path=%s", summary_path)
        logging.info("LEARNING_PIPELINE_END symbol=%s status=ok", symbol)

        return LearningRunResult(
            history_rows=len(history_df),
            feature_rows=len(features_df),
            outcome_rows=len(outcomes_df),
            context_rows=len(context_table),
            pattern_rows=len(pattern_table),
            history_path=str(history_path),
            features_path=str(features_path),
            outcomes_path=str(outcomes_path),
            context_path=str(context_path),
            pattern_path=str(pattern_path),
            strategy_path=str(strategy_path),
            summary_path=str(summary_path),
        )

    def _timeframes_to_refresh(self) -> list[str]:
        configured = ["M5", "M15", "H1", str(self.cfg.timeframe)]
        out: list[str] = []
        for tf in configured:
            if tf not in out:
                out.append(tf)
        return out

    @staticmethod
    def _infer_regime(df: pd.DataFrame) -> pd.Series:
        trend_strength = df.get("trend_strength", pd.Series([0.0] * len(df), index=df.index)).astype(float)
        atr_pct = df.get("atr_percentile", pd.Series([0.0] * len(df), index=df.index)).astype(float)
        close_pos = df.get("close_position_within_range", pd.Series([0.5] * len(df), index=df.index)).astype(float)

        regime = pd.Series("range", index=df.index, dtype="object")
        regime = regime.mask(atr_pct >= 0.9, "high_volatility")
        regime = regime.mask((trend_strength > 0.35) & (atr_pct < 0.9), "trend")
        regime = regime.mask((trend_strength < -0.35) & (atr_pct < 0.9), "trend")
        regime = regime.mask(close_pos > 0.8, "range_breakout_pressure_up")
        regime = regime.mask(close_pos < 0.2, "range_breakout_pressure_down")
        return regime

    def _save_learning_frame(self, df: pd.DataFrame, name: str) -> Path:
        target = Path(self.cfg.store_learning_path) / f"{name}.parquet"
        actual = write_table(df=df, preferred_path=target)
        logging.info(
            "LEARNING_FRAME_SAVED name=%s rows=%s path=%s format=%s",
            name,
            len(df),
            actual,
            actual.suffix.lstrip("."),
        )
        return actual
