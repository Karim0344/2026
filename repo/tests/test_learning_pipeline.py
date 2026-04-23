from pathlib import Path
import sys
import types

import pandas as pd

mt5_stub = types.SimpleNamespace(
    TIMEFRAME_M1=1,
    TIMEFRAME_M5=5,
    TIMEFRAME_M15=15,
    TIMEFRAME_H1=60,
    TIMEFRAME_H4=240,
)
sys.modules.setdefault("MetaTrader5", mt5_stub)

from flexbot.ai.learning_pipeline import LearningPipeline
from flexbot.core.config import BotConfig


def test_learning_pipeline_saves_history_features_and_outcomes(tmp_path):
    cfg = BotConfig(
        store_history_path=str(tmp_path / "history"),
        store_learning_path=str(tmp_path / "learned"),
        store_reports_path=str(tmp_path / "reports"),
        min_samples_context=1,
        min_samples_pattern=1,
    )
    pipeline = LearningPipeline(cfg)

    def fake_refresh_history(symbol: str, timeframe: str) -> pd.DataFrame:
        base = pd.Timestamp("2026-01-01T00:00:00Z")
        rows = []
        for i in range(80):
            price = 100.0 + i * 0.1
            rows.append(
                {
                    "time": base + pd.Timedelta(minutes=i * 5),
                    "open": price,
                    "high": price + 0.2,
                    "low": price - 0.2,
                    "close": price + 0.05,
                    "tick_volume": 100 + i,
                    "spread": 10,
                    "real_volume": 0,
                }
            )
        return pd.DataFrame(rows)

    pipeline.recorder.refresh_history = fake_refresh_history  # type: ignore[method-assign]
    result = pipeline.run(symbol="XAUUSD")

    assert result.history_rows > 0
    assert result.feature_rows > 0
    assert result.outcome_rows > 0
    assert Path(result.history_path).exists()
    assert Path(result.features_path).exists()
    assert Path(result.outcomes_path).exists()
    assert Path(result.context_path).exists()
    assert Path(result.pattern_path).exists()
    assert Path(result.summary_path).exists()


def test_learning_pipeline_falls_back_to_csv_when_parquet_unavailable(tmp_path, monkeypatch):
    cfg = BotConfig(
        store_history_path=str(tmp_path / "history"),
        store_learning_path=str(tmp_path / "learned"),
        store_reports_path=str(tmp_path / "reports"),
        min_samples_context=1,
        min_samples_pattern=1,
    )
    pipeline = LearningPipeline(cfg)

    def fake_refresh_history(symbol: str, timeframe: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "time": pd.Timestamp("2026-01-01T00:00:00Z"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "tick_volume": 100,
                    "spread": 10,
                    "real_volume": 0,
                },
                {
                    "time": pd.Timestamp("2026-01-01T00:05:00Z"),
                    "open": 100.5,
                    "high": 101.5,
                    "low": 100.0,
                    "close": 101.0,
                    "tick_volume": 110,
                    "spread": 10,
                    "real_volume": 0,
                },
            ]
        )

    def fail_parquet(*args, **kwargs):
        raise ImportError("Unable to find a usable engine; tried using: 'pyarrow', 'fastparquet'.")

    pipeline.recorder.refresh_history = fake_refresh_history  # type: ignore[method-assign]
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_parquet)

    result = pipeline.run(symbol="XAUUSD")

    assert result.history_path.endswith(".csv")
    assert result.features_path.endswith(".csv")
    assert result.outcomes_path.endswith(".csv")
    assert result.context_path.endswith(".csv")
    assert result.pattern_path.endswith(".csv")
    assert Path(result.history_path).exists()
