from __future__ import annotations

from pathlib import Path
import pandas as pd


PATTERN_COLS = [
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


def build_pattern_edge_table(df: pd.DataFrame, min_samples: int = 20) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=PATTERN_COLS + ["count", "winrate", "avg_r"])

    grouped = (
        df.groupby(PATTERN_COLS, dropna=False)
        .agg(
            count=("result_r", "count"),
            winrate=("result_r", lambda s: float((s > 0).mean() * 100.0)),
            avg_r=("result_r", "mean"),
            pattern_strength=("mfe_r", "mean"),
            bias_long=("result_r", lambda s: float(s[df.loc[s.index, "side"] == "long"].mean()) if any(df.loc[s.index, "side"] == "long") else 0.0),
            bias_short=("result_r", lambda s: float(s[df.loc[s.index, "side"] == "short"].mean()) if any(df.loc[s.index, "side"] == "short") else 0.0),
        )
        .reset_index()
    )
    return grouped[grouped["count"] >= int(min_samples)].reset_index(drop=True)


def save_pattern_edge_table(table: pd.DataFrame, store_path: str) -> Path:
    p = Path(store_path)
    p.mkdir(parents=True, exist_ok=True)
    target = p / "pattern_edge_table.parquet"
    table.to_parquet(target, index=False)
    return target
