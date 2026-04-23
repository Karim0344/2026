from __future__ import annotations

from pathlib import Path
import pandas as pd
from flexbot.ai.storage import write_table


GROUP_COLS = [
    "weekday",
    "hour",
    "session_name",
    "regime",
    "side",
    "strategy_name",
    "timeframe",
]


def build_context_edge_table(df: pd.DataFrame, min_samples: int = 20) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=GROUP_COLS + ["count", "winrate", "avg_r"])

    grouped = (
        df.groupby(GROUP_COLS, dropna=False)
        .agg(
            count=("result_r", "count"),
            winrate=("result_r", lambda s: float((s > 0).mean() * 100.0)),
            avg_r=("result_r", "mean"),
            median_r=("result_r", "median"),
            tp1_rate=("tp1_hit", lambda s: float(s.mean() * 100.0)),
            tp2_rate=("tp2_hit", lambda s: float(s.mean() * 100.0)),
            tp3_rate=("tp3_hit", lambda s: float(s.mean() * 100.0)),
            sl_rate=("sl_hit", lambda s: float(s.mean() * 100.0)),
            mfe_avg=("mfe_r", "mean"),
            mae_avg=("mae_r", "mean"),
        )
        .reset_index()
    )
    return grouped[grouped["count"] >= int(min_samples)].reset_index(drop=True)


def save_context_edge_table(table: pd.DataFrame, store_path: str) -> Path:
    p = Path(store_path)
    p.mkdir(parents=True, exist_ok=True)
    target = p / "context_edge_table.parquet"
    return write_table(df=table, preferred_path=target)
