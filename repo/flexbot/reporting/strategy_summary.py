from __future__ import annotations

import json
from pathlib import Path
import pandas as pd


def build_strategy_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"by_strategy": {}, "by_side": {}}

    by_strategy = (
        df.groupby("strategy_name")
        .agg(signals=("result_r", "count"), winrate=("result_r", lambda s: float((s > 0).mean() * 100.0)), avg_r=("result_r", "mean"))
        .round(4)
        .to_dict(orient="index")
    )
    by_side = (
        df.groupby("side")
        .agg(trades=("result_r", "count"), winrate=("result_r", lambda s: float((s > 0).mean() * 100.0)), avg_r=("result_r", "mean"))
        .round(4)
        .to_dict(orient="index")
    )
    return {"by_strategy": by_strategy, "by_side": by_side}


def save_strategy_summary(summary: dict, report_dir: str) -> Path:
    p = Path(report_dir)
    p.mkdir(parents=True, exist_ok=True)
    target = p / "strategy_summary.json"
    target.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return target
