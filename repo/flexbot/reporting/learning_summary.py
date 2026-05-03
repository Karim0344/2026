from __future__ import annotations

import json
from pathlib import Path
import pandas as pd


def build_learning_summary(context_table: pd.DataFrame, pattern_table: pd.DataFrame, strategy_table: pd.DataFrame | None = None) -> dict:
    return {
        "context_rows": int(len(context_table)),
        "pattern_rows": int(len(pattern_table)),
        "strategy_rows": int(len(strategy_table)) if strategy_table is not None else 0,
        "top_context_edges": context_table.sort_values("avg_r", ascending=False).head(10).to_dict(orient="records") if not context_table.empty else [],
        "worst_context_edges": context_table.sort_values("avg_r", ascending=True).head(10).to_dict(orient="records") if not context_table.empty else [],
        "top_pattern_edges": pattern_table.sort_values("avg_r", ascending=False).head(10).to_dict(orient="records") if not pattern_table.empty else [],
        "worst_pattern_edges": pattern_table.sort_values("avg_r", ascending=True).head(10).to_dict(orient="records") if not pattern_table.empty else [],
    }


def save_learning_summary(summary: dict, report_dir: str) -> Path:
    p = Path(report_dir)
    p.mkdir(parents=True, exist_ok=True)
    target = p / "learning_summary.json"
    target.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return target
