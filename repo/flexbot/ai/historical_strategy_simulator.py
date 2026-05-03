from __future__ import annotations

import pandas as pd


def _strategy_from_row(row: pd.Series) -> str | None:
    reg = str(row.get("regime", ""))
    trend_score_long = float(row.get("trend_score_long", 0.0))
    trend_score_short = float(row.get("trend_score_short", 0.0))
    trend_min = float(row.get("trend_min_score", 60.0))
    short_extra = float(row.get("trend_short_extra_score", 10.0))
    allow_short = bool(row.get("trend_allow_short", False))

    if reg.startswith("range"):
        if all(bool(row.get(k, False)) for k in ("near_bottom", "fake_break_bottom", "reclaim_bottom", "wick_body_ok_bottom", "range_confirmed")):
            return "RANGE_LONG"
        if all(bool(row.get(k, False)) for k in ("near_top", "fake_break_top", "reclaim_top", "wick_body_ok_top", "range_confirmed")):
            return "RANGE_SHORT"
        return None

    effective_min = float(row.get("effective_min_score", trend_min))
    require_htf = bool(row.get("trend_require_htf", False))
    htf_ok_long = bool(row.get("htf_ok_long", True))
    htf_ok_short = bool(row.get("htf_ok_short", True))
    require_breakout = bool(row.get("require_breakout", False))
    breakout_ok_long = bool(row.get("breakout_ok_long", True))
    breakout_ok_short = bool(row.get("breakout_ok_short", True))
    momentum_long = bool(row.get("bullish_close", True))
    momentum_short = bool(row.get("bearish_close", True))

    long_gate = (not require_htf or htf_ok_long) and (not require_breakout or breakout_ok_long) and momentum_long
    short_gate = (not require_htf or htf_ok_short) and (not require_breakout or breakout_ok_short) and momentum_short

    if trend_score_long >= effective_min and bool(row.get("pullback_ok_long", False)) and long_gate:
        return "PRO_LONG"
    if allow_short and trend_score_short >= (effective_min + short_extra) and bool(row.get("pullback_ok_short", False)) and short_gate:
        return "PRO_SHORT"
    return None


def build_strategy_edge_table(df: pd.DataFrame, min_samples: int = 20) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    simulated = df.copy()
    simulated["strategy_name"] = simulated.apply(_strategy_from_row, axis=1)
    simulated = simulated[simulated["strategy_name"].notna()].copy()
    if simulated.empty:
        return pd.DataFrame()
    simulated["side"] = simulated["strategy_name"].map({"PRO_LONG": "long", "RANGE_LONG": "long", "PRO_SHORT": "short", "RANGE_SHORT": "short"})
    grouped = simulated.groupby(["strategy_name", "regime", "side", "session_name", "timeframe"], dropna=False).agg(
        count=("result_r", "size"), winrate=("result_r", lambda s: (s.gt(0).mean() * 100.0)), avg_r=("result_r", "mean"),
        tp1_rate=("tp1_hit", "mean"), tp2_rate=("tp2_hit", "mean"), tp3_rate=("tp3_hit", "mean"), sl_rate=("sl_hit", "mean"),
    ).reset_index()
    grouped = grouped[grouped["count"] >= int(min_samples)].copy()
    for c in ("winrate", "avg_r", "tp1_rate", "tp2_rate", "tp3_rate", "sl_rate"):
        grouped[c] = grouped[c].astype(float).round(4)
    return grouped
