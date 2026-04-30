from __future__ import annotations

import pandas as pd


def _strategy_from_row(row: pd.Series) -> str | None:
    reg = str(row.get("regime", ""))
    side = str(row.get("side", "")).lower()
    trend_ok = bool(row.get("trend_ok", False))
    pullback = bool(row.get("pullback", False))
    momentum = bool(row.get("momentum", False))
    breakout = bool(row.get("breakout", False))

    if reg.startswith("range"):
        if side == "long" and bool(row.get("rising_lows", False)) and bool(row.get("three_candle_reversal", False)):
            return "RANGE_LONG"
        if side == "short" and bool(row.get("falling_highs", False)) and bool(row.get("breakout_pressure_up", False)):
            return "RANGE_SHORT"
        return None

    if side == "long" and trend_ok and pullback and momentum and breakout:
        return "PRO_LONG"
    if side == "short" and trend_ok and pullback and momentum and breakout:
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
    grouped = simulated.groupby(["strategy_name", "regime", "side", "session_name", "timeframe"], dropna=False).agg(
        count=("result_r", "size"),
        winrate=("result_r", lambda s: (s.gt(0).mean() * 100.0)),
        avg_r=("result_r", "mean"),
        tp1_rate=("tp1_hit", "mean"),
        tp2_rate=("tp2_hit", "mean"),
        tp3_rate=("tp3_hit", "mean"),
        sl_rate=("sl_hit", "mean"),
    ).reset_index()
    grouped = grouped[grouped["count"] >= int(min_samples)].copy()
    for c in ("winrate", "avg_r", "tp1_rate", "tp2_rate", "tp3_rate", "sl_rate"):
        grouped[c] = grouped[c].astype(float).round(4)
    return grouped
