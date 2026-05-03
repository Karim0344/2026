from __future__ import annotations
import numpy as np
import pandas as pd


def compute_range_features(df: pd.DataFrame, cfg) -> pd.DataFrame:
    out = df.copy()
    lookback = int(getattr(cfg, "range_lookback", 60))
    min_atr_ratio = float(getattr(cfg, "range_min_atr_ratio", 1.0))
    max_atr_ratio = float(getattr(cfg, "range_max_atr_ratio", 20.0))
    touch_tol_mult = float(getattr(cfg, "range_touch_tol_mult", 0.2))
    required_touches = int(getattr(cfg, "range_required_touches", 1))
    break_buffer_mult = float(getattr(cfg, "range_break_buffer_mult", 0.1))
    wick_body_min = float(getattr(cfg, "range_wick_body_min", 1.35))
    atr = out.get("atr", pd.Series(0.0, index=out.index))
    range_high = out["high"].rolling(lookback, min_periods=max(5, lookback // 3)).max()
    range_low = out["low"].rolling(lookback, min_periods=max(5, lookback // 3)).min()
    width = (range_high - range_low).clip(lower=1e-12)
    out["close_position_within_range"] = (out["close"] - range_low) / width
    out["near_top"] = out["close_position_within_range"] >= 0.75
    out["near_bottom"] = out["close_position_within_range"] <= 0.25
    out["fake_break_top"] = out["high"] > (range_high + atr * break_buffer_mult)
    out["fake_break_bottom"] = out["low"] < (range_low - atr * break_buffer_mult)
    out["reclaim_top"] = out["close"] < range_high
    out["reclaim_bottom"] = out["close"] > range_low
    out["touches_top"] = out["high"] >= (range_high - (touch_tol_mult * atr))
    out["touches_bottom"] = out["low"] <= (range_low + (touch_tol_mult * atr))
    out["range_width_atr_ratio"] = width / atr.replace(0, np.nan)
    out["range_width_valid"] = (out["range_width_atr_ratio"] >= min_atr_ratio) & (out["range_width_atr_ratio"] <= max_atr_ratio)
    out["range_confirmed"] = out["range_width_valid"] & (out["touches_top"].rolling(lookback, min_periods=1).sum() >= required_touches) & (out["touches_bottom"].rolling(lookback, min_periods=1).sum() >= required_touches)
    body = (out["close"] - out["open"]).abs().replace(0, np.nan)
    out["wick_body_ok_top"] = ((out["high"] - out[["open", "close"]].max(axis=1)) / body) >= wick_body_min
    out["wick_body_ok_bottom"] = ((out[["open", "close"]].min(axis=1) - out["low"]) / body) >= wick_body_min
    return out
