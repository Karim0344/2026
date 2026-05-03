from __future__ import annotations

import numpy as np
import pandas as pd
from flexbot.ai.session_utils import normalize_session_name
from flexbot.strategy.range_features import compute_range_features


def build_features(df: pd.DataFrame, strategy_name: str, symbol: str, timeframe: str, cfg=None) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy().sort_values("time").reset_index(drop=True)
    out["time"] = pd.to_datetime(out["time"], utc=True)

    out["weekday"] = out["time"].dt.weekday
    out["hour"] = out["time"].dt.hour
    out["minute_bucket"] = (out["time"].dt.minute // 5) * 5
    out["week_number"] = out["time"].dt.isocalendar().week.astype(int)
    out["month"] = out["time"].dt.month
    out["session_name"] = out["hour"].map(normalize_session_name)
    out["is_monday_open"] = (out["weekday"] == 0) & (out["hour"] < 2)
    out["is_friday_close"] = (out["weekday"] == 4) & (out["hour"] >= 20)

    out["candle_range"] = (out["high"] - out["low"]).clip(lower=1e-12)
    out["body_size"] = (out["close"] - out["open"]).abs()
    out["upper_wick"] = (out[["open", "close"]].max(axis=1) - out["high"]).abs()
    out["lower_wick"] = (out[["open", "close"]].min(axis=1) - out["low"]).abs()
    out["wick_ratio"] = (out["upper_wick"] + out["lower_wick"]) / out["body_size"].replace(0, np.nan)
    out["body_ratio"] = out["body_size"] / out["candle_range"]
    out["bullish"] = out["close"] > out["open"]
    out["bearish"] = out["close"] < out["open"]
    out["full_body_flag"] = out["body_ratio"] >= 0.7
    out["indecision_flag"] = out["body_ratio"] <= 0.25
    out["compression_flag"] = out["candle_range"] <= out["candle_range"].rolling(20, min_periods=5).quantile(0.25)

    ma_fast_period = int(getattr(cfg, "ma_fast", 50)) if cfg is not None else 50
    ma_trend_period = int(getattr(cfg, "ma_trend", 100)) if cfg is not None else 100
    fast_ma = out["close"].rolling(ma_fast_period, min_periods=max(5, ma_fast_period // 4)).mean()
    slow_ma = out["close"].rolling(ma_trend_period, min_periods=max(10, ma_trend_period // 4)).mean()
    out["distance_to_ma_fast"] = out["close"] - fast_ma
    out["distance_to_ma_slow"] = out["close"] - slow_ma
    out["trend_strength"] = (fast_ma - slow_ma) / out["candle_range"]

    atr_period = int(getattr(cfg, "atr_period", 14)) if cfg is not None else 14
    atr = _atr(out, period=atr_period)
    out["atr"] = atr
    out["atr_percentile"] = atr.rolling(252, min_periods=30).rank(pct=True)
    out["volatility_bucket"] = pd.cut(
        out["atr_percentile"],
        bins=[-0.01, 0.2, 0.8, 1.01],
        labels=["low", "normal", "high"],
    ).astype(str)

    range_lookback = int(getattr(cfg, "range_lookback", 60)) if cfg is not None else 60
    min_atr_ratio = float(getattr(cfg, "range_min_atr_ratio", 1.0)) if cfg is not None else 1.0
    max_atr_ratio = float(getattr(cfg, "range_max_atr_ratio", 20.0)) if cfg is not None else 20.0
    max_atr_ratio_percentile = float(getattr(cfg, "range_max_atr_ratio_percentile", 0.95)) if cfg is not None else 0.95
    ratio_window = int(getattr(cfg, "range_atr_ratio_percentile_window", 240)) if cfg is not None else 240
    touch_tol_mult = float(getattr(cfg, "range_touch_tol_mult", 0.2)) if cfg is not None else 0.2
    required_touches = int(getattr(cfg, "range_required_touches", 1)) if cfg is not None else 1
    break_buffer_mult = float(getattr(cfg, "range_break_buffer_mult", 0.1)) if cfg is not None else 0.1
    wick_body_min = float(getattr(cfg, "range_wick_body_min", 1.35)) if cfg is not None else 1.35
    range_high = out["high"].rolling(range_lookback, min_periods=max(5, range_lookback // 3)).max()
    range_low = out["low"].rolling(range_lookback, min_periods=max(5, range_lookback // 3)).min()
    width = (range_high - range_low).clip(lower=1e-12)
    out["distance_to_range_high"] = range_high - out["close"]
    out["distance_to_range_low"] = out["close"] - range_low
    out["close_position_within_range"] = (out["close"] - range_low) / width
    out["touches_top"] = out["high"] >= (range_high - (touch_tol_mult * atr))
    out["touches_bottom"] = out["low"] <= (range_low + (touch_tol_mult * atr))
    out["mid_range_flag"] = out["close_position_within_range"].between(0.4, 0.6)
    out["range_width"] = width
    out["range_width_atr_ratio"] = width / atr.replace(0, np.nan)

    out["higher_high"] = out["high"] > out["high"].shift(1)
    out["lower_low"] = out["low"] < out["low"].shift(1)
    out["previous_high"] = out["high"].shift(1)
    out["previous_low"] = out["low"].shift(1)
    out["rising_lows"] = (out["low"] > out["low"].shift(1)) & (out["low"].shift(1) > out["low"].shift(2))
    out["falling_highs"] = (out["high"] < out["high"].shift(1)) & (out["high"].shift(1) < out["high"].shift(2))
    out["breakout_pressure_up"] = out["rising_lows"] & out["compression_flag"]
    out["breakout_pressure_down"] = out["falling_highs"] & out["compression_flag"]
    out["three_candle_breakout"] = out["close"] > out["high"].shift(1).rolling(2).max()
    out["three_candle_reversal"] = out["bullish"] & out["bearish"].shift(1) & out["bearish"].shift(2)
    out["trend_ok_long"] = out["close"] > slow_ma
    out["trend_ok_short"] = out["close"] < slow_ma
    out["htf_ok_long"] = out["distance_to_ma_fast"] > 0
    out["htf_ok_short"] = out["distance_to_ma_fast"] < 0
    out["pullback_ok_long"] = out["low"] <= (fast_ma + (atr * 0.35))
    out["pullback_ok_short"] = out["high"] >= (fast_ma - (atr * 0.35))
    out["bullish_close"] = out["close"] > out["open"]
    out["bearish_close"] = out["close"] < out["open"]
    out["breakout_ok_long"] = out["high"] > out["previous_high"]
    out["breakout_ok_short"] = out["low"] < out["previous_low"]
    out["trend_ok"] = out["trend_ok_long"] | out["trend_ok_short"]
    out["pullback"] = out["pullback_ok_long"] | out["pullback_ok_short"]
    out["momentum"] = out["bullish_close"] | out["bearish_close"]
    out["breakout"] = out["breakout_ok_long"] | out["breakout_ok_short"]
    out["trend_score_long"] = (out["trend_ok_long"].astype(int) * 40) + (out["pullback_ok_long"].astype(int) * 25) + (out["bullish_close"].astype(int) * 15) + (out["breakout_ok_long"].astype(int) * 20)
    out["trend_score_short"] = (out["trend_ok_short"].astype(int) * 40) + (out["pullback_ok_short"].astype(int) * 25) + (out["bearish_close"].astype(int) * 15) + (out["breakout_ok_short"].astype(int) * 20)
    out["trend_min_score"] = int(getattr(cfg, "trend_min_score", 60)) if cfg is not None else 60
    out["trend_short_extra_score"] = int(getattr(cfg, "trend_short_extra_score", 10)) if cfg is not None else 10
    out["trend_allow_short"] = bool(getattr(cfg, "trend_allow_short", False)) if cfg is not None else False
    out = compute_range_features(out, cfg if cfg is not None else type("Cfg", (), {})())

    out["strategy_name"] = strategy_name
    out["symbol"] = symbol
    out["timeframe"] = timeframe

    out = out.replace([np.inf, -np.inf], np.nan)
    numeric_cols = out.select_dtypes(include=[np.number, "bool"]).columns
    object_cols = [c for c in out.columns if c not in numeric_cols]
    if len(numeric_cols) > 0:
        out.loc[:, numeric_cols] = out.loc[:, numeric_cols].fillna(0)
    if object_cols:
        out.loc[:, object_cols] = out.loc[:, object_cols].fillna("")
    return out


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=max(2, period // 2)).mean().fillna(0)
