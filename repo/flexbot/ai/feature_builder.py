from __future__ import annotations

import numpy as np
import pandas as pd


def build_features(df: pd.DataFrame, strategy_name: str, symbol: str, timeframe: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy().sort_values("time").reset_index(drop=True)
    out["time"] = pd.to_datetime(out["time"], utc=True)

    out["weekday"] = out["time"].dt.weekday
    out["hour"] = out["time"].dt.hour
    out["minute_bucket"] = (out["time"].dt.minute // 5) * 5
    out["week_number"] = out["time"].dt.isocalendar().week.astype(int)
    out["month"] = out["time"].dt.month
    out["session_name"] = out["hour"].map(_session_name)
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

    fast_ma = out["close"].rolling(20, min_periods=5).mean()
    slow_ma = out["close"].rolling(50, min_periods=10).mean()
    out["distance_to_ma_fast"] = out["close"] - fast_ma
    out["distance_to_ma_slow"] = out["close"] - slow_ma
    out["trend_strength"] = (fast_ma - slow_ma) / out["candle_range"]

    atr = _atr(out, period=14)
    out["atr"] = atr
    out["atr_percentile"] = atr.rolling(252, min_periods=30).rank(pct=True)
    out["volatility_bucket"] = pd.cut(
        out["atr_percentile"],
        bins=[-0.01, 0.2, 0.8, 1.01],
        labels=["low", "normal", "high"],
    ).astype(str)

    range_high = out["high"].rolling(20, min_periods=5).max()
    range_low = out["low"].rolling(20, min_periods=5).min()
    width = (range_high - range_low).clip(lower=1e-12)
    out["distance_to_range_high"] = range_high - out["close"]
    out["distance_to_range_low"] = out["close"] - range_low
    out["close_position_within_range"] = (out["close"] - range_low) / width
    out["touches_top"] = out["high"] >= (range_high - 0.1 * atr)
    out["touches_bottom"] = out["low"] <= (range_low + 0.1 * atr)
    out["mid_range_flag"] = out["close_position_within_range"].between(0.4, 0.6)
    out["range_width"] = width
    out["range_width_atr_ratio"] = width / atr.replace(0, np.nan)

    out["higher_high"] = out["high"] > out["high"].shift(1)
    out["lower_low"] = out["low"] < out["low"].shift(1)
    out["rising_lows"] = (out["low"] > out["low"].shift(1)) & (out["low"].shift(1) > out["low"].shift(2))
    out["falling_highs"] = (out["high"] < out["high"].shift(1)) & (out["high"].shift(1) < out["high"].shift(2))
    out["breakout_pressure_up"] = out["rising_lows"] & out["compression_flag"]
    out["breakout_pressure_down"] = out["falling_highs"] & out["compression_flag"]
    out["three_candle_breakout"] = out["close"] > out["high"].shift(1).rolling(2).max()
    out["three_candle_reversal"] = out["bullish"] & out["bearish"].shift(1) & out["bearish"].shift(2)

    out["strategy_name"] = strategy_name
    out["symbol"] = symbol
    out["timeframe"] = timeframe

    return out.replace([np.inf, -np.inf], np.nan).fillna(0)


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=max(2, period // 2)).mean().fillna(0)


def _session_name(hour: int) -> str:
    if hour < 7:
        return "Asia"
    if hour < 13:
        return "London"
    if hour < 17:
        return "London/NY_overlap"
    return "New_York"
