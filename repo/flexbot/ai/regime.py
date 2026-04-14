from typing import Literal

import numpy as np
import pandas as pd

from flexbot.mt5 import client

Regime = Literal[
    "trend",
    "trend_overextended",
    "range",
    "range_breakout_pressure_up",
    "range_breakout_pressure_down",
    "high_volatility",
    "dead",
]


def _sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n).mean()


def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(n).mean()


def detect_regime(
    symbol: str,
    timeframe: str,
    bars: int = 400,
    ma_fast: int = 50,
    ma_slow: int = 100,
) -> tuple[Regime, dict]:
    if ma_fast <= 1 or ma_slow <= ma_fast:
        return "dead", {"reason": "invalid_ma_config", "ma_fast": ma_fast, "ma_slow": ma_slow}

    min_required_bars = max(100, ma_slow + 5)
    rates = client.copy_rates(symbol, timeframe, bars)
    if rates is None or len(rates) < min_required_bars:
        return "dead", {"reason": "no_data"}

    df = pd.DataFrame(rates)
    fast_label = f"ma{ma_fast}"
    slow_label = f"ma{ma_slow}"
    df[fast_label] = _sma(df["close"], ma_fast)
    df[slow_label] = _sma(df["close"], ma_slow)
    df["atr14"] = _atr(df, 14)

    c0 = df.iloc[-2]
    c10 = df.iloc[-12]
    clean_df = df.dropna(subset=[fast_label, slow_label, "atr14"])
    valid_rows = int(len(clean_df))

    if np.isnan(c0[fast_label]) or np.isnan(c0[slow_label]) or np.isnan(c0["atr14"]):
        fallback_debug = {
            "reason": "indicator_nan_uncertain",
            "bars": int(len(df)),
            "valid_rows": valid_rows,
            "ma_fast": ma_fast,
            "ma_slow": ma_slow,
        }
        if valid_rows:
            last_valid = clean_df.iloc[-1]
            fallback_debug.update(
                {
                    "last_valid_close": round(float(last_valid["close"]), 5),
                    "last_valid_fast_ma": round(float(last_valid[fast_label]), 5),
                    "last_valid_slow_ma": round(float(last_valid[slow_label]), 5),
                    "last_valid_atr": round(float(last_valid["atr14"]), 5),
                }
            )
        return "dead", fallback_debug

    close0 = float(c0["close"])
    fast_ma = float(c0[fast_label])
    slow_ma = float(c0[slow_label])
    atr = float(c0["atr14"])

    trend_distance = abs(fast_ma - slow_ma)
    recent_move = abs(float(c0["close"]) - float(c10["close"]))
    move_dir = float(c0["close"]) - float(c10["close"])

    debug = {
        "close": round(close0, 5),
        "ma_fast": round(fast_ma, 5),
        "ma_slow": round(slow_ma, 5),
        "atr": round(atr, 5),
        "bars": int(len(df)),
        "valid_rows": valid_rows,
        "trend_distance": round(trend_distance, 5),
        "recent_move": round(recent_move, 5),
        "ma_fast_period": ma_fast,
        "ma_slow_period": ma_slow,
        "move_dir": round(move_dir, 5),
    }

    if atr <= 0:
        return "dead", debug

    if recent_move > atr * 6:
        return "high_volatility", debug

    if trend_distance > atr * 0.8 and recent_move > atr * 3.6:
        return "trend_overextended", debug

    if trend_distance > atr * 0.8 and recent_move > atr * 2:
        return "trend", debug

    if trend_distance < atr * 1.2:
        recent_high = float(df["high"].iloc[-22:-2].max())
        recent_low = float(df["low"].iloc[-22:-2].min())
        zone_width = max(recent_high - recent_low, 1e-9)
        close_pos = (close0 - recent_low) / zone_width
        debug["close_pos"] = round(close_pos, 4)
        debug["recent_high"] = round(recent_high, 5)
        debug["recent_low"] = round(recent_low, 5)
        if close_pos > 0.83 and move_dir > atr * 1.3:
            return "range_breakout_pressure_up", debug
        if close_pos < 0.17 and move_dir < -(atr * 1.3):
            return "range_breakout_pressure_down", debug
        return "range", debug

    return "range", debug
