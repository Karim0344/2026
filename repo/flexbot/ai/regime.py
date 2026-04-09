from typing import Literal

import numpy as np
import pandas as pd

from flexbot.mt5 import client

Regime = Literal["trend", "range", "high_volatility", "dead"]


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


def detect_regime(symbol: str, timeframe: str, bars: int = 400) -> tuple[Regime, dict]:
    rates = client.copy_rates(symbol, timeframe, bars)
    if rates is None or len(rates) < 100:
        return "dead", {"reason": "no_data"}

    df = pd.DataFrame(rates)
    df["ma50"] = _sma(df["close"], 50)
    df["ma200"] = _sma(df["close"], 200)
    df["atr14"] = _atr(df, 14)

    c0 = df.iloc[-2]
    c10 = df.iloc[-12]
    clean_df = df.dropna(subset=["ma50", "ma200", "atr14"])
    valid_rows = int(len(clean_df))

    if np.isnan(c0["ma50"]) or np.isnan(c0["ma200"]) or np.isnan(c0["atr14"]):
        fallback_debug = {
            "reason": "indicator_nan_uncertain",
            "bars": int(len(df)),
            "valid_rows": valid_rows,
        }
        if valid_rows:
            last_valid = clean_df.iloc[-1]
            fallback_debug.update(
                {
                    "last_valid_close": round(float(last_valid["close"]), 5),
                    "last_valid_ma50": round(float(last_valid["ma50"]), 5),
                    "last_valid_ma200": round(float(last_valid["ma200"]), 5),
                    "last_valid_atr": round(float(last_valid["atr14"]), 5),
                }
            )
        return "dead", fallback_debug

    close0 = float(c0["close"])
    ma50 = float(c0["ma50"])
    ma200 = float(c0["ma200"])
    atr = float(c0["atr14"])

    trend_distance = abs(ma50 - ma200)
    recent_move = abs(float(c0["close"]) - float(c10["close"]))

    debug = {
        "close": round(close0, 5),
        "ma50": round(ma50, 5),
        "ma200": round(ma200, 5),
        "atr": round(atr, 5),
        "bars": int(len(df)),
        "valid_rows": valid_rows,
        "trend_distance": round(trend_distance, 5),
        "recent_move": round(recent_move, 5),
    }

    if atr <= 0:
        return "dead", debug

    if recent_move > atr * 6:
        return "high_volatility", debug

    if trend_distance > atr * 0.8 and recent_move > atr * 2:
        return "trend", debug

    if trend_distance < atr * 1.2:
        return "range", debug

    return "range", debug
