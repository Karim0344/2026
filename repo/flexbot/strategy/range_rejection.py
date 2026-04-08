from dataclasses import dataclass
from typing import Optional

import pandas as pd

from flexbot.mt5 import client


@dataclass
class Intent:
    direction: Optional[str]
    entry: float
    sl: float
    reason: str
    debug: dict


def get_range_intent(symbol: str, timeframe: str, cfg) -> Intent:
    rates = client.copy_rates(symbol, timeframe, 260)
    if rates is None or len(rates) < 140:
        return Intent(None, 0.0, 0.0, "no_data", {})

    df = pd.DataFrame(rates)
    lookback = 40
    touch_tol_mult = 0.2

    high_zone = float(df["high"].rolling(lookback).max().iloc[-2])
    low_zone = float(df["low"].rolling(lookback).min().iloc[-2])
    atr = (
        pd.concat(
            [
                (df["high"] - df["low"]),
                (df["high"] - df["close"].shift(1)).abs(),
                (df["low"] - df["close"].shift(1)).abs(),
            ],
            axis=1,
        )
        .max(axis=1)
        .rolling(14)
        .mean()
        .iloc[-2]
    )

    c0 = df.iloc[-2]

    close = float(c0["close"])
    open_ = float(c0["open"])
    high = float(c0["high"])
    low = float(c0["low"])

    body = abs(close - open_)
    range_ = high - low

    if range_ == 0 or pd.isna(atr) or atr <= 0:
        return Intent(None, 0.0, 0.0, "invalid_candle_or_atr", {})

    range_width = high_zone - low_zone
    atr_ratio = range_width / atr if atr > 0 else 0.0
    touch_tol = atr * touch_tol_mult
    recent = df.iloc[-(lookback + 2):-2]
    top_touches = int((recent["high"] >= (high_zone - touch_tol)).sum())
    bottom_touches = int((recent["low"] <= (low_zone + touch_tol)).sum())

    debug = {
        "high_zone": round(high_zone, 5),
        "low_zone": round(low_zone, 5),
        "atr": round(float(atr), 5),
        "range_width": round(float(range_width), 5),
        "atr_ratio": round(float(atr_ratio), 3),
        "top_touches": top_touches,
        "bottom_touches": bottom_touches,
    }

    if atr_ratio < 1.6 or atr_ratio > 6.0:
        return Intent(None, close, 0.0, "range_width_invalid", debug)

    if top_touches < 2 and bottom_touches < 2:
        return Intent(None, close, 0.0, "range_not_confirmed", debug)

    # filter weak candles
    if body < (range_ * 0.2):
        return Intent(None, close, 0.0, "weak_candle", debug)

    wick_top = high - max(close, open_)
    wick_bottom = min(close, open_) - low

    near_top = high >= (high_zone - touch_tol)
    near_bottom = low <= (low_zone + touch_tol)

    bearish_rejection = near_top and wick_top > body * 1.5 and close < open_
    bullish_rejection = near_bottom and wick_bottom > body * 1.5 and close > open_

    if bearish_rejection:
        sl = high + (range_ * 0.5)
        debug["trigger"] = "bearish_rejection"
        return Intent("short", close, sl, "RANGE_SHORT", debug)

    if bullish_rejection:
        sl = low - (range_ * 0.5)
        debug["trigger"] = "bullish_rejection"
        return Intent("long", close, sl, "RANGE_LONG", debug)

    return Intent(None, close, 0.0, "no_signal", debug)
