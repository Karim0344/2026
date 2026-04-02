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
    rates = client.copy_rates(symbol, timeframe, 200)
    if rates is None or len(rates) < 100:
        return Intent(None, 0.0, 0.0, "no_data", {})

    df = pd.DataFrame(rates)

    high_zone = float(df["high"].rolling(20).max().iloc[-2])
    low_zone = float(df["low"].rolling(20).min().iloc[-2])

    c0 = df.iloc[-2]

    close = float(c0["close"])
    open_ = float(c0["open"])
    high = float(c0["high"])
    low = float(c0["low"])

    body = abs(close - open_)
    range_ = high - low

    if range_ == 0:
        return Intent(None, 0.0, 0.0, "zero_range", {})

    # filter weak candles
    if body < (range_ * 0.2):
        return Intent(None, close, 0.0, "weak_candle", {})

    wick_top = high - max(close, open_)
    wick_bottom = min(close, open_) - low

    near_top = high >= high_zone * 0.999
    near_bottom = low <= low_zone * 1.001

    bearish_rejection = near_top and wick_top > body * 1.5 and close < open_
    bullish_rejection = near_bottom and wick_bottom > body * 1.5 and close > open_

    if bearish_rejection:
        sl = high + (range_ * 0.5)
        return Intent("short", close, sl, "RANGE_SHORT", {})

    if bullish_rejection:
        sl = low - (range_ * 0.5)
        return Intent("long", close, sl, "RANGE_LONG", {})

    return Intent(None, close, 0.0, "no_signal", {})
