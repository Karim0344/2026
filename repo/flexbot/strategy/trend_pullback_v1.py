from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from flexbot.mt5 import client


@dataclass
class TradeIntent:
    valid: bool
    is_long: bool = False
    entry: float = 0.0
    sl: float = 0.0
    batch_id: str = ""
    reason: str = ""
    debug: dict[str, Any] = field(default_factory=dict)


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


def _htf_trend_ok(symbol: str, htf: str, ma_trend: int, is_long: bool) -> bool:
    rates = client.copy_rates(symbol, htf, max(ma_trend + 5, 200))
    if rates is None or len(rates) < ma_trend + 2:
        return False

    df = pd.DataFrame(rates)
    df["ma"] = _sma(df["close"], ma_trend)

    c0 = df.iloc[-2]
    if np.isnan(c0["ma"]):
        return False

    return bool(c0["close"] > c0["ma"]) if is_long else bool(c0["close"] < c0["ma"])


def get_intent(symbol: str, timeframe: str, cfg, last_closed_bar_time: int) -> TradeIntent:
    rates = client.copy_rates(symbol, timeframe, 200)
    if rates is None or len(rates) < 100:
        return TradeIntent(False, reason="no_data")

    df = pd.DataFrame(rates)
    c0 = df.iloc[-2]
    if int(c0["time"]) == int(last_closed_bar_time):
        return TradeIntent(False, reason="same_bar")

    df["ma_fast"] = _sma(df["close"], cfg.ma_fast)
    df["ma_trend"] = _sma(df["close"], cfg.ma_trend)
    df["atr"] = _atr(df, cfg.atr_period)

    c0 = df.iloc[-2]
    c1 = df.iloc[-3]

    close = float(c0["close"])
    open_ = float(c0["open"])
    high = float(c0["high"])
    low = float(c0["low"])

    ma_fast = float(c0["ma_fast"])
    ma_trend = float(c0["ma_trend"])
    atr = float(c0["atr"])

    if np.isnan(ma_fast) or np.isnan(ma_trend) or np.isnan(atr):
        return TradeIntent(False, entry=close, reason="nan")

    trend_long = close > ma_trend
    trend_short = close < ma_trend

    htf_long = _htf_trend_ok(symbol, "H1", cfg.ma_trend, True)
    htf_short = _htf_trend_ok(symbol, "H1", cfg.ma_trend, False)

    pullback_long = low <= ma_fast + atr * 0.2
    pullback_short = high >= ma_fast - atr * 0.2

    bullish = close > open_
    bearish = close < open_

    breakout_long = high > float(c1["high"])
    breakout_short = low < float(c1["low"])

    entry = close

    sl_long = low - atr * 1.2
    sl_short = high + atr * 1.2

    candle_range = max(high - low, 1e-9)
    body_size = abs(close - open_)
    upper_wick = max(high - max(open_, close), 0.0)
    lower_wick = max(min(open_, close) - low, 0.0)
    wick_ratio = (upper_wick + lower_wick) / candle_range

    session = "london_ny" if 7 <= client.broker_datetime_utc(symbol).hour <= 20 else "off_session"

    debug = {
        "symbol": symbol,
        "timeframe": timeframe,
        "bar_time": int(c0["time"]),
        "trend_ok_long": trend_long,
        "trend_ok_short": trend_short,
        "htf_ok_long": htf_long,
        "htf_ok_short": htf_short,
        "pullback_ok_long": pullback_long,
        "pullback_ok_short": pullback_short,
        "bullish_close": bullish,
        "bearish_close": bearish,
        "breakout_ok_long": breakout_long,
        "breakout_ok_short": breakout_short,
        "trend_ok": trend_long or trend_short,
        "htf_ok": htf_long or htf_short,
        "pullback": pullback_long or pullback_short,
        "momentum": bullish or bearish,
        "breakout": breakout_long or breakout_short,
        "body_size": round(body_size, 6),
        "wick_ratio": round(wick_ratio, 6),
        "session": session,
    }

    long_ok = all([trend_long, htf_long, pullback_long, bullish, breakout_long])
    short_ok = all([trend_short, htf_short, pullback_short, bearish, breakout_short])

    batch_id = f"{symbol}_{timeframe}_{int(c0['time'])}"
    if long_ok:
        return TradeIntent(True, True, entry=entry, sl=sl_long, batch_id=batch_id, reason="PRO_LONG", debug=debug)

    if short_ok:
        return TradeIntent(True, False, entry=entry, sl=sl_short, batch_id=batch_id, reason="PRO_SHORT", debug=debug)

    return TradeIntent(False, entry=entry, batch_id=batch_id, reason="no_signal", debug=debug)
