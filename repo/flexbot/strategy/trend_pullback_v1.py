import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from flexbot.mt5 import client


@dataclass
class TradeIntent:
    valid: bool
    is_long: bool = False
    sl: float = 0.0
    entry: float = 0.0
    batch_id: str = ""
    reason: str = ""
    debug: dict[str, Any] = field(default_factory=dict)


def _sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period).mean()


def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _atr(df: pd.DataFrame, period: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def get_intent(
    symbol: str,
    timeframe: str,
    ma_fast: int,
    ma_trend: int,
    rsi_period: int,
    atr_period: int,
    pullback_atr_mult: float,
    rsi_long_max: float,
    rsi_short_min: float,
    swing_lookback: int,
    sl_atr_buffer_mult: float,
    last_closed_bar_time: int,
    require_breakout: bool = True,
) -> TradeIntent:
    bars = max(ma_trend + 10, swing_lookback + 10, atr_period + 10, rsi_period + 10, 300)
    rates = client.copy_rates(symbol, timeframe, bars)
    if rates is None or len(rates) < (ma_trend + 10):
        return TradeIntent(valid=False, reason="not_enough_rates")

    df = pd.DataFrame(rates)
    if int(df["time"].iloc[-2]) == int(last_closed_bar_time):
        return TradeIntent(valid=False, reason="same_bar")

    df["ma_fast"] = _sma(df["close"], ma_fast)
    df["ma_trend"] = _sma(df["close"], ma_trend)
    df["rsi"] = _rsi(df["close"], rsi_period)
    df["atr"] = _atr(df, atr_period)

    c0 = df.iloc[-2]
    c1 = df.iloc[-3]

    ma_fast_v = float(c0["ma_fast"])
    ma_trend_v = float(c0["ma_trend"])
    rsi_v = float(c0["rsi"])
    atr_v = float(c0["atr"])

    if (
        np.isnan(ma_fast_v)
        or np.isnan(ma_trend_v)
        or np.isnan(rsi_v)
        or np.isnan(atr_v)
        or atr_v <= 0
    ):
        return TradeIntent(valid=False, reason="indicator_nan")

    open0 = float(c0["open"])
    close0 = float(c0["close"])
    high0 = float(c0["high"])
    low0 = float(c0["low"])

    high1 = float(c1["high"])
    low1 = float(c1["low"])

    pull_dist = pullback_atr_mult * atr_v
    dist_ma = abs(close0 - ma_fast_v)

    long_zone_touch = low0 <= (ma_fast_v + pull_dist)
    short_zone_touch = high0 >= (ma_fast_v - pull_dist)

    trend_ok_long = close0 > ma_trend_v and ma_fast_v > ma_trend_v
    trend_ok_short = close0 < ma_trend_v and ma_fast_v < ma_trend_v

    pullback_ok_long = long_zone_touch
    pullback_ok_short = short_zone_touch

    bullish_close = close0 > open0
    bearish_close = close0 < open0

    breakout_ok_long = close0 > high1
    breakout_ok_short = close0 < low1

    rsi_ok_long = 30.0 < rsi_v < rsi_long_max
    rsi_ok_short = rsi_short_min < rsi_v < 70.0

    long_ok = (
        trend_ok_long
        and pullback_ok_long
        and bullish_close
        and rsi_ok_long
        and (breakout_ok_long if require_breakout else True)
    )
    short_ok = (
        trend_ok_short
        and pullback_ok_short
        and bearish_close
        and rsi_ok_short
        and (breakout_ok_short if require_breakout else True)
    )

    look = df.iloc[-(swing_lookback + 1): -1]
    lowest_low = float(look["low"].min())
    highest_high = float(look["high"].max())

    bar_time = int(c0["time"])
    batch_id = f"{symbol}_{timeframe}_{bar_time}"

    debug = {
        "symbol": symbol,
        "timeframe": timeframe,
        "bar_time": bar_time,
        "open": round(open0, 5),
        "close": round(close0, 5),
        "high": round(high0, 5),
        "low": round(low0, 5),
        "prev_high": round(high1, 5),
        "prev_low": round(low1, 5),
        "ma_fast": round(ma_fast_v, 5),
        "ma_trend": round(ma_trend_v, 5),
        "rsi": round(rsi_v, 2),
        "atr": round(atr_v, 5),
        "dist_ma": round(dist_ma, 5),
        "pull_dist": round(pull_dist, 5),
        "trend_ok_long": trend_ok_long,
        "trend_ok_short": trend_ok_short,
        "pullback_ok_long": pullback_ok_long,
        "pullback_ok_short": pullback_ok_short,
        "bullish_close": bullish_close,
        "bearish_close": bearish_close,
        "rsi_ok_long": rsi_ok_long,
        "rsi_ok_short": rsi_ok_short,
        "breakout_ok_long": breakout_ok_long,
        "breakout_ok_short": breakout_ok_short,
        "require_breakout": require_breakout,
    }

    logging.info("BAR_DEBUG %s", debug)

    if long_ok:
        sl = lowest_low - (sl_atr_buffer_mult * atr_v)
        return TradeIntent(
            valid=True,
            is_long=True,
            sl=float(sl),
            batch_id=batch_id,
            reason="pro_trend_pullback_long",
            debug=debug,
        )

    if short_ok:
        sl = highest_high + (sl_atr_buffer_mult * atr_v)
        return TradeIntent(
            valid=True,
            is_long=False,
            sl=float(sl),
            batch_id=batch_id,
            reason="pro_trend_pullback_short",
            debug=debug,
        )

    if trend_ok_long:
        if not pullback_ok_long:
            return TradeIntent(False, reason="pullback_fail_long", debug=debug)
        if not bullish_close:
            return TradeIntent(False, reason="confirm_fail_long", debug=debug)
        if not rsi_ok_long:
            return TradeIntent(False, reason="rsi_fail_long", debug=debug)
        if require_breakout and not breakout_ok_long:
            return TradeIntent(False, reason="breakout_fail_long", debug=debug)

    if trend_ok_short:
        if not pullback_ok_short:
            return TradeIntent(False, reason="pullback_fail_short", debug=debug)
        if not bearish_close:
            return TradeIntent(False, reason="confirm_fail_short", debug=debug)
        if not rsi_ok_short:
            return TradeIntent(False, reason="rsi_fail_short", debug=debug)
        if require_breakout and not breakout_ok_short:
            return TradeIntent(False, reason="breakout_fail_short", debug=debug)

    if not trend_ok_long and not trend_ok_short:
        return TradeIntent(False, reason="trend_fail", debug=debug)

    return TradeIntent(False, reason="no_signal", debug=debug)
