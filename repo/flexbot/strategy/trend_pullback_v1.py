import logging
from dataclasses import dataclass
from typing import Optional
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
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1
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
    require_breakout: bool,
    last_closed_bar_time: int,
) -> TradeIntent:
    # Need enough bars
    bars = max(ma_trend + 5, swing_lookback + 5, atr_period + 5, rsi_period + 5, 300)
    rates = client.copy_rates(symbol, timeframe, bars)
    if rates is None or len(rates) < (ma_trend + 5):
        return TradeIntent(valid=False, reason="not_enough_rates")

    df = pd.DataFrame(rates)
    # df columns: time, open, high, low, close, tick_volume, spread, real_volume
    # We act on closed candle [1] => df.iloc[-2]
    if int(df["time"].iloc[-2]) == int(last_closed_bar_time):
        return TradeIntent(valid=False, reason="same_bar")

    df["ma_fast"] = _sma(df["close"], ma_fast)
    df["ma_trend"] = _sma(df["close"], ma_trend)
    df["rsi"] = _rsi(df["close"], rsi_period)
    df["atr"] = _atr(df, atr_period)

    c0 = df.iloc[-2]  # closed bar
    c1 = df.iloc[-3]  # previous closed bar

    ma50 = float(c0["ma_fast"])
    ma200 = float(c0["ma_trend"])
    rsi = float(c0["rsi"])
    atr = float(c0["atr"])

    if np.isnan(ma50) or np.isnan(ma200) or np.isnan(rsi) or np.isnan(atr) or atr <= 0:
        return TradeIntent(valid=False, reason="indicator_nan")

    pull_dist = pullback_atr_mult * atr

    close0 = float(c0["close"])
    high1 = float(c1["high"])
    low1 = float(c1["low"])

    # mechanical SL from last N lows/highs on closed bars
    look = df.iloc[-(swing_lookback + 1) : -1]  # exclude forming bar
    lowest_low = float(look["low"].min())
    highest_high = float(look["high"].max())

    dist_ma = abs(close0 - ma50)
    trend_ok_long = close0 > ma200
    pullback_ok_long = dist_ma <= pull_dist
    rsi_ok_long = rsi < rsi_long_max
    breakout_ok_long = close0 > high1
    long_ok = (
        trend_ok_long
        and pullback_ok_long
        and rsi_ok_long
        and (breakout_ok_long or not require_breakout)
    )

    trend_ok_short = close0 < ma200
    pullback_ok_short = dist_ma <= pull_dist
    rsi_ok_short = rsi > rsi_short_min
    breakout_ok_short = close0 < low1
    short_ok = (
        trend_ok_short
        and pullback_ok_short
        and rsi_ok_short
        and (breakout_ok_short or not require_breakout)
    )

    logging.info(
        "BAR_DEBUG symbol=%s tf=%s close=%.5f ma_fast=%.5f ma_trend=%.5f rsi=%.2f atr=%.5f dist_ma=%.5f pull_dist=%.5f high1=%.5f low1=%.5f trend_ok_long=%s pullback_ok_long=%s rsi_ok_long=%s breakout_ok_long=%s trend_ok_short=%s pullback_ok_short=%s rsi_ok_short=%s breakout_ok_short=%s require_breakout=%s",
        symbol,
        timeframe,
        close0,
        ma50,
        ma200,
        rsi,
        atr,
        dist_ma,
        pull_dist,
        high1,
        low1,
        trend_ok_long,
        pullback_ok_long,
        rsi_ok_long,
        breakout_ok_long,
        trend_ok_short,
        pullback_ok_short,
        rsi_ok_short,
        breakout_ok_short,
        require_breakout,
    )

    bar_time = int(c0["time"])
    batch_id = f"{symbol}_{timeframe}_{bar_time}"

    if long_ok:
        sl = lowest_low - (sl_atr_buffer_mult * atr)
        return TradeIntent(
            valid=True,
            is_long=True,
            sl=float(sl),
            entry=0.0,
            batch_id=batch_id,
            reason="trend_pullback_long",
        )
    if short_ok:
        sl = highest_high + (sl_atr_buffer_mult * atr)
        return TradeIntent(
            valid=True,
            is_long=False,
            sl=float(sl),
            entry=0.0,
            batch_id=batch_id,
            reason="trend_pullback_short",
        )

    if not trend_ok_long and not trend_ok_short:
        return TradeIntent(valid=False, reason="trend_fail")
    if trend_ok_long and not pullback_ok_long:
        return TradeIntent(valid=False, reason="pullback_fail_long")
    if trend_ok_long and pullback_ok_long and not rsi_ok_long:
        return TradeIntent(valid=False, reason="rsi_fail_long")
    if (
        trend_ok_long
        and pullback_ok_long
        and rsi_ok_long
        and require_breakout
        and not breakout_ok_long
    ):
        return TradeIntent(valid=False, reason="breakout_fail_long")
    if trend_ok_short and not pullback_ok_short:
        return TradeIntent(valid=False, reason="pullback_fail_short")
    if trend_ok_short and pullback_ok_short and not rsi_ok_short:
        return TradeIntent(valid=False, reason="rsi_fail_short")
    if (
        trend_ok_short
        and pullback_ok_short
        and rsi_ok_short
        and require_breakout
        and not breakout_ok_short
    ):
        return TradeIntent(valid=False, reason="breakout_fail_short")
    return TradeIntent(valid=False, reason="no_signal")
