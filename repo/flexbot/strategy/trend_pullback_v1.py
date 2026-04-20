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

    pullback_long = low <= ma_fast + atr * 0.35
    pullback_short = high >= ma_fast - atr * 0.35

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

    trend_score_long = 0
    trend_score_short = 0

    if trend_long:
        trend_score_long += 25
    if trend_short:
        trend_score_short += 25

    if htf_long:
        trend_score_long += 15
    elif trend_long:
        trend_score_long += 5
    if htf_short:
        trend_score_short += 15
    elif trend_short:
        trend_score_short += 5

    if pullback_long:
        trend_score_long += 20
    if pullback_short:
        trend_score_short += 20

    if bullish:
        trend_score_long += 20
    if bearish:
        trend_score_short += 20

    if breakout_long:
        trend_score_long += 20
    if breakout_short:
        trend_score_short += 20

    min_score = int(getattr(cfg, "trend_min_score", 65))
    paper_mode = bool(getattr(cfg, "paper_mode", False))
    paper_relax = int(getattr(cfg, "paper_trend_score_relax", 0)) if paper_mode else 0
    effective_min_score = max(min_score - max(paper_relax, 0), 0)
    require_breakout = bool(getattr(cfg, "require_breakout", False))
    allow_short = bool(getattr(cfg, "trend_allow_short", False))
    short_extra_score = max(int(getattr(cfg, "trend_short_extra_score", 0)), 0)
    short_min_score = effective_min_score + short_extra_score
    allow_paper_near = bool(getattr(cfg, "paper_allow_near_signals", False))
    paper_near_extra_score = max(int(getattr(cfg, "paper_near_extra_score", 0)), 0)
    near_min_score = effective_min_score + paper_near_extra_score

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
        "require_breakout": require_breakout,
        "trend_score_long": trend_score_long,
        "trend_score_short": trend_score_short,
        "trend_min_score": min_score,
        "effective_min_score": effective_min_score,
        "paper_mode": paper_mode,
        "paper_trend_score_relax": paper_relax,
        "paper_allow_near_signals": allow_paper_near,
        "paper_near_extra_score": paper_near_extra_score,
        "trend_allow_short": allow_short,
        "trend_short_extra_score": short_extra_score,
        "trend_short_min_score": short_min_score,
        "near_min_score": near_min_score,
        "long_score_gap": int(trend_score_long - min_score),
        "short_score_gap": int(trend_score_short - min_score),
        "body_size": round(body_size, 6),
        "wick_ratio": round(wick_ratio, 6),
        "session": session,
    }

    long_ok = trend_long and pullback_long and bullish and trend_score_long >= effective_min_score
    short_ok = allow_short and trend_short and pullback_short and bearish and trend_score_short >= short_min_score

    near_long_ok = (
        paper_mode
        and allow_paper_near
        and trend_long
        and trend_score_long >= near_min_score
        and pullback_long
        and bullish
    )
    near_short_ok = (
        paper_mode
        and allow_paper_near
        and allow_short
        and trend_short
        and trend_score_short >= max(short_min_score, near_min_score)
        and pullback_short
        and bearish
    )

    if require_breakout:
        long_ok = long_ok and breakout_long
        short_ok = short_ok and breakout_short
        near_long_ok = near_long_ok and breakout_long
        near_short_ok = near_short_ok and breakout_short

    batch_id = f"{symbol}_{timeframe}_{int(c0['time'])}"
    if long_ok:
        if trend_score_long < min_score and effective_min_score < min_score:
            debug["paper_relaxed_entry"] = True
            return TradeIntent(True, True, entry=entry, sl=sl_long, batch_id=batch_id, reason="PRO_LONG_PAPER", debug=debug)
        return TradeIntent(True, True, entry=entry, sl=sl_long, batch_id=batch_id, reason="PRO_LONG", debug=debug)

    if short_ok:
        if trend_score_short < min_score and effective_min_score < min_score:
            debug["paper_relaxed_entry"] = True
            return TradeIntent(True, False, entry=entry, sl=sl_short, batch_id=batch_id, reason="PRO_SHORT_PAPER", debug=debug)
        return TradeIntent(True, False, entry=entry, sl=sl_short, batch_id=batch_id, reason="PRO_SHORT", debug=debug)

    if near_long_ok:
        debug["paper_near_signal_entry"] = True
        return TradeIntent(True, True, entry=entry, sl=sl_long, batch_id=batch_id, reason="PRO_LONG_PAPER_NEAR", debug=debug)

    if near_short_ok:
        debug["paper_near_signal_entry"] = True
        return TradeIntent(True, False, entry=entry, sl=sl_short, batch_id=batch_id, reason="PRO_SHORT_PAPER_NEAR", debug=debug)

    fail_reason = "trend_fail"
    if trend_score_long >= max(min_score - 10, 0) or trend_score_short >= max(min_score - 10, 0):
        fail_reason = "trend_near_signal"
    return TradeIntent(False, entry=entry, batch_id=batch_id, reason=fail_reason, debug=debug)
