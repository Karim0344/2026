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
    lookback = int(getattr(cfg, "range_lookback", 60))
    touch_tol_mult = float(getattr(cfg, "range_touch_tol_mult", 0.2))
    min_atr_ratio = float(getattr(cfg, "range_min_atr_ratio", 1.3))
    max_atr_ratio = float(getattr(cfg, "range_max_atr_ratio", 6.5))
    required_touches = int(getattr(cfg, "range_required_touches", 1))
    mid_low = float(getattr(cfg, "range_mid_low", 0.35))
    mid_high = float(getattr(cfg, "range_mid_high", 0.65))
    weak_body_min = float(getattr(cfg, "range_weak_body_min", 0.15))
    break_buffer_mult = float(getattr(cfg, "range_break_buffer_mult", 0.1))
    wick_body_min = float(getattr(cfg, "range_wick_body_min", 1.15))

    zone_slice = df.iloc[-(lookback + 3):-3]
    high_zone = float(zone_slice["high"].max())
    low_zone = float(zone_slice["low"].min())
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
    c1 = df.iloc[-3]

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

    wick_top = high - max(close, open_)
    wick_bottom = min(close, open_) - low
    zone_mid = (high_zone + low_zone) / 2.0
    close_pos = (close - low_zone) / max(range_width, 1e-9)
    near_top = high >= (high_zone - touch_tol)
    near_bottom = low <= (low_zone + touch_tol)
    break_buffer = atr * break_buffer_mult
    fake_break_top = high > (high_zone + break_buffer)
    fake_break_bottom = low < (low_zone - break_buffer)
    reclaim_from_top = close < high_zone
    reclaim_from_bottom = close > low_zone
    reclaim_from_top_strict = float(c1["close"]) <= high_zone
    reclaim_from_bottom_strict = float(c1["close"]) >= low_zone
    in_middle = mid_low <= close_pos <= mid_high

    debug = {
        "high_zone": round(high_zone, 5),
        "low_zone": round(low_zone, 5),
        "atr": round(float(atr), 5),
        "range_width": round(float(range_width), 5),
        "atr_ratio": round(float(atr_ratio), 3),
        "top_touches": top_touches,
        "bottom_touches": bottom_touches,
        "zone_mid": round(zone_mid, 5),
        "body": round(float(body), 5),
        "wick_top": round(float(wick_top), 5),
        "wick_bottom": round(float(wick_bottom), 5),
        "near_top": near_top,
        "near_bottom": near_bottom,
        "fake_break_top": fake_break_top,
        "fake_break_bottom": fake_break_bottom,
        "reclaim_top": reclaim_from_top,
        "reclaim_bottom": reclaim_from_bottom,
        "reclaim_top_strict": reclaim_from_top_strict,
        "reclaim_bottom_strict": reclaim_from_bottom_strict,
        "close_pos": round(float(close_pos), 4),
        "in_middle": in_middle,
        "range_min_atr_ratio": round(min_atr_ratio, 3),
        "range_max_atr_ratio": round(max_atr_ratio, 3),
        "required_touches": required_touches,
        "mid_low": round(mid_low, 3),
        "mid_high": round(mid_high, 3),
        "weak_body_min": round(weak_body_min, 3),
        "break_buffer_mult": round(break_buffer_mult, 3),
        "wick_body_min": round(wick_body_min, 3),
    }

    if atr_ratio < min_atr_ratio or atr_ratio > max_atr_ratio:
        debug["min_required"] = round(min_atr_ratio, 3)
        debug["max_allowed"] = round(max_atr_ratio, 3)
        return Intent(None, close, 0.0, "range_width_invalid", debug)

    if top_touches < required_touches or bottom_touches < required_touches:
        debug["required_top_touches"] = required_touches
        debug["required_bottom_touches"] = required_touches
        return Intent(None, close, 0.0, "range_not_confirmed", debug)

    if in_middle:
        debug["required_edge"] = f"<{mid_low:.2f} or >{mid_high:.2f}"
        debug["mid_block"] = {
            "close_pos": round(float(close_pos), 4),
            "fake_break_top": bool(fake_break_top),
            "fake_break_bottom": bool(fake_break_bottom),
            "reclaim_top": bool(reclaim_from_top),
            "reclaim_bottom": bool(reclaim_from_bottom),
        }
        return Intent(None, close, 0.0, "mid_range_candle", debug)

    body_min = range_ * weak_body_min
    debug["body_min_required"] = round(body_min, 5)
    if body < body_min:
        return Intent(None, close, 0.0, "weak_candle", debug)

    bearish_rejection = (
        near_top
        and fake_break_top
        and (reclaim_from_top or reclaim_from_top_strict)
        and wick_top > body * wick_body_min
        and close < open_
    )
    bullish_rejection = (
        near_bottom
        and fake_break_bottom
        and (reclaim_from_bottom or reclaim_from_bottom_strict)
        and wick_bottom > body * wick_body_min
        and close > open_
    )

    if bearish_rejection:
        sl = high + (range_ * 0.5)
        debug["trigger"] = "bearish_rejection"
        return Intent("short", close, sl, "RANGE_SHORT", debug)

    if bullish_rejection:
        sl = low - (range_ * 0.5)
        debug["trigger"] = "bullish_rejection"
        return Intent("long", close, sl, "RANGE_LONG", debug)

    return Intent(None, close, 0.0, "no_signal", debug)
