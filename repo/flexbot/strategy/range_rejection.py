from dataclasses import dataclass
from typing import Optional

import pandas as pd
import numpy as np

from flexbot.mt5 import client
from flexbot.ai.session_utils import normalize_session_name
from flexbot.strategy.range_features import compute_range_features


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
    max_atr_ratio = float(getattr(cfg, "range_max_atr_ratio", 20.0))
    atr_ratio_window = int(getattr(cfg, "range_atr_ratio_window", 240))
    max_quantile = float(getattr(cfg, "range_atr_ratio_max_quantile", 0.95))
    max_buffer = float(getattr(cfg, "range_atr_ratio_max_buffer", 1.1))
    min_quantile = float(getattr(cfg, "range_atr_ratio_min_quantile", 0.50))
    min_floor = float(getattr(cfg, "range_atr_ratio_min_floor", 0.50))
    max_atr_ratio_percentile = float(getattr(cfg, "range_max_atr_ratio_percentile", 0.95))
    atr_ratio_percentile_window = max(int(getattr(cfg, "range_atr_ratio_percentile_window", 240)), 30)
    required_touches = int(getattr(cfg, "range_required_touches", 1))
    mid_low = float(getattr(cfg, "range_mid_low", 0.35))
    mid_high = float(getattr(cfg, "range_mid_high", 0.65))
    weak_body_min = float(getattr(cfg, "range_weak_body_min", 0.15))
    break_buffer_mult = float(getattr(cfg, "range_break_buffer_mult", 0.1))
    wick_body_min = float(getattr(cfg, "range_wick_body_min", 1.15))

    df = compute_range_features(df, cfg)
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

    bar_time = int(c0["time"])
    try:
        session_hour = client.broker_datetime_utc(symbol).hour
    except Exception:
        session_hour = pd.to_datetime(c0["time"], unit="s", utc=True).hour
    session = normalize_session_name(session_hour)

    close = float(c0["close"])
    open_ = float(c0["open"])
    high = float(c0["high"])
    low = float(c0["low"])

    body = abs(close - open_)
    range_ = high - low

    if range_ == 0 or pd.isna(atr) or atr <= 0:
        return Intent(None, 0.0, 0.0, "invalid_candle_or_atr", {})
    if atr < float(getattr(cfg, "range_dead_atr_threshold", 0.0)):
        return Intent(None, close, 0.0, "dead", {"regime": "dead", "atr": round(float(atr), 5)})

    range_width = high_zone - low_zone
    atr_ratio = range_width / atr if atr > 0 else 0.0

    atr_series = (
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
    )
    hist_high = df["high"].shift(3).rolling(lookback).max()
    hist_low = df["low"].shift(3).rolling(lookback).min()
    atr_ratio_hist = ((hist_high - hist_low) / atr_series.replace(0.0, float("nan"))).dropna()
    if atr_ratio_window > 0:
        atr_ratio_hist = atr_ratio_hist.iloc[-atr_ratio_window:]
    if not atr_ratio_hist.empty:
        min_quantile = min(max(min_quantile, 0.0), 1.0)
        max_quantile = min(max(max_quantile, 0.0), 1.0)
        p50 = float(atr_ratio_hist.quantile(0.50))
        p75 = float(atr_ratio_hist.quantile(0.75))
        p90 = float(atr_ratio_hist.quantile(0.90))
        p95 = float(atr_ratio_hist.quantile(0.95))
        min_from_dist = float(atr_ratio_hist.quantile(min_quantile)) * 0.60
        max_from_dist = float(atr_ratio_hist.quantile(max_quantile)) * max(max_buffer, 1.0)
        dynamic_min = max(min_floor, min(min_from_dist, max_from_dist - 1e-6))
        dynamic_max = max(max_from_dist, dynamic_min + 1e-6)
    else:
        p50 = p75 = p90 = p95 = 0.0
        dynamic_min = min_atr_ratio
        dynamic_max = max_atr_ratio
    ratio_series = (
        (df["high"].rolling(lookback).max() - df["low"].rolling(lookback).min())
        / (
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
        )
    ).replace([np.inf, -np.inf], np.nan).dropna()
    ratio_tail = ratio_series.tail(atr_ratio_percentile_window)
    dynamic_max_atr_ratio = max_atr_ratio
    if not ratio_tail.empty:
        clipped_pct = min(max(max_atr_ratio_percentile, 0.50), 0.995)
        dynamic_max_atr_ratio = float(np.percentile(ratio_tail.to_numpy(dtype=float), clipped_pct * 100.0))
    effective_max_atr_ratio = max(min_atr_ratio, min(max_atr_ratio, dynamic_max_atr_ratio))
    touch_tol = atr * touch_tol_mult
    recent = df.iloc[-(lookback + 2):-2]
    top_touches = int((recent["high"] >= (high_zone - touch_tol)).sum())
    bottom_touches = int((recent["low"] <= (low_zone + touch_tol)).sum())

    wick_top = high - max(close, open_)
    wick_bottom = min(close, open_) - low
    body_size = body
    wick_ratio = (wick_top + wick_bottom) / max(body_size, 1e-9)
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
    middle_override_top = fake_break_top and (reclaim_from_top or reclaim_from_top_strict)
    middle_override_bottom = fake_break_bottom and (reclaim_from_bottom or reclaim_from_bottom_strict)

    debug = {
        "symbol": symbol,
        "timeframe": timeframe,
        "bar_time": bar_time,
        "session": session,
        "body_size": round(float(body_size), 5),
        "wick_ratio": round(float(wick_ratio), 6),
        "compression_flag": bool(range_width <= (atr * 2.0)),
        "breakout_pressure_up": bool(rising_lows := ((float(c0["low"]) > float(c1["low"])) and (float(c1["low"]) > float(df.iloc[-4]["low"])))),
        "breakout_pressure_down": bool(falling_highs := ((float(c0["high"]) < float(c1["high"])) and (float(c1["high"]) < float(df.iloc[-4]["high"])))),
        "rising_lows": bool(rising_lows),
        "falling_highs": bool(falling_highs),
        "three_candle_breakout": bool(close > max(float(c1["high"]), float(df.iloc[-4]["high"]))),
        "three_candle_reversal": bool((close > open_) and (float(c1["close"]) < float(c1["open"])) and (float(df.iloc[-4]["close"]) < float(df.iloc[-4]["open"]))),
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
        "middle_override_top": middle_override_top,
        "middle_override_bottom": middle_override_bottom,
        "range_min_atr_ratio": round(min_atr_ratio, 3),
        "range_max_atr_ratio": round(max_atr_ratio, 3),
        "range_min_atr_ratio_dynamic": round(dynamic_min, 3),
        "range_max_atr_ratio_dynamic": round(dynamic_max, 3),
        "atr_ratio_p50": round(p50, 3),
        "atr_ratio_p75": round(p75, 3),
        "atr_ratio_p90": round(p90, 3),
        "atr_ratio_p95": round(p95, 3),
        "range_max_atr_ratio_percentile": round(max_atr_ratio_percentile, 3),
        "range_atr_ratio_percentile_window": atr_ratio_percentile_window,
        "range_max_atr_ratio_dynamic": round(dynamic_max_atr_ratio, 3),
        "range_max_atr_ratio_effective": round(effective_max_atr_ratio, 3),
        "required_touches": required_touches,
        "mid_low": round(mid_low, 3),
        "mid_high": round(mid_high, 3),
        "weak_body_min": round(weak_body_min, 3),
        "break_buffer_mult": round(break_buffer_mult, 3),
        "wick_body_min": round(wick_body_min, 3),
    }

    if atr_ratio < dynamic_min or atr_ratio > dynamic_max:
        debug["min_required"] = round(dynamic_min, 3)
        debug["max_allowed"] = round(dynamic_max, 3)
    if atr_ratio < min_atr_ratio or atr_ratio > effective_max_atr_ratio:
        debug["min_required"] = round(min_atr_ratio, 3)
        debug["max_allowed"] = round(effective_max_atr_ratio, 3)
        return Intent(None, close, 0.0, "range_width_invalid", debug)

    if top_touches < required_touches or bottom_touches < required_touches:
        debug["required_top_touches"] = required_touches
        debug["required_bottom_touches"] = required_touches
        return Intent(None, close, 0.0, "range_not_confirmed", debug)

    if not (near_top or near_bottom or fake_break_top or fake_break_bottom):
        return Intent(None, close, 0.0, "range_idle", {
            "close_pos": round(float(close_pos), 4),
            "range_width": round(float(range_width), 5),
            "atr_ratio": round(float(atr_ratio), 3),
        })

    if in_middle and not (middle_override_top or middle_override_bottom):
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
