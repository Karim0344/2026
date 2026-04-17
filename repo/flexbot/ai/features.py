from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _session_from_hour(hour: int) -> str:
    if hour < 7:
        return "Asia"
    if hour < 13:
        return "London"
    if hour < 17:
        return "London/NY_overlap"
    return "New_York"


def build_feature_snapshot(
    *,
    signal_reason: str,
    intent_debug: dict[str, Any] | None,
    spread_points: int,
    max_spread_points: int,
    regime: str = "",
    strategy_name: str = "",
    side: str = "",
) -> dict[str, Any]:
    debug = intent_debug or {}

    trend_ok_long = bool(debug.get("trend_ok_long", False))
    trend_ok_short = bool(debug.get("trend_ok_short", False))
    htf_ok_long = bool(debug.get("htf_ok_long", False))
    htf_ok_short = bool(debug.get("htf_ok_short", False))
    pullback_ok_long = bool(debug.get("pullback_ok_long", False))
    pullback_ok_short = bool(debug.get("pullback_ok_short", False))
    bullish_close = bool(debug.get("bullish_close", False))
    bearish_close = bool(debug.get("bearish_close", False))
    breakout_ok_long = bool(debug.get("breakout_ok_long", False))
    breakout_ok_short = bool(debug.get("breakout_ok_short", False))

    bar_time = int(debug.get("bar_time", 0) or 0)
    dt = datetime.fromtimestamp(bar_time, tz=timezone.utc) if bar_time else datetime.now(timezone.utc)
    session_name = str(debug.get("session", "") or _session_from_hour(dt.hour))

    return {
        "signal_reason": signal_reason,
        "bar_time": bar_time,
        "symbol": str(debug.get("symbol", "")),
        "timeframe": str(debug.get("timeframe", "")),
        "strategy_name": strategy_name,
        "side": side,
        "weekday": dt.weekday(),
        "hour": dt.hour,
        "session_name": session_name,
        "trend_ok_long": trend_ok_long,
        "trend_ok_short": trend_ok_short,
        "htf_ok_long": htf_ok_long,
        "htf_ok_short": htf_ok_short,
        "pullback_ok_long": pullback_ok_long,
        "pullback_ok_short": pullback_ok_short,
        "bullish_close": bullish_close,
        "bearish_close": bearish_close,
        "breakout_ok_long": breakout_ok_long,
        "breakout_ok_short": breakout_ok_short,
        "trend_ok": bool(debug.get("trend_ok", trend_ok_long or trend_ok_short)),
        "htf_ok": bool(debug.get("htf_ok", htf_ok_long or htf_ok_short)),
        "pullback": bool(debug.get("pullback", pullback_ok_long or pullback_ok_short)),
        "momentum": bool(debug.get("momentum", bullish_close or bearish_close)),
        "breakout": bool(debug.get("breakout", breakout_ok_long or breakout_ok_short)),
        "regime": str(regime),
        "body_size": float(debug.get("body_size", 0.0) or 0.0),
        "wick_ratio": float(debug.get("wick_ratio", 0.0) or 0.0),
        "compression_flag": bool(debug.get("compression_flag", False)),
        "breakout_pressure_up": bool(debug.get("breakout_pressure_up", False)),
        "breakout_pressure_down": bool(debug.get("breakout_pressure_down", False)),
        "three_candle_breakout": bool(debug.get("three_candle_breakout", False)),
        "three_candle_reversal": bool(debug.get("three_candle_reversal", False)),
        "rising_lows": bool(debug.get("rising_lows", False)),
        "falling_highs": bool(debug.get("falling_highs", False)),
        "mid_range_flag": bool(debug.get("mid_range_candle", False)),
        "session": str(debug.get("session", "")),
        "spread_points": int(spread_points),
        "max_spread_points": int(max_spread_points),
    }
