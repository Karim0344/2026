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
    symbol: str = "",
    timeframe: str = "",
    bar_time: int = 0,
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

    resolved_bar_time = int(debug.get("bar_time", 0) or bar_time or 0)
    dt = datetime.fromtimestamp(resolved_bar_time, tz=timezone.utc) if resolved_bar_time else datetime.now(timezone.utc)

    resolved_symbol = str(debug.get("symbol", "") or symbol)
    resolved_timeframe = str(debug.get("timeframe", "") or timeframe)
    session_name = str(debug.get("session_name", "") or debug.get("session", "") or _session_from_hour(dt.hour))

    body_size = float(debug.get("body_size", debug.get("body", 0.0)) or 0.0)
    wick_ratio = float(debug.get("wick_ratio", 0.0) or 0.0)
    if wick_ratio <= 0 and body_size > 0:
        wick_top = float(debug.get("wick_top", 0.0) or 0.0)
        wick_bottom = float(debug.get("wick_bottom", 0.0) or 0.0)
        wick_ratio = (wick_top + wick_bottom) / body_size

    normalized_side = str(side or "").lower()
    side_is_long = normalized_side == "long"
    side_is_short = normalized_side == "short"

    side_trend_ok = trend_ok_long if side_is_long else (trend_ok_short if side_is_short else (trend_ok_long or trend_ok_short))
    side_htf_ok = htf_ok_long if side_is_long else (htf_ok_short if side_is_short else (htf_ok_long or htf_ok_short))
    side_pullback_ok = (
        pullback_ok_long if side_is_long else (pullback_ok_short if side_is_short else (pullback_ok_long or pullback_ok_short))
    )
    side_momentum_ok = bullish_close if side_is_long else (bearish_close if side_is_short else (bullish_close or bearish_close))
    side_breakout_ok = breakout_ok_long if side_is_long else (breakout_ok_short if side_is_short else (breakout_ok_long or breakout_ok_short))

    return {
        "signal_reason": signal_reason,
        "bar_time": resolved_bar_time,
        "symbol": resolved_symbol,
        "timeframe": resolved_timeframe,
        "strategy_name": strategy_name,
        "side": normalized_side,
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
        "trend_ok": bool(side_trend_ok),
        "htf_ok": bool(side_htf_ok),
        "pullback": bool(side_pullback_ok),
        "momentum": bool(side_momentum_ok),
        "breakout": bool(side_breakout_ok),
        "feature_side_consistent": bool(
            (not side_is_long and not side_is_short)
            or (
                bool(debug.get("trend_ok", side_trend_ok)) == bool(side_trend_ok)
                and bool(debug.get("htf_ok", side_htf_ok)) == bool(side_htf_ok)
                and bool(debug.get("pullback", side_pullback_ok)) == bool(side_pullback_ok)
                and bool(debug.get("momentum", side_momentum_ok)) == bool(side_momentum_ok)
                and bool(debug.get("breakout", side_breakout_ok)) == bool(side_breakout_ok)
            )
        ),
        "regime": str(regime),
        "body_size": body_size,
        "wick_ratio": wick_ratio,
        "compression_flag": bool(debug.get("compression_flag", False)),
        "breakout_pressure_up": bool(debug.get("breakout_pressure_up", False)),
        "breakout_pressure_down": bool(debug.get("breakout_pressure_down", False)),
        "three_candle_breakout": bool(debug.get("three_candle_breakout", False)),
        "three_candle_reversal": bool(debug.get("three_candle_reversal", False)),
        "rising_lows": bool(debug.get("rising_lows", False)),
        "falling_highs": bool(debug.get("falling_highs", False)),
        "mid_range_flag": bool(debug.get("mid_range_candle", debug.get("in_middle", False))),
        "session": str(debug.get("session", "") or session_name),
        "spread_points": int(spread_points),
        "max_spread_points": int(max_spread_points),
    }
