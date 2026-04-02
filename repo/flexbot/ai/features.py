from typing import Any


def build_feature_snapshot(
    *,
    signal_reason: str,
    intent_debug: dict[str, Any] | None,
    spread_points: int,
    max_spread_points: int,
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

    return {
        "signal_reason": signal_reason,
        "bar_time": int(debug.get("bar_time", 0)),
        "symbol": str(debug.get("symbol", "")),
        "timeframe": str(debug.get("timeframe", "")),
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
        "regime": str(debug.get("regime", "")),
        "body_size": float(debug.get("body_size", 0.0) or 0.0),
        "wick_ratio": float(debug.get("wick_ratio", 0.0) or 0.0),
        "session": str(debug.get("session", "")),
        "spread_points": int(spread_points),
        "max_spread_points": int(max_spread_points),
    }
