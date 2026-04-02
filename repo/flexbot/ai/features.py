from typing import Any


def build_feature_snapshot(
    *,
    signal_reason: str,
    intent_debug: dict[str, Any] | None,
    spread_points: int,
    max_spread_points: int,
) -> dict[str, Any]:
    debug = intent_debug or {}
    return {
        "signal_reason": signal_reason,
        "bar_time": int(debug.get("bar_time", 0)),
        "symbol": str(debug.get("symbol", "")),
        "timeframe": str(debug.get("timeframe", "")),
        "trend_ok_long": bool(debug.get("trend_ok_long", False)),
        "trend_ok_short": bool(debug.get("trend_ok_short", False)),
        "htf_ok_long": bool(debug.get("htf_ok_long", False)),
        "htf_ok_short": bool(debug.get("htf_ok_short", False)),
        "pullback_ok_long": bool(debug.get("pullback_ok_long", False)),
        "pullback_ok_short": bool(debug.get("pullback_ok_short", False)),
        "bullish_close": bool(debug.get("bullish_close", False)),
        "bearish_close": bool(debug.get("bearish_close", False)),
        "breakout_ok_long": bool(debug.get("breakout_ok_long", False)),
        "breakout_ok_short": bool(debug.get("breakout_ok_short", False)),
        "rsi_ok_long": bool(debug.get("rsi_ok_long", False)),
        "rsi_ok_short": bool(debug.get("rsi_ok_short", False)),
        "rsi": float(debug.get("rsi", 0.0) or 0.0),
        "atr": float(debug.get("atr", 0.0) or 0.0),
        "dist_ma": float(debug.get("dist_ma", 0.0) or 0.0),
        "pull_dist": float(debug.get("pull_dist", 0.0) or 0.0),
        "require_breakout": bool(debug.get("require_breakout", False)),
        "spread_points": int(spread_points),
        "max_spread_points": int(max_spread_points),
    }
