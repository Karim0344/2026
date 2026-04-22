from flexbot.ai.features import build_feature_snapshot


def test_feature_snapshot_uses_side_specific_flags_for_long():
    features = build_feature_snapshot(
        signal_reason="PRO_LONG",
        intent_debug={
            "trend_ok_long": True,
            "trend_ok_short": False,
            "htf_ok_long": False,
            "htf_ok_short": True,
            "pullback_ok_long": True,
            "pullback_ok_short": False,
            "bullish_close": True,
            "bearish_close": False,
            "breakout_ok_long": True,
            "breakout_ok_short": False,
            "htf_ok": True,
        },
        spread_points=12,
        max_spread_points=45,
        strategy_name="PRO_LONG",
        side="long",
    )

    assert features["trend_ok"] is True
    assert features["htf_ok"] is False
    assert features["pullback"] is True
    assert features["momentum"] is True
    assert features["breakout"] is True
    assert features["feature_side_consistent"] is False
