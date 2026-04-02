from flexbot.ai.scoring import confidence_score


def test_confidence_score_high_quality_long_setup():
    features = {
        "trend_ok_long": True,
        "pullback_ok_long": True,
        "bullish_close": True,
        "breakout_ok_long": True,
        "rsi": 50,
        "spread_points": 20,
    }

    score = confidence_score(features, is_long=True, max_spread_points=35)
    assert score == 100


def test_confidence_score_blocks_low_quality_setup():
    features = {
        "trend_ok_long": True,
        "pullback_ok_long": False,
        "bullish_close": False,
        "breakout_ok_long": False,
        "rsi": 72,
        "spread_points": 40,
    }

    score = confidence_score(features, is_long=True, max_spread_points=35)
    assert score == 25
