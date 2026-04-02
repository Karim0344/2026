from flexbot.ai.scoring import confidence_score


def test_confidence_score_high_quality_long_setup():
    features = {
        "trend_ok": True,
        "htf_ok": True,
        "pullback": True,
        "momentum": True,
        "breakout": True,
        "spread_points": 20,
    }

    score = confidence_score(features, is_long=True, max_spread=35)
    assert score == 90


def test_confidence_score_blocks_low_quality_setup():
    features = {
        "trend_ok": True,
        "htf_ok": False,
        "pullback": False,
        "momentum": False,
        "breakout": False,
        "spread_points": 40,
    }

    score = confidence_score(features, is_long=True, max_spread=35)
    assert score == 10
