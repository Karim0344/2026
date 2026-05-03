import importlib
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch


def _mk_bar(t: int, o: float, h: float, l: float, c: float) -> dict:
    return {"time": t, "open": o, "high": h, "low": l, "close": c}


def _build_sideways_rates() -> list[dict]:
    rates: list[dict] = []
    for i in range(260):
        base = 100.0 + ((i % 8) - 4) * 0.15
        high = base + 0.35
        low = base - 0.35
        rates.append(_mk_bar(i, base - 0.05, high, low, base + 0.05))

    # force prior bars to define a stable top zone
    rates[-10] = _mk_bar(250, 100.05, 100.55, 99.75, 100.10)
    rates[-9] = _mk_bar(251, 100.00, 100.56, 99.70, 100.04)
    # force final closed candle (-2) to fake-break and reclaim from top
    rates[-2] = _mk_bar(258, 100.65, 101.20, 100.10, 100.30)
    return rates


def test_range_intent_emits_short_on_confirmed_top_rejection():
    sys.modules.setdefault(
        "MetaTrader5",
        SimpleNamespace(
            TIMEFRAME_M1=1,
            TIMEFRAME_M5=5,
            TIMEFRAME_M15=15,
            TIMEFRAME_H1=60,
            TIMEFRAME_H4=240,
        ),
    )
    mod = importlib.import_module("flexbot.strategy.range_rejection")

    with patch(
        "flexbot.strategy.range_rejection.client.copy_rates",
        return_value=_build_sideways_rates(),
    ), patch(
        "flexbot.strategy.range_rejection.client.broker_datetime_utc",
        return_value=datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
    ):
        intent = mod.get_range_intent("XAUUSD", "M5", cfg=SimpleNamespace())

    assert intent.direction == "short"
    assert intent.reason == "RANGE_SHORT"
    assert intent.debug["top_touches"] >= 2
    assert bool(intent.debug["fake_break_top"]) is True
    assert bool(intent.debug["reclaim_top"]) is True


def test_range_intent_allows_fake_break_reclaim_inside_middle_band():
    sys.modules.setdefault(
        "MetaTrader5",
        SimpleNamespace(
            TIMEFRAME_M1=1,
            TIMEFRAME_M5=5,
            TIMEFRAME_M15=15,
            TIMEFRAME_H1=60,
            TIMEFRAME_H4=240,
        ),
    )
    mod = importlib.import_module("flexbot.strategy.range_rejection")

    rates = _build_sideways_rates()
    # Make the signal candle close in the middle region, while still fake-breaking above range high.
    rates[-2] = _mk_bar(258, 100.33, 101.20, 100.10, 100.15)

    with patch(
        "flexbot.strategy.range_rejection.client.copy_rates",
        return_value=rates,
    ), patch(
        "flexbot.strategy.range_rejection.client.broker_datetime_utc",
        return_value=datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
    ):
        intent = mod.get_range_intent("XAUUSD", "M5", cfg=SimpleNamespace())

    assert intent.direction == "short"
    assert intent.reason == "RANGE_SHORT"
    assert bool(intent.debug["in_middle"]) is True
    assert bool(intent.debug["middle_override_top"]) is True


def test_range_intent_uses_percentile_based_max_atr_ratio():
    sys.modules.setdefault(
        "MetaTrader5",
        SimpleNamespace(
            TIMEFRAME_M1=1,
            TIMEFRAME_M5=5,
            TIMEFRAME_M15=15,
            TIMEFRAME_H1=60,
            TIMEFRAME_H4=240,
        ),
    )
    mod = importlib.import_module("flexbot.strategy.range_rejection")
    rates = _build_sideways_rates()
    # Inject one large spike into the range-estimation window so current atr_ratio becomes extreme.
    rates[-40] = _mk_bar(220, 100.00, 130.00, 70.00, 100.00)

    cfg = SimpleNamespace(
        range_max_atr_ratio=50.0,
        range_max_atr_ratio_percentile=0.50,
        range_atr_ratio_percentile_window=120,
    )
    with patch(
        "flexbot.strategy.range_rejection.client.copy_rates",
        return_value=rates,
    ), patch(
        "flexbot.strategy.range_rejection.client.broker_datetime_utc",
        return_value=datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
    ):
        intent = mod.get_range_intent("XAUUSD", "M5", cfg=cfg)

    assert intent.direction is None
    assert intent.reason == "range_width_invalid"
    assert intent.debug["max_allowed"] <= intent.debug["range_max_atr_ratio"]
