import importlib
import sys
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

    # force final closed candle (-2) to look like a top rejection
    rates[-2] = _mk_bar(258, 100.20, 100.95, 99.95, 100.00)
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
    ):
        intent = mod.get_range_intent("XAUUSD", "M5", cfg=SimpleNamespace())

    assert intent.direction == "short"
    assert intent.reason == "RANGE_SHORT"
    assert intent.debug["top_touches"] >= 2
