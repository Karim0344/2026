import importlib
import sys
from types import SimpleNamespace
from unittest.mock import patch


def _build_rates(n: int, start: float = 2000.0):
    rates = []
    for i in range(n):
        close = start + (i * 0.1)
        rates.append(
            {
                "time": i,
                "open": close - 0.2,
                "high": close + 0.4,
                "low": close - 0.4,
                "close": close,
            }
        )
    return rates


def test_detect_regime_returns_dead_when_indicators_nan():
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
    regime = importlib.import_module("flexbot.ai.regime")

    with patch("flexbot.ai.regime.client.copy_rates", return_value=_build_rates(120)):
        detected, debug = regime.detect_regime("XAUUSD", "M5", bars=120)

    assert detected == "dead"
    assert debug["reason"] == "indicator_nan_uncertain"
    assert debug["valid_rows"] == 0
