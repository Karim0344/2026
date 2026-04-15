import importlib
import sys
from types import SimpleNamespace
from unittest.mock import patch


def _mk_bar(t: int, o: float, h: float, l: float, c: float) -> dict:
    return {"time": t, "open": o, "high": h, "low": l, "close": c}


def _build_trend_rates() -> list[dict]:
    rates: list[dict] = []
    for i in range(220):
        close = 100.0 + (i * 0.08)
        rates.append(_mk_bar(i, close - 0.10, close + 0.30, close - 0.30, close))

    # previous closed candle (-3)
    rates[-3] = _mk_bar(217, 116.80, 117.20, 116.30, 116.90)
    # signal candle (-2): bullish pullback near MA but no breakout above prev high
    rates[-2] = _mk_bar(218, 116.85, 117.18, 112.00, 117.05)
    return rates


def test_trend_intent_allows_no_breakout_when_config_disabled():
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
    mod = importlib.import_module("flexbot.strategy.trend_pullback_v1")
    rates = _build_trend_rates()

    cfg = SimpleNamespace(
        ma_fast=50,
        ma_trend=100,
        atr_period=14,
        trend_min_score=65,
        require_breakout=False,
    )

    with patch("flexbot.strategy.trend_pullback_v1.client.copy_rates", return_value=rates), patch(
        "flexbot.strategy.trend_pullback_v1.client.broker_datetime_utc",
        return_value=SimpleNamespace(hour=10),
    ):
        intent = mod.get_intent("XAUUSD", "M5", cfg=cfg, last_closed_bar_time=0)

    assert intent.valid is True
    assert intent.is_long is True
    assert intent.debug["breakout_ok_long"] is False
    assert intent.debug["trend_score_long"] >= cfg.trend_min_score


def test_trend_intent_uses_paper_relaxed_threshold_for_near_signal():
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
    mod = importlib.import_module("flexbot.strategy.trend_pullback_v1")
    rates = _build_trend_rates()

    cfg = SimpleNamespace(
        ma_fast=50,
        ma_trend=100,
        atr_period=14,
        trend_min_score=85,
        require_breakout=False,
        paper_mode=True,
        paper_trend_score_relax=5,
    )

    with patch("flexbot.strategy.trend_pullback_v1.client.copy_rates", return_value=rates), patch(
        "flexbot.strategy.trend_pullback_v1.client.broker_datetime_utc",
        return_value=SimpleNamespace(hour=10),
    ):
        intent = mod.get_intent("XAUUSD", "M5", cfg=cfg, last_closed_bar_time=0)

    assert intent.valid is True
    assert intent.is_long is True
    assert intent.reason == "PRO_LONG_PAPER"
    assert intent.debug["paper_relaxed_entry"] is True
