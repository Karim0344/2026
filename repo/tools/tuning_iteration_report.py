from __future__ import annotations

from collections import Counter
from dataclasses import replace
import sys
from types import SimpleNamespace
from unittest.mock import patch

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

from flexbot.core.config import BotConfig
from flexbot.strategy.range_rejection import get_range_intent
from flexbot.strategy.trend_pullback_v1 import get_intent


def _mk_bar(t: int, o: float, h: float, l: float, c: float) -> dict:
    return {"time": t, "open": o, "high": h, "low": l, "close": c}


def build_rates(n: int = 1200) -> tuple[list[dict], list[str]]:
    rates: list[dict] = []
    regimes: list[str] = []
    px = 100.0
    for i in range(n):
        block = (i // 120) % 2
        if block == 0:
            regimes.append("trend")
            drift = 0.08 if (i // 240) % 2 == 0 else -0.08
            noise = ((i % 7) - 3) * 0.015
            close = px + drift + noise
            pull = 0.55 if i % 9 == 0 else 0.30
            h = max(px, close) + 0.35 + (0.10 if i % 14 == 0 else 0.0)
            l = min(px, close) - pull
            o = px
        else:
            regimes.append("range")
            center = 110.0 + ((i // 120) % 3) * 0.5
            swing = ((i % 16) - 8) * 0.12
            close = center + swing
            o = close - (0.08 if i % 2 == 0 else -0.08)
            h = max(o, close) + 0.42 + (0.35 if i % 19 == 0 else 0.0)
            l = min(o, close) - 0.42 - (0.35 if i % 23 == 0 else 0.0)
        rates.append(_mk_bar(i, o, h, l, close))
        px = close
    return rates, regimes


def _trade_outcome(rates: list[dict], idx: int, is_long: bool, entry: float, sl: float) -> float:
    risk = abs(entry - sl)
    if risk <= 1e-9:
        return 0.0
    tp = entry + (2.0 * risk if is_long else -2.0 * risk)
    for step in range(1, 9):
        j = idx + step
        if j >= len(rates):
            break
        bar = rates[j]
        if is_long:
            if bar["low"] <= sl:
                return -1.0
            if bar["high"] >= tp:
                return 2.0
        else:
            if bar["high"] >= sl:
                return -1.0
            if bar["low"] <= tp:
                return 2.0
    return 0.0


def run_iteration(cfg: BotConfig) -> dict:
    rates, regimes = build_rates()
    cand = 0
    true_sig = 0
    near_sig = 0
    rejects = Counter()
    outcomes = {"trend": [], "range": []}
    outcomes_side = {"long": [], "short": []}
    atr_ratios = []

    for i in range(260, len(rates) - 10):
        subset = rates[: i + 1]
        regime = regimes[i - 1]
        if regime == "trend":
            with patch("flexbot.strategy.trend_pullback_v1.client.copy_rates", return_value=subset), patch(
                "flexbot.strategy.trend_pullback_v1.client.broker_datetime_utc", return_value=SimpleNamespace(hour=10)
            ):
                intent = get_intent("XAUUSD", "M5", cfg, last_closed_bar_time=-1)
            reason = intent.reason
            is_candidate = bool(intent.valid) or reason == "trend_near_signal"
        else:
            with patch("flexbot.strategy.range_rejection.client.copy_rates", return_value=subset):
                r_intent = get_range_intent("XAUUSD", "M5", cfg)
            reason = r_intent.reason
            is_candidate = bool(r_intent.direction) or reason not in {
                "range_idle",
                "range_not_confirmed",
                "range_width_invalid",
            }
            if "atr_ratio" in r_intent.debug:
                atr_ratios.append(float(r_intent.debug["atr_ratio"]))
            intent = SimpleNamespace(
                valid=bool(r_intent.direction),
                is_long=(r_intent.direction == "long"),
                entry=r_intent.entry,
                sl=r_intent.sl,
                reason=reason,
            )

        if is_candidate:
            cand += 1
        if intent.valid:
            true_sig += 1
            rr = _trade_outcome(rates, i, intent.is_long, float(intent.entry), float(intent.sl))
            outcomes[regime].append(rr)
            outcomes_side["long" if intent.is_long else "short"].append(rr)
        elif reason == "trend_near_signal" or reason in {"no_signal", "mid_range_candle", "weak_candle"}:
            near_sig += 1
        else:
            rejects[reason] += 1

    bars = len(rates) - 270
    per100 = lambda x: (x / bars) * 100.0 if bars else 0.0

    def avg(xs: list[float]) -> float:
        return sum(xs) / len(xs) if xs else 0.0

    atr_sorted = sorted(atr_ratios)

    def pct(q: float) -> float:
        if not atr_sorted:
            return 0.0
        pos = int((len(atr_sorted) - 1) * q)
        return atr_sorted[pos]

    return {
        "bars": bars,
        "candidate_per_100": round(per100(cand), 2),
        "true_per_100": round(per100(true_sig), 2),
        "near_per_100": round(per100(near_sig), 2),
        "candidate_to_true_pct": round((true_sig / cand * 100.0), 2) if cand else 0.0,
        "near_to_true_ratio": round((near_sig / true_sig), 2) if true_sig else 0.0,
        "top_rejects": rejects.most_common(3),
        "expectancy_trend": round(avg(outcomes["trend"]), 3),
        "expectancy_range": round(avg(outcomes["range"]), 3),
        "long_count": len(outcomes_side["long"]),
        "short_count": len(outcomes_side["short"]),
        "expectancy_long": round(avg(outcomes_side["long"]), 3),
        "expectancy_short": round(avg(outcomes_side["short"]), 3),
        "atr_ratio_p50": round(pct(0.50), 3),
        "atr_ratio_p75": round(pct(0.75), 3),
        "atr_ratio_p90": round(pct(0.90), 3),
        "atr_ratio_p95": round(pct(0.95), 3),
    }


def main() -> None:
    base = BotConfig()
    baseline = replace(
        base,
        trend_min_score=60,
        range_required_touches=2,
        range_atr_ratio_window=0,
        ai_enable_scoring=True,
        ai_selector_enable=True,
        paper_allow_near_signals=False,
        paper_near_extra_score=5,
        paper_near_tolerance=0,
    )
    iter1 = replace(base)
    iter2 = replace(iter1, trend_allow_short=True, trend_short_extra_score=15, range_wick_body_min=1.25)
    iter3 = replace(
        iter2,
        ai_enable_scoring=True,
        ai_selector_enable=True,
        context_score_weight=1.15,
        pattern_score_weight=0.9,
        setup_score_weight=1.0,
    )

    for name, cfg in (("baseline", baseline), ("iter1", iter1), ("iter2", iter2), ("iter3", iter3)):
        print(name, run_iteration(cfg))


if __name__ == "__main__":
    main()
