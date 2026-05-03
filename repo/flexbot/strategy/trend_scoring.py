from __future__ import annotations


def compute_trend_decision(features: dict, cfg) -> dict:
    trend_score_long = int(features.get("trend_score_long", 0))
    trend_score_short = int(features.get("trend_score_short", 0))
    min_score = int(getattr(cfg, "trend_min_score", 60))
    paper_mode = bool(getattr(cfg, "paper_mode", False))
    paper_relax = int(getattr(cfg, "paper_trend_score_relax", 0)) if paper_mode else 0
    effective_min_score = max(min_score - max(paper_relax, 0), 0)
    short_extra_score = max(int(getattr(cfg, "trend_short_extra_score", 0)), 0)
    short_min_score = effective_min_score + short_extra_score
    allow_short = bool(getattr(cfg, "trend_allow_short", False))

    long_valid = bool(features.get("trend_ok_long")) and bool(features.get("pullback_ok_long")) and bool(features.get("bullish_close")) and trend_score_long >= effective_min_score
    short_valid = allow_short and bool(features.get("trend_ok_short")) and bool(features.get("pullback_ok_short")) and bool(features.get("bearish_close")) and trend_score_short >= short_min_score

    return {
        "trend_score_long": trend_score_long,
        "trend_score_short": trend_score_short,
        "effective_min_score": effective_min_score,
        "long_valid": long_valid,
        "short_valid": short_valid,
        "long_reason": "ok" if long_valid else "long_gate_failed",
        "short_reason": "ok" if short_valid else "short_gate_failed",
    }
