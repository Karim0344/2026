from __future__ import annotations

import hashlib
import json


def build_learning_version(cfg) -> str:
    payload = {
        "symbol": getattr(cfg, "symbol", ""),
        "timeframe": getattr(cfg, "timeframe", ""),
        "ma_fast": getattr(cfg, "ma_fast", None),
        "ma_trend": getattr(cfg, "ma_trend", None),
        "atr_period": getattr(cfg, "atr_period", None),
        "range_lookback": getattr(cfg, "range_lookback", None),
        "range_min_atr_ratio": getattr(cfg, "range_min_atr_ratio", None),
        "range_max_atr_ratio": getattr(cfg, "range_max_atr_ratio", None),
        "range_required_touches": getattr(cfg, "range_required_touches", None),
        "range_wick_body_min": getattr(cfg, "range_wick_body_min", None),
        "tp1_r_multiple": getattr(cfg, "tp1_r_multiple", None),
        "tp2_r_multiple": getattr(cfg, "tp2_r_multiple", None),
        "tp3_r_multiple": getattr(cfg, "tp3_r_multiple", None),
        "tp1_size_ratio": getattr(cfg, "tp1_size_ratio", None),
        "tp2_size_ratio": getattr(cfg, "tp2_size_ratio", None),
        "tp3_size_ratio": getattr(cfg, "tp3_size_ratio", None),
        "same_bar_priority": getattr(cfg, "same_bar_priority", None),
        "learning_timeout_policy": getattr(cfg, "learning_timeout_policy", None),
        "learning_horizon_bars": getattr(cfg, "learning_horizon_bars", None),
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
