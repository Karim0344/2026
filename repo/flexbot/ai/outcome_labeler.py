from __future__ import annotations

import pandas as pd


def _resolve_same_bar_exit(tp_hit: bool, sl_hit: bool, same_bar_priority: str) -> str | None:
    if tp_hit and sl_hit:
        if same_bar_priority == "conservative":
            return "SL"
        if same_bar_priority == "optimistic":
            return "TP3"
        if same_bar_priority == "skip_ambiguous":
            return "AMBIGUOUS_SKIP"
    elif tp_hit:
        return "TP3"
    elif sl_hit:
        return "SL"
    return None


def _weighted_result_r(tp1_hit: bool, tp2_hit: bool, tp3_hit: bool, exit_reason: str, tp1_r: float, tp2_r: float, tp3_r: float, tp1_size_ratio: float, tp2_size_ratio: float, tp3_size_ratio: float) -> float:
    reached = [tp1_hit, tp2_hit, tp3_hit]
    if exit_reason == "TP3":
        reached = [True, True, True]
    rr = 0.0
    for ratio, hit, lvl_r in zip((tp1_size_ratio, tp2_size_ratio, tp3_size_ratio), reached, (tp1_r, tp2_r, tp3_r)):
        rr += ratio * (lvl_r if hit else -1.0)
    return rr


def _timeout_result_r(exit_reason: str, current_r: float, tp1_hit: bool, tp2_hit: bool, tp1_r: float, tp2_r: float, tp1_size_ratio: float, tp2_size_ratio: float, tp3_size_ratio: float, timeout_policy: str) -> float | None:
    if exit_reason != "timeout":
        return None
    if timeout_policy == "skip":
        return None
    remaining_r = 0.0 if timeout_policy == "breakeven" else current_r
    result = 0.0
    result += tp1_size_ratio * (tp1_r if tp1_hit else remaining_r)
    result += tp2_size_ratio * (tp2_r if tp2_hit else remaining_r)
    result += tp3_size_ratio * remaining_r
    return result


def label_outcomes(df: pd.DataFrame, horizon_bars: int = 20, risk_atr_mult: float = 1.0, spread_cost_points: int = 10, slippage_points: int = 5, point_size: float = 0.01, tp1_r_multiple: float = 1.0, tp2_r_multiple: float = 2.2, tp3_r_multiple: float = 3.2, tp1_size_ratio: float = 0.30, tp2_size_ratio: float = 0.35, tp3_size_ratio: float = 0.35, same_bar_priority: str = "conservative", learning_timeout_policy: str = "mark_to_market") -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    atr = out.get("atr", pd.Series([0.0] * len(out)))
    risk = (atr * risk_atr_mult).replace(0, pd.NA).bfill().ffill().fillna(1e-6)

    out["future_return_5_bars"] = (out["close"].shift(-5) - out["close"]) / risk
    out["future_return_10_bars"] = (out["close"].shift(-10) - out["close"]) / risk
    out["future_return_20_bars"] = (out["close"].shift(-20) - out["close"]) / risk

    rows = []
    highs, lows, closes, risks = out["high"].tolist(), out["low"].tolist(), out["close"].tolist(), risk.tolist()
    side_series = out.get("side", pd.Series(["long"] * len(out), index=out.index)).astype(str).str.lower()

    for i in range(len(out)):
        entry = closes[i]
        r = float(risks[i]) if risks[i] else 1e-6
        end = min(len(out), i + horizon_bars + 1)
        future_highs = highs[i:end] if i < end else [entry]
        future_lows = lows[i:end] if i < end else [entry]
        window_high, window_low = max(future_highs), min(future_lows)
        up_r, down_r = (window_high - entry) / r, (window_low - entry) / r

        long_tp1, long_tp2, long_tp3, long_sl = entry + tp1_r_multiple * r, entry + tp2_r_multiple * r, entry + tp3_r_multiple * r, entry - r
        short_tp1, short_tp2, short_tp3, short_sl = entry - tp1_r_multiple * r, entry - tp2_r_multiple * r, entry - tp3_r_multiple * r, entry + r

        long_exit, short_exit = "timeout", "timeout"
        long_tp1_hit = long_tp2_hit = short_tp1_hit = short_tp2_hit = False
        long_bars = short_bars = max(0, end - i - 1)

        for offset in range(1, max(1, end - i)):
            idx = i + offset
            if idx >= len(out):
                break
            hi, lo = highs[idx], lows[idx]

            if long_exit == "timeout":
                long_tp1_hit |= hi >= long_tp1
                long_tp2_hit |= hi >= long_tp2
                long_decision = _resolve_same_bar_exit(hi >= long_tp3, lo <= long_sl, same_bar_priority)
                if long_decision is not None:
                    long_exit = long_decision
                    long_bars = offset

            if short_exit == "timeout":
                short_tp1_hit |= lo <= short_tp1
                short_tp2_hit |= lo <= short_tp2
                short_decision = _resolve_same_bar_exit(lo <= short_tp3, hi >= short_sl, same_bar_priority)
                if short_decision is not None:
                    short_exit = short_decision
                    short_bars = offset

            if long_exit != "timeout" and short_exit != "timeout":
                break

        is_short = side_series.iloc[i] == "short"
        selected_exit = short_exit if is_short else long_exit
        tp3_hit_flag = selected_exit == "TP3"
        tp1_hit_flag = (short_tp1_hit if is_short else long_tp1_hit) or tp3_hit_flag
        tp2_hit_flag = (short_tp2_hit if is_short else long_tp2_hit) or tp3_hit_flag

        timeout_current_r = ((entry - closes[end - 1]) / r) if is_short else ((closes[end - 1] - entry) / r)
        selected_r = _timeout_result_r(
            selected_exit,
            timeout_current_r,
            tp1_hit_flag,
            tp2_hit_flag,
            tp1_r_multiple,
            tp2_r_multiple,
            tp1_size_ratio,
            tp2_size_ratio,
            tp3_size_ratio,
            learning_timeout_policy,
        )
        if selected_r is None:
            selected_r = _weighted_result_r(tp1_hit_flag, tp2_hit_flag, tp3_hit_flag, selected_exit, tp1_r_multiple, tp2_r_multiple, tp3_r_multiple, tp1_size_ratio, tp2_size_ratio, tp3_size_ratio)

        cost_r = ((spread_cost_points + slippage_points) * float(point_size)) / max(r, 1e-6)
        selected_r -= cost_r

        rows.append({
            "mfe_r": (entry - window_low) / r if is_short else up_r,
            "mae_r": (entry - window_high) / r if is_short else down_r,
            "result_r": selected_r,
            "bars_to_outcome": short_bars if is_short else long_bars,
            "tp1_hit": tp1_hit_flag,
            "tp2_hit": tp2_hit_flag,
            "tp3_hit": tp3_hit_flag,
            "sl_hit": selected_exit == "SL",
            "first_exit": selected_exit,
            "first_exit_side": "short" if is_short else "long",
        })

    out = pd.concat([out.reset_index(drop=True), pd.DataFrame(rows)], axis=1)
    out["be_hit"] = out["mfe_r"] >= 1.0
    out["trailing_exit"] = out["mfe_r"] >= 2.0
    if learning_timeout_policy == "skip":
        out = out[out["first_exit"] != "timeout"].reset_index(drop=True)
    return out
