from __future__ import annotations

import pandas as pd


def _weighted_result_r(tp1_hit: bool, tp2_hit: bool, tp3_hit: bool, exit_reason: str, tp1_r: float, tp2_r: float, tp3_r: float, tp1_size_ratio: float, tp2_size_ratio: float, tp3_size_ratio: float) -> float:
    reached = [tp1_hit, tp2_hit, tp3_hit]
    if exit_reason == "TP3":
        reached = [True, True, True]
    rr = 0.0
    for ratio, hit, lvl_r in zip((tp1_size_ratio, tp2_size_ratio, tp3_size_ratio), reached, (tp1_r, tp2_r, tp3_r)):
        rr += ratio * (lvl_r if hit else -1.0)
    return rr


def label_outcomes(df: pd.DataFrame, horizon_bars: int = 20, risk_atr_mult: float = 1.0, spread_cost_points: int = 10, slippage_points: int = 5, point_size: float = 0.01, tp1_r_multiple: float = 1.0, tp2_r_multiple: float = 2.2, tp3_r_multiple: float = 3.2, tp1_size_ratio: float = 0.30, tp2_size_ratio: float = 0.35, tp3_size_ratio: float = 0.35, same_bar_priority: str = "conservative") -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    atr = out.get("atr", pd.Series([0.0] * len(out)))
    risk = (atr * risk_atr_mult).replace(0, pd.NA).bfill().ffill().fillna(1e-6)

    out["future_return_5_bars"] = (out["close"].shift(-5) - out["close"]) / risk
    out["future_return_10_bars"] = (out["close"].shift(-10) - out["close"]) / risk
    out["future_return_20_bars"] = (out["close"].shift(-20) - out["close"]) / risk

    mfe = []
    mae = []
    result_r = []
    bars_to_outcome = []
    tp1 = []
    tp2 = []
    tp3 = []
    sl = []
    first_exit = []
    first_exit_side = []

    highs = out["high"].tolist()
    lows = out["low"].tolist()
    closes = out["close"].tolist()
    risks = risk.tolist()

    side_series = out.get("side", pd.Series(["long"] * len(out), index=out.index)).astype(str).str.lower()

    for i in range(len(out)):
        entry = closes[i]
        r = float(risks[i]) if risks[i] else 1e-6
        end = min(len(out), i + horizon_bars + 1)
        future_highs = highs[i:end] if i < end else [entry]
        future_lows = lows[i:end] if i < end else [entry]
        window_high = max(future_highs)
        window_low = min(future_lows)

        up_r = (window_high - entry) / r
        down_r = (window_low - entry) / r
        mfe.append(up_r)
        mae.append(down_r)
        long_tp1 = entry + (tp1_r_multiple * r)
        long_tp2 = entry + (tp2_r_multiple * r)
        long_tp3 = entry + (tp3_r_multiple * r)
        long_sl = entry - (1.0 * r)
        short_tp1 = entry - (tp1_r_multiple * r)
        short_tp2 = entry - (tp2_r_multiple * r)
        short_tp3 = entry - (tp3_r_multiple * r)
        short_sl = entry + (1.0 * r)

        long_exit = "timeout"
        short_exit = "timeout"
        long_tp1_hit = False
        long_tp2_hit = False
        short_tp1_hit = False
        short_tp2_hit = False
        long_bars = max(0, end - i - 1)
        short_bars = max(0, end - i - 1)

        for offset in range(1, max(1, end - i)):
            idx = i + offset
            if idx >= len(out):
                break
            hi = highs[idx]
            lo = lows[idx]

            if long_exit == "timeout":
                if hi >= long_tp1:
                    long_tp1_hit = True
                if hi >= long_tp2:
                    long_tp2_hit = True
                long_tp_hit = hi >= long_tp3
                long_sl_hit = lo <= long_sl
                if long_tp_hit and long_sl_hit and same_bar_priority == "skip_ambiguous":
                    long_exit = "AMBIGUOUS_SKIP"
                    long_bars = offset
                if long_tp_hit and long_sl_hit and same_bar_priority == "conservative":
                    long_exit = "SL"
                    long_bars = offset
                elif long_tp_hit:
                    long_exit = "TP3"
                    long_bars = offset
                elif long_sl_hit:
                    long_exit = "SL"
                    long_bars = offset

            if short_exit == "timeout":
                if lo <= short_tp1:
                    short_tp1_hit = True
                if lo <= short_tp2:
                    short_tp2_hit = True
                short_tp_hit = lo <= short_tp3
                short_sl_hit = hi >= short_sl
                if short_tp_hit and short_sl_hit and same_bar_priority == "skip_ambiguous":
                    short_exit = "AMBIGUOUS_SKIP"
                    short_bars = offset
                if short_tp_hit and short_sl_hit and same_bar_priority == "conservative":
                    short_exit = "SL"
                    short_bars = offset
                elif short_tp_hit:
                    short_exit = "TP3"
                    short_bars = offset
                elif short_sl_hit:
                    short_exit = "SL"
                    short_bars = offset

            if long_exit != "timeout" and short_exit != "timeout":
                break

        side = side_series.iloc[i]
        is_short = side == "short"
        selected_exit = short_exit if is_short else long_exit
        tp1_hit_flag = False
        tp2_hit_flag = False
        tp3_hit_flag = selected_exit == "TP3"
        if is_short:
            tp1_hit_flag = short_tp1_hit or tp3_hit_flag
            tp2_hit_flag = short_tp2_hit or tp3_hit_flag
        else:
            tp1_hit_flag = long_tp1_hit or tp3_hit_flag
            tp2_hit_flag = long_tp2_hit or tp3_hit_flag
        selected_r = _weighted_result_r(tp1_hit_flag, tp2_hit_flag, tp3_hit_flag, selected_exit, tp1_r_multiple, tp2_r_multiple, tp3_r_multiple, tp1_size_ratio, tp2_size_ratio, tp3_size_ratio)
        cost_price = (spread_cost_points + slippage_points) * float(point_size)
        cost_r = cost_price / max(r, 1e-6)
        selected_r = selected_r - cost_r
        selected_bars = short_bars if is_short else long_bars
        selected_up_r = (entry - window_low) / r if is_short else up_r
        selected_down_r = (entry - window_high) / r if is_short else down_r

        result_r.append(selected_r)
        bars_to_outcome.append(selected_bars)
        first_exit.append(selected_exit)
        first_exit_side.append("short" if is_short else "long")
        tp1.append(tp1_hit_flag)
        tp2.append(tp2_hit_flag)
        tp3.append(tp3_hit_flag)
        sl.append(selected_exit == "SL")
        mfe[-1] = selected_up_r
        mae[-1] = selected_down_r

    out["mfe_r"] = mfe
    out["mae_r"] = mae
    out["result_r"] = result_r
    out["bars_to_outcome"] = bars_to_outcome
    out["tp1_hit"] = tp1
    out["tp2_hit"] = tp2
    out["tp3_hit"] = tp3
    out["sl_hit"] = sl
    out["first_exit"] = first_exit
    out["first_exit_side"] = first_exit_side
    out["be_hit"] = out["mfe_r"] >= 1.0
    out["trailing_exit"] = out["mfe_r"] >= 2.0

    return out
