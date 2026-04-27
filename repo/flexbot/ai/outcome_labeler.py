from __future__ import annotations

import pandas as pd


def label_outcomes(df: pd.DataFrame, horizon_bars: int = 20, risk_atr_mult: float = 1.0) -> pd.DataFrame:
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
        long_tp1 = entry + (1.0 * r)
        long_tp2 = entry + (2.0 * r)
        long_tp3 = entry + (3.0 * r)
        long_sl = entry - (1.0 * r)
        short_tp1 = entry - (1.0 * r)
        short_tp2 = entry - (2.0 * r)
        short_tp3 = entry - (3.0 * r)
        short_sl = entry + (1.0 * r)

        long_exit = "timeout"
        short_exit = "timeout"
        long_r = (closes[end - 1] - entry) / r if end - 1 >= i else 0.0
        short_r = (entry - closes[end - 1]) / r if end - 1 >= i else 0.0
        long_bars = max(0, end - i - 1)
        short_bars = max(0, end - i - 1)

        for offset in range(1, max(1, end - i)):
            idx = i + offset
            if idx >= len(out):
                break
            hi = highs[idx]
            lo = lows[idx]

            if long_exit == "timeout":
                if lo <= long_sl:
                    long_exit = "SL"
                    long_r = -1.0
                    long_bars = offset
                elif hi >= long_tp3:
                    long_exit = "TP3"
                    long_r = 3.0
                    long_bars = offset
                elif hi >= long_tp2:
                    long_exit = "TP2"
                    long_r = 2.0
                    long_bars = offset
                elif hi >= long_tp1:
                    long_exit = "TP1"
                    long_r = 1.0
                    long_bars = offset

            if short_exit == "timeout":
                if hi >= short_sl:
                    short_exit = "SL"
                    short_r = -1.0
                    short_bars = offset
                elif lo <= short_tp3:
                    short_exit = "TP3"
                    short_r = 3.0
                    short_bars = offset
                elif lo <= short_tp2:
                    short_exit = "TP2"
                    short_r = 2.0
                    short_bars = offset
                elif lo <= short_tp1:
                    short_exit = "TP1"
                    short_r = 1.0
                    short_bars = offset

            if long_exit != "timeout" and short_exit != "timeout":
                break

        side = side_series.iloc[i]
        is_short = side == "short"
        selected_exit = short_exit if is_short else long_exit
        selected_r = short_r if is_short else long_r
        selected_bars = short_bars if is_short else long_bars
        selected_up_r = (entry - window_low) / r if is_short else up_r
        selected_down_r = (entry - window_high) / r if is_short else down_r

        result_r.append(selected_r)
        bars_to_outcome.append(selected_bars)
        first_exit.append(selected_exit)
        first_exit_side.append("short" if is_short else "long")
        tp1.append(selected_exit == "TP1")
        tp2.append(selected_exit == "TP2")
        tp3.append(selected_exit == "TP3")
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
