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

    highs = out["high"].tolist()
    lows = out["low"].tolist()
    closes = out["close"].tolist()
    risks = risk.tolist()

    for i in range(len(out)):
        entry = closes[i]
        r = float(risks[i]) if risks[i] else 1e-6
        end = min(len(out), i + horizon_bars + 1)
        window_high = max(highs[i:end]) if i < end else entry
        window_low = min(lows[i:end]) if i < end else entry

        up_r = (window_high - entry) / r
        down_r = (window_low - entry) / r
        mfe.append(up_r)
        mae.append(down_r)
        result_r.append((closes[end - 1] - entry) / r if end - 1 >= i else 0.0)
        bars_to_outcome.append(max(0, end - i - 1))

        tp1.append(up_r >= 1.0)
        tp2.append(up_r >= 2.0)
        tp3.append(up_r >= 3.0)
        sl.append(down_r <= -1.0)

    out["mfe_r"] = mfe
    out["mae_r"] = mae
    out["result_r"] = result_r
    out["bars_to_outcome"] = bars_to_outcome
    out["tp1_hit"] = tp1
    out["tp2_hit"] = tp2
    out["tp3_hit"] = tp3
    out["sl_hit"] = sl
    out["be_hit"] = out["mfe_r"] >= 1.0
    out["trailing_exit"] = out["mfe_r"] >= 2.0

    return out
