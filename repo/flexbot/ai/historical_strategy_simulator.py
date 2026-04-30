from __future__ import annotations

import pandas as pd


def build_strategy_edge_table(df: pd.DataFrame, min_samples: int = 20) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    def _map(r):
        reg = str(r.get('regime',''))
        side = str(r.get('side','long'))
        if reg.startswith('range'):
            return 'RANGE_LONG' if side == 'long' else 'RANGE_SHORT'
        return 'PRO_LONG' if side == 'long' else 'PRO_SHORT'
    out['strategy_name'] = out.apply(_map, axis=1)
    grouped = out.groupby(['strategy_name','regime','side','session_name','timeframe'], dropna=False).agg(
        count=('result_r','size'), winrate=('result_r', lambda s: (s.gt(0).mean()*100.0)), avg_r=('result_r','mean'),
        tp1_rate=('tp1_hit','mean'), tp2_rate=('tp2_hit','mean'), tp3_rate=('tp3_hit','mean'), sl_rate=('sl_hit','mean')
    ).reset_index()
    grouped = grouped[grouped['count'] >= int(min_samples)].copy()
    for c in ('winrate','avg_r','tp1_rate','tp2_rate','tp3_rate','sl_rate'):
        grouped[c] = grouped[c].astype(float).round(4)
    return grouped
