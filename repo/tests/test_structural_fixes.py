import types
import sys
import pandas as pd

mt5_stub = types.SimpleNamespace()
sys.modules.setdefault("MetaTrader5", mt5_stub)

from flexbot.trading.paper_tracker import PaperTrade, _update_trade_with_bar
from flexbot.ai.historical_strategy_simulator import build_strategy_edge_table
from flexbot.ai.outcome_labeler import label_outcomes


def test_paper_tracker_runner_hits_tp3_after_tp1_tp2():
    t = PaperTrade(batch_id="b1", symbol="X", timeframe="M5", is_long=True, entry=100, sl=99, tp1=101, tp2=102.2, tp3=103.2, created_bar_time=1)
    t, _ = _update_trade_with_bar(t, 2, 101.1, 100.2)
    assert t.status == "open" and t.tp1_hit and not t.tp2_hit
    t, _ = _update_trade_with_bar(t, 3, 102.3, 100.4)
    assert t.status == "open" and t.tp2_hit
    t, _ = _update_trade_with_bar(t, 4, 103.3, 101.0)
    assert t.status == "tp3_hit" and t.closed_bar_time == 4
    assert round(t.result_r, 2) == round((0.30*1.0)+(0.35*2.2)+(0.35*3.2), 2)


def test_paper_tracker_tp1_then_sl_weighted_result():
    t = PaperTrade(batch_id="b2", symbol="X", timeframe="M5", is_long=True, entry=100, sl=99, tp1=101, tp2=102.2, tp3=103.2, created_bar_time=1)
    t, _ = _update_trade_with_bar(t, 2, 101.1, 100.1)
    t, _ = _update_trade_with_bar(t, 3, 100.5, 98.9)
    assert t.status == "sl_hit"
    assert round(t.result_r, 2) == -0.40


def test_strategy_edge_simulator_only_real_setups():
    df = pd.DataFrame([
        {"regime":"trend","trend_score_long":75,"trend_score_short":10,"trend_min_score":60,"trend_short_extra_score":10,"trend_allow_short":False,"pullback_ok_long":True,"pullback_ok_short":False,"session_name":"London","timeframe":"M5","result_r":1.0,"tp1_hit":True,"tp2_hit":False,"tp3_hit":False,"sl_hit":False},
        {"regime":"trend","trend_score_long":20,"trend_score_short":10,"trend_min_score":60,"trend_short_extra_score":10,"trend_allow_short":False,"pullback_ok_long":False,"pullback_ok_short":False,"session_name":"London","timeframe":"M5","result_r":-1.0,"tp1_hit":False,"tp2_hit":False,"tp3_hit":False,"sl_hit":True},
    ])
    table = build_strategy_edge_table(df, min_samples=1)
    assert len(table) == 1
    assert table.iloc[0]["strategy_name"] == "PRO_LONG"


def test_outcome_labeler_cost_point_size_conversion():
    df = pd.DataFrame([
        {"open":100,"high":101,"low":99.8,"close":100.2,"atr":1.0,"side":"long"},
        {"open":100.2,"high":100.3,"low":100.1,"close":100.25,"atr":1.0,"side":"long"},
    ])
    out = label_outcomes(df, horizon_bars=1, spread_cost_points=10, slippage_points=5, point_size=0.01)
    # timeout r approx close diff/r (0.05) - cost (0.15)
    assert round(float(out.iloc[0]["result_r"]), 2) == -0.10

def test_engine_live_decision_uses_decision_variable():
    from pathlib import Path
    src = Path('flexbot/trading/engine.py').read_text(encoding='utf-8')
    assert '"paper_signal" if self.cfg.paper_mode else "live_signal"' not in src
    assert 'decision,' in src
