from dataclasses import dataclass


@dataclass
class BotConfig:
    symbol: str = "XAUUSD"
    timeframe: str = "M5"
    risk_percent: float = 0.5
    daily_stop_percent: float = 2.0
    max_consec_loss: int = 2
    max_spread_points: int = 45
    magic: int = 26022026

    # MT5
    terminal_path: str = ""
    auto_resolve_symbol: bool = True
    mt5_login: int | None = None
    mt5_password: str = ""
    mt5_server: str = ""

    # Modes
    paper_mode: bool = True  # dry-run: log signals, do not send orders

    # AI assist
    ai_enable_scoring: bool = True
    ai_min_confidence: int = 40
    ai_block_on_confidence: bool = False
    ai_memory_path: str = "trade_memory.jsonl"

    # AI selector
    ai_selector_enable: bool = True
    ai_selector_blocking: bool = False
    ai_selector_min_samples: int = 10

    # Learning / history
    history_bars_m5: int = 50000
    history_bars_m15: int = 20000
    history_bars_h1: int = 10000

    enable_statistical_learning: bool = True
    enable_pattern_learning: bool = True
    enable_context_score: bool = True
    enable_pattern_score: bool = True

    context_score_weight: float = 1.0
    pattern_score_weight: float = 1.0
    setup_score_weight: float = 1.0

    learning_refresh_minutes: int = 60
    min_samples_context: int = 20
    min_samples_pattern: int = 20

    min_final_score_paper: int = 45
    min_final_score_live: int = 65
    trend_require_htf: bool = False
    trend_no_htf_penalty: int = 20
    min_minutes_between_signals: int = 15
    max_open_paper_trades: int = 1
    duplicate_entry_atr_tolerance: float = 0.5
    enable_strategy_edge_table: bool = True
    strategy_edge_weight: float = 1.0
    learning_spread_cost_points: int = 10
    learning_slippage_points: int = 5

    store_history_path: str = "data/history"
    store_learning_path: str = "data/learned"
    store_reports_path: str = "reports"

    # Strategy params
    ma_fast: int = 50
    ma_trend: int = 100
    rsi_period: int = 14
    atr_period: int = 14
    pullback_atr_mult: float = 2.0
    rsi_long_max: float = 65.0
    rsi_short_min: float = 38.0
    require_breakout: bool = False
    trend_min_score: int = 60
    paper_trend_score_relax: int = 5
    paper_allow_near_signals: bool = False
    paper_near_extra_score: int = 5
    trend_near_signal_gap: int = 6
    trend_allow_short: bool = False
    trend_short_extra_score: int = 10
    swing_lookback: int = 10
    sl_atr_buffer_mult: float = 0.25
    range_lookback: int = 60
    range_touch_tol_mult: float = 0.2
    range_min_atr_ratio: float = 1.0
    range_max_atr_ratio: float = 20.0
    range_max_atr_ratio_percentile: float = 0.95
    range_atr_ratio_percentile_window: int = 240
    range_required_touches: int = 2
    range_mid_low: float = 0.40
    range_mid_high: float = 0.60
    range_weak_body_min: float = 0.15
    range_break_buffer_mult: float = 0.1
    range_wick_body_min: float = 1.35

    # Management
    be_buffer_points: int = 2
    be_trigger_r: float = 1.2
    trail_atr_mult: float = 1.0
    trail_step_atr_mult: float = 0.25

    # TP profile
    tp1_r_multiple: float = 1.0
    tp2_r_multiple: float = 2.2
    tp3_r_multiple: float = 3.2
    tp1_size_ratio: float = 0.30
    tp2_size_ratio: float = 0.35
    tp3_size_ratio: float = 0.35

    # Loop
    entry_check_seconds: float = 1.0  # check new bar via tick time
    manage_seconds: float = 1.0

    # Session
    session_start_hour: int = 7
    session_end_hour: int = 20

    def apply_overrides(self, raw: dict) -> None:
        for key in self.__dataclass_fields__:
            if key not in raw:
                continue

            value = raw.get(key)
            if value is None:
                setattr(self, key, None)
                continue

            current = getattr(self, key)
            try:
                if isinstance(current, bool):
                    setattr(self, key, bool(value))
                elif isinstance(current, int):
                    setattr(self, key, int(value))
                elif isinstance(current, float):
                    setattr(self, key, float(value))
                elif isinstance(current, str):
                    setattr(self, key, str(value))
                else:
                    setattr(self, key, value)
            except (TypeError, ValueError):
                setattr(self, key, value)

    def to_dict(self) -> dict:
        return {key: getattr(self, key) for key in self.__dataclass_fields__}
