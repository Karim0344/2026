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
    ai_min_confidence: int = 45
    ai_block_on_confidence: bool = False
    ai_memory_path: str = "trade_memory.jsonl"

    # AI selector
    ai_selector_enable: bool = True
    ai_selector_blocking: bool = False
    ai_selector_min_samples: int = 10

    # Strategy params
    ma_fast: int = 50
    ma_trend: int = 100
    rsi_period: int = 14
    atr_period: int = 14
    pullback_atr_mult: float = 2.0
    rsi_long_max: float = 65.0
    rsi_short_min: float = 38.0
    require_breakout: bool = False
    trend_min_score: int = 65
    swing_lookback: int = 10
    sl_atr_buffer_mult: float = 0.25
    range_lookback: int = 60
    range_touch_tol_mult: float = 0.2
    range_min_atr_ratio: float = 1.3
    range_max_atr_ratio: float = 6.5
    range_required_touches: int = 1
    range_mid_low: float = 0.35
    range_mid_high: float = 0.65
    range_weak_body_min: float = 0.15
    range_break_buffer_mult: float = 0.1
    range_wick_body_min: float = 1.15

    # Management
    be_buffer_points: int = 2
    trail_atr_mult: float = 1.0
    trail_step_atr_mult: float = 0.25

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
