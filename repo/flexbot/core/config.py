from dataclasses import dataclass


@dataclass
class BotConfig:
    symbol: str = "XAUUSD"
    timeframe: str = "M5"
    risk_percent: float = 0.5
    daily_stop_percent: float = 2.0
    max_consec_loss: int = 2
    max_spread_points: int = 35
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
    ai_min_confidence: int = 80
    ai_memory_path: str = "trade_memory.jsonl"

    # Strategy params
    ma_fast: int = 50
    ma_trend: int = 200
    rsi_period: int = 14
    atr_period: int = 14
    pullback_atr_mult: float = 2.5
    rsi_long_max: float = 65.0
    rsi_short_min: float = 35.0
    require_breakout: bool = True
    swing_lookback: int = 10
    sl_atr_buffer_mult: float = 0.25

    # Management
    be_buffer_points: int = 2
    trail_atr_mult: float = 1.0
    trail_step_atr_mult: float = 0.25

    # Loop
    entry_check_seconds: float = 1.0  # check new bar via tick time
    manage_seconds: float = 1.0
