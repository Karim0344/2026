from dataclasses import dataclass


@dataclass
class BotConfig:
    symbol: str = "XAUUSD"
    timeframe: str = "H1"  # "H1" or "H4"
    risk_percent: float = 1.0
    daily_stop_percent: float = 3.0
    max_consec_loss: int = 3
    max_spread_points: int = 35
    magic: int = 26022026

    # MT5
    terminal_path: str = ""
    auto_resolve_symbol: bool = True
    mt5_login: int | None = None
    mt5_password: str = ""
    mt5_server: str = ""

    # Modes
    paper_mode: bool = False  # dry-run: log signals, do not send orders

    # Strategy params
    ma_fast: int = 50
    ma_trend: int = 200
    rsi_period: int = 14
    atr_period: int = 14
    pullback_atr_mult: float = 0.5
    rsi_long_max: float = 40.0
    rsi_short_min: float = 60.0
    swing_lookback: int = 10
    sl_atr_buffer_mult: float = 0.2

    # Management
    be_buffer_points: int = 2
    trail_atr_mult: float = 1.2
    trail_step_atr_mult: float = 0.3

    # Loop
    entry_check_seconds: float = 1.0  # check new bar via tick time
    manage_seconds: float = 1.0
