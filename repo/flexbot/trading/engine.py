import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
import MetaTrader5 as mt5

from flexbot.core.config import BotConfig
from flexbot.mt5 import client
from flexbot.strategy.trend_pullback_v1 import get_intent
from flexbot.trading.state import BatchState, load_state, save_state
from flexbot.trading.execution import open_batch
from flexbot.trading.manager import manage_batch

@dataclass
class EngineStatus:
    running: bool = False
    last_msg: str = "idle"
    equity: float = 0.0
    daily_dd: float = 0.0
    consec_losses: int = 0

class TradingEngine:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.lock = threading.Lock()

        self.state = load_state()
        self.last_closed_bar_time = 0
        self.last_batch_id = self.state.batch_id or ""
        self.equity_day_start = 0.0
        self.current_day = None
        self.consec_losses = 0
        self.trading_disabled_today = False

        self.status = EngineStatus()
        self._last_diag_err_ts = 0.0
        self._last_market_closed_ts = 0.0

    def start(self):
        terminal = client.initialize(
            terminal_path=self.cfg.terminal_path,
            login=self.cfg.mt5_login,
            password=self.cfg.mt5_password,
            server=self.cfg.mt5_server,
        )
        self.cfg.symbol = client.resolve_symbol(self.cfg.symbol, auto_resolve=self.cfg.auto_resolve_symbol)
        logging.info("ENGINE_MT5_READY terminal=%s symbol=%s", terminal, self.cfg.symbol)
        self._reset_day_if_needed(force=True)

        self.stop_event.clear()
        threading.Thread(target=self._entry_loop, daemon=True, name="entry-loop").start()
        threading.Thread(target=self._manage_loop, daemon=True, name="manage-loop").start()
        self.status.running = True
        logging.info("ENGINE_STARTED")

    def stop(self):
        self.stop_event.set()
        self.status.running = False
        client.shutdown()
        logging.info("ENGINE_STOPPED")

    def _reset_day_if_needed(self, force: bool = False):
        now = client.broker_datetime_utc(self.cfg.symbol)
        day = now.date()
        if force or self.current_day != day:
            self.current_day = day
            self.equity_day_start = client.account_equity()
            self.consec_losses = 0
            self.trading_disabled_today = False
            logging.info(f"NEW_DAY day={day} equity_start={self.equity_day_start}")

    def _update_guards(self):
        self._reset_day_if_needed()
        eq = client.account_equity()
        dd = (eq - self.equity_day_start) / self.equity_day_start if self.equity_day_start else 0.0
        self.status.equity = eq
        self.status.daily_dd = dd
        self.status.consec_losses = self.consec_losses

        if dd <= -(self.cfg.daily_stop_percent / 100.0):
            if not self.trading_disabled_today:
                logging.warning(f"DAILY_STOP triggered dd={dd:.4f}")
            self.trading_disabled_today = True

        if self.consec_losses >= self.cfg.max_consec_loss:
            if not self.trading_disabled_today:
                logging.warning(f"CONSEC_LOSS_STOP triggered losses={self.consec_losses}")
            self.trading_disabled_today = True

    def _spread_ok(self) -> bool:
        try:
            diag = client.get_symbol_diagnostics(self.cfg.symbol)
            return diag.spread_points <= self.cfg.max_spread_points
        except Exception as e:
            import time
            now = time.time()
            # avoid log spam
            if now - self._last_diag_err_ts > 15:
                self._last_diag_err_ts = now
                logging.error(f"SYMBOL_DIAG_ERROR: {e}")
            msg = str(e)
            if "tick not available" in msg.lower() or "market is closed" in msg.lower():
                import time
                now = time.time()
                if now - self._last_market_closed_ts > 30:
                    self._last_market_closed_ts = now
                    logging.info("MARKET_DATA_UNAVAILABLE symbol=%s", self.cfg.symbol)
                self.status.last_msg = "market_closed/no_ticks"
            else:
                self.status.last_msg = f"symbol_error: {e}"
            return False

    def _can_enter(self) -> bool:
        if self.trading_disabled_today:
            return False
        if self.state.batch_id:
            return False
        if not self._spread_ok():
            return False
        return True

    def _entry_loop(self):
        while not self.stop_event.is_set():
            try:
                self._update_guards()
                if self._can_enter():
                    # detect closed bar time
                    rates = client.copy_rates(self.cfg.symbol, self.cfg.timeframe, 5)
                    if rates is not None and len(rates) >= 3:
                        closed_bar_time = int(rates[-2]["time"])
                    else:
                        closed_bar_time = 0

                    intent = get_intent(
                        symbol=self.cfg.symbol,
                        timeframe=self.cfg.timeframe,
                        ma_fast=self.cfg.ma_fast,
                        ma_trend=self.cfg.ma_trend,
                        rsi_period=self.cfg.rsi_period,
                        atr_period=self.cfg.atr_period,
                        pullback_atr_mult=self.cfg.pullback_atr_mult,
                        rsi_long_max=self.cfg.rsi_long_max,
                        rsi_short_min=self.cfg.rsi_short_min,
                        swing_lookback=self.cfg.swing_lookback,
                        sl_atr_buffer_mult=self.cfg.sl_atr_buffer_mult,
                        last_closed_bar_time=self.last_closed_bar_time,
                    )
                    if intent.valid and intent.batch_id != self.last_batch_id and closed_bar_time:
                        if self.cfg.paper_mode:
                            logging.info(f"PAPER_SIGNAL batch_id={intent.batch_id} side={'BUY' if intent.is_long else 'SELL'} sl={intent.sl:.5f}")
                            self.last_batch_id = intent.batch_id
                            self.last_closed_bar_time = closed_bar_time
                            self.status.last_msg = "paper_signal_logged"
                            continue
                        # record this bar time to prevent repeated
                        self.last_closed_bar_time = closed_bar_time
                        self.last_batch_id = intent.batch_id

                        # open batch
                        st, res = open_batch(
                            symbol=self.cfg.symbol,
                            magic=self.cfg.magic,
                            batch_id=intent.batch_id,
                            is_long=intent.is_long,
                            sl=intent.sl,
                            risk_percent=self.cfg.risk_percent,
                            be_buf_points=self.cfg.be_buffer_points,
                        )
                        if res.ok:
                            self.state = st
                            self.status.last_msg = f"Opened {intent.batch_id}"
                        else:
                            self.status.last_msg = f"Entry failed: {res.msg}"
                            logging.error(self.status.last_msg)
                    else:
                        self.status.last_msg = intent.reason
                else:
                    self.status.last_msg = "guards_blocked"
            except Exception as e:
                logging.exception(f"ENTRY_LOOP_ERROR: {e}")
                self.status.last_msg = f"error: {e}"
            self.stop_event.wait(self.cfg.entry_check_seconds)

    def _manage_loop(self):
        while not self.stop_event.is_set():
            try:
                self._update_guards()
                if self.state.batch_id:
                    prev = self.state
                    self.state = manage_batch(
                        state=self.state,
                        be_buffer_points=self.cfg.be_buffer_points,
                        trail_atr_mult=self.cfg.trail_atr_mult,
                        trail_step_atr_mult=self.cfg.trail_step_atr_mult,
                        atr_period=self.cfg.atr_period,
                        timeframe=self.cfg.timeframe,
                    )
                    if not self.state.batch_id and prev.batch_id:
                        # batch finished -> compute batch profit by summing deals with batch_id
                        now = client.broker_datetime_utc(self.cfg.symbol)
                        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                        deals = client.history_deals(day_start, now)
                        pnl = 0.0
                        key = f"FlexBot|{prev.batch_id}|"
                        for d in deals:
                            if getattr(d, "symbol", "") != self.cfg.symbol:
                                continue
                            c = getattr(d, "comment", "") or ""
                            if key in c:
                                pnl += float(getattr(d, "profit", 0.0))
                        if pnl < 0:
                            self.consec_losses += 1
                        else:
                            self.consec_losses = 0
                        logging.info(f"BATCH_PNL id={prev.batch_id} pnl={pnl} consec_losses={self.consec_losses}")
                else:
                    # try recover from open positions if state empty
                    pass
            except Exception as e:
                logging.exception(f"MANAGE_LOOP_ERROR: {e}")
            self.stop_event.wait(self.cfg.manage_seconds)
