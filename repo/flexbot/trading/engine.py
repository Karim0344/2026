import logging
import threading
import time
from dataclasses import dataclass
import MetaTrader5 as mt5

from flexbot.core.config import BotConfig
from flexbot.mt5 import client
from flexbot.strategy.trend_pullback_v1 import TradeIntent, get_intent as trend_intent
from flexbot.strategy.range_rejection import get_range_intent
from flexbot.trading.state import BatchState, load_state, save_state
from flexbot.trading.execution import open_batch
from flexbot.trading.manager import manage_batch
from flexbot.trading.paper_tracker import (
    PaperTrade,
    update_open_paper_trades,
    upsert_paper_trade,
    load_paper_stats,
)
from flexbot.ai.features import build_feature_snapshot
from flexbot.ai.scoring import confidence_score
from flexbot.ai.memory import log_trade_open, log_trade_close
from flexbot.ai.optimizer import analyze_memory
from flexbot.ai.regime import detect_regime
from flexbot.ai.selector import selector_adjustment


@dataclass
class EngineStatus:
    running: bool = False
    last_msg: str = "idle"
    equity: float = 0.0
    daily_dd: float = 0.0
    consec_losses: int = 0
    loop_state: str = "idle"
    last_eval_reason: str = "none"
    last_eval_bar_time: int = 0
    signal_count: int = 0

    paper_total: int = 0
    paper_open: int = 0
    paper_closed: int = 0
    paper_winrate: float = 0.0
    paper_avg_r: float = 0.0
    paper_tp1: int = 0
    paper_tp2: int = 0
    paper_tp3: int = 0
    paper_sl: int = 0


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
        self.signal_count = 0

        self.status = EngineStatus()
        self._last_diag_err_ts = 0.0
        self._last_market_closed_ts = 0.0
        self._last_heartbeat = 0.0
        self._last_strategy_reason: str | None = None
        self._entry_thread: threading.Thread | None = None
        self._manage_thread: threading.Thread | None = None
        self._last_optimizer_check = 0.0

    def start(self):
        mt5_initialize_started = False
        try:
            mt5_initialize_started = True
            terminal = client.initialize(
                terminal_path=self.cfg.terminal_path,
                login=self.cfg.mt5_login,
                password=self.cfg.mt5_password,
                server=self.cfg.mt5_server,
            )
            self.cfg.symbol = client.resolve_symbol(
                self.cfg.symbol, auto_resolve=self.cfg.auto_resolve_symbol
            )
            logging.info(
                "ENGINE_MT5_READY terminal=%s symbol=%s", terminal, self.cfg.symbol
            )
            self._reset_day_if_needed(force=True)

            self.stop_event.clear()
            self._entry_thread = threading.Thread(
                target=self._entry_loop, daemon=True, name="entry-loop"
            )
            self._manage_thread = threading.Thread(
                target=self._manage_loop, daemon=True, name="manage-loop"
            )
            self._entry_thread.start()
            self._manage_thread.start()
            self.status.running = True
            logging.info("ENGINE_STARTED")
        except Exception:
            self.stop_event.set()
            self._join_worker_threads(timeout=1.0)
            self.status.running = False
            if mt5_initialize_started:
                self._shutdown_mt5()
            raise

    def stop(self):
        self.stop_event.set()
        self._join_worker_threads(timeout=2.0)
        self.status.running = False
        self._shutdown_mt5()
        logging.info("ENGINE_STOPPED")

    def _shutdown_mt5(self):
        client.shutdown()
        logging.info("MT5 shutdown complete")

    def _join_worker_threads(self, timeout: float):
        for attr in ("_entry_thread", "_manage_thread"):
            t = getattr(self, attr)
            if t is not None and t.is_alive() and t is not threading.current_thread():
                t.join(timeout=timeout)
            setattr(self, attr, None)

    def _reset_day_if_needed(self, force: bool = False):
        now = client.broker_datetime_utc(self.cfg.symbol)
        day = now.date()
        if force or self.current_day != day:
            self.current_day = day
            self.equity_day_start = client.account_equity()
            self.consec_losses = 0
            self.trading_disabled_today = False
            self.signal_count = 0
            logging.info(f"NEW_DAY day={day} equity_start={self.equity_day_start}")


    def _refresh_paper_stats(self):
        stats = load_paper_stats()
        self.status.paper_total = int(stats.get("total", 0))
        self.status.paper_open = int(stats.get("open", 0))
        self.status.paper_closed = int(stats.get("closed", 0))
        self.status.paper_winrate = float(stats.get("winrate", 0.0))
        self.status.paper_avg_r = float(stats.get("avg_r", 0.0))
        self.status.paper_tp1 = int(stats.get("tp1", 0))
        self.status.paper_tp2 = int(stats.get("tp2", 0))
        self.status.paper_tp3 = int(stats.get("tp3", 0))
        self.status.paper_sl = int(stats.get("losses", 0))

    def _update_guards(self):
        self._reset_day_if_needed()
        eq = client.account_equity()
        dd = (
            (eq - self.equity_day_start) / self.equity_day_start
            if self.equity_day_start
            else 0.0
        )
        self.status.equity = eq
        self.status.daily_dd = dd
        self.status.consec_losses = self.consec_losses
        self.status.signal_count = self.signal_count
        self._refresh_paper_stats()

        if dd <= -(self.cfg.daily_stop_percent / 100.0):
            if not self.trading_disabled_today:
                logging.warning(f"DAILY_STOP triggered dd={dd:.4f}")
            self.trading_disabled_today = True

        if self.consec_losses >= self.cfg.max_consec_loss:
            if not self.trading_disabled_today:
                logging.warning(
                    f"CONSEC_LOSS_STOP triggered losses={self.consec_losses}"
                )
            self.trading_disabled_today = True
        self._performance_guard()

        now_ts = time.time()
        if now_ts - self._last_optimizer_check >= 1800:
            self._last_optimizer_check = now_ts
            analysis = analyze_memory(self.cfg.ai_memory_path)
            if analysis.get("total", 0) >= 30:
                for msg in analysis.get("suggestions", []):
                    logging.info("AI_OPTIMIZER %s", msg)

    def _performance_guard(self):
        stats = load_paper_stats()
        closed = int(stats.get("closed", 0))
        avg_r = float(stats.get("avg_r", 0.0))
        winrate = float(stats.get("winrate", 0.0))

        if closed > 20 and avg_r < 0:
            if not self.trading_disabled_today:
                logging.warning("PERF_STOP avg_r=%.2f closed=%s", avg_r, closed)
            self.trading_disabled_today = True

        if closed > 20 and winrate < 40:
            logging.warning("PERF_WEAK winrate=%.2f closed=%s", winrate, closed)

    def _spread_ok(self) -> bool:
        try:
            diag = client.get_symbol_diagnostics(self.cfg.symbol)
            return diag.spread_points <= self.cfg.max_spread_points
        except Exception as e:
            import time

            msg = str(e)
            msg_l = msg.lower()
            if "tick not available" in msg_l or "market is closed" in msg_l:
                now = time.time()
                if now - self._last_market_closed_ts > 120:
                    self._last_market_closed_ts = now
                    logging.info("MARKET_DATA_UNAVAILABLE symbol=%s", self.cfg.symbol)
                self.status.last_msg = "market_closed/no_ticks"
            else:
                now = time.time()
                if now - self._last_diag_err_ts > 30:
                    self._last_diag_err_ts = now
                    logging.error(f"SYMBOL_DIAG_ERROR: {e}")
                self.status.last_msg = f"symbol_error: {e}"
            return False

    def _can_enter(self) -> tuple[bool, str]:
        if self.trading_disabled_today:
            if self.status.daily_dd <= -(self.cfg.daily_stop_percent / 100.0):
                return False, "daily_stop_blocked"
            if self.consec_losses >= self.cfg.max_consec_loss:
                return False, "consec_loss_blocked"
            return False, "trading_disabled"

        if self.state.batch_id:
            return False, "batch_open_blocked"

        try:
            diag = client.get_symbol_diagnostics(self.cfg.symbol)
            if diag.spread_points > self.cfg.max_spread_points:
                return (
                    False,
                    f"spread_blocked:{diag.spread_points}>{self.cfg.max_spread_points}",
                )
        except Exception as e:
            msg = str(e).lower()
            if "tick not available" in msg or "market is closed" in msg:
                return False, "market_closed/no_ticks"
            return False, f"symbol_error:{e}"

        return True, "ok"

    def _log_strategy_heartbeat(self):
        now = time.time()
        if now - self._last_heartbeat > 30:
            self._last_heartbeat = now
            logging.info(
                "HEARTBEAT symbol=%s tf=%s loop_state=%s last_eval_reason=%s signal_count=%s equity=%.2f",
                self.cfg.symbol,
                self.cfg.timeframe,
                self.status.loop_state,
                self.status.last_eval_reason,
                self.status.signal_count,
                self.status.equity,
            )

    def _log_strategy_reason_change(self, reason: str):
        if reason != self._last_strategy_reason:
            self._last_strategy_reason = reason
            logging.info("STATE_CHANGE reason=%s", reason)

    def _entry_loop(self):
        while not self.stop_event.is_set():
            try:
                self._update_guards()
                can_enter, guard_reason = self._can_enter()
                if not can_enter:
                    self.status.loop_state = guard_reason
                    self.status.last_msg = guard_reason
                    self._log_strategy_reason_change(guard_reason)
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue
                now_utc = client.broker_datetime_utc(self.cfg.symbol)
                hour = now_utc.hour
                if hour < self.cfg.session_start_hour or hour > self.cfg.session_end_hour:
                    self.status.loop_state = "session_blocked"
                    self.status.last_msg = "session_blocked"
                    self._log_strategy_reason_change("session_blocked")
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue

                rates = client.copy_rates(self.cfg.symbol, self.cfg.timeframe, 5)
                if rates is None or len(rates) < 3:
                    self.status.loop_state = "no_rates"
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue

                closed_bar_time = int(rates[-2]["time"])
                if not closed_bar_time:
                    self.status.loop_state = "no_rates"
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue

                if closed_bar_time == self.last_closed_bar_time:
                    self.status.loop_state = "same_bar"
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue

                self.status.loop_state = "new_bar"
                logging.info(
                    "NEW_BAR symbol=%s tf=%s closed_bar_time=%s",
                    self.cfg.symbol,
                    self.cfg.timeframe,
                    closed_bar_time,
                )

                closed_bar = rates[-2]
                bar_high = float(closed_bar["high"])
                bar_low = float(closed_bar["low"])
                updates = update_open_paper_trades(
                    symbol=self.cfg.symbol,
                    timeframe=self.cfg.timeframe,
                    bar_time=closed_bar_time,
                    bar_high=bar_high,
                    bar_low=bar_low,
                )
                for trade in updates:
                    if trade.status in ("sl_hit", "tp1_hit", "tp2_hit", "tp3_hit"):
                        rr_realized = float(getattr(trade, "result_r", 0.0))
                        log_trade_close(
                            trade=trade,
                            result_r=rr_realized,
                            path=self.cfg.ai_memory_path,
                        )
                        logging.info(
                            "PAPER_CLOSE batch_id=%s final_status=%s exit_reason=%s rr_realized=%.2f mfe_r=%.2f mae_r=%.2f",
                            trade.batch_id,
                            trade.status,
                            trade.exit_reason,
                            rr_realized,
                            float(getattr(trade, "mfe_r", 0.0)),
                            float(getattr(trade, "mae_r", 0.0)),
                        )
                    else:
                        logging.info(
                            "PAPER_UPDATE batch_id=%s status=%s",
                            trade.batch_id,
                            trade.status,
                        )

                self._refresh_paper_stats()
                logging.info(
                    "PAPER_STATS total=%s open=%s closed=%s winrate=%.2f avg_r=%.2f tp1=%s tp2=%s tp3=%s sl=%s",
                    self.status.paper_total,
                    self.status.paper_open,
                    self.status.paper_closed,
                    self.status.paper_winrate,
                    self.status.paper_avg_r,
                    self.status.paper_tp1,
                    self.status.paper_tp2,
                    self.status.paper_tp3,
                    self.status.paper_sl,
                )
                stats = load_paper_stats()
                logging.info(
                    "PAPER_STRATEGY_SUMMARY by_strategy=%s by_side=%s",
                    stats.get("by_strategy", {}),
                    stats.get("by_side", {}),
                )

                regime, regime_debug = detect_regime(
                    self.cfg.symbol,
                    self.cfg.timeframe,
                    ma_fast=self.cfg.ma_fast,
                    ma_slow=self.cfg.ma_trend,
                )

                if regime == "dead":
                    self.last_closed_bar_time = closed_bar_time
                    self.status.last_eval_bar_time = closed_bar_time
                    self.status.last_eval_reason = "skip_dead_regime"
                    self.status.loop_state = "skip_dead_regime"
                    self.status.last_msg = "skip_dead_regime"
                    logging.info("REGIME_SKIP regime=%s debug=%s", regime, regime_debug)
                    self._log_strategy_reason_change(self.status.last_msg)
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue

                if regime == "high_volatility":
                    self.last_closed_bar_time = closed_bar_time
                    self.status.last_eval_bar_time = closed_bar_time
                    self.status.last_eval_reason = "skip_high_vol"
                    self.status.loop_state = "skip_high_vol"
                    self.status.last_msg = "skip_high_vol"
                    logging.info("REGIME_SKIP regime=%s debug=%s", regime, regime_debug)
                    self._log_strategy_reason_change(self.status.last_msg)
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue

                if regime in ("trend", "trend_overextended"):
                    intent = trend_intent(
                        symbol=self.cfg.symbol,
                        timeframe=self.cfg.timeframe,
                        cfg=self.cfg,
                        last_closed_bar_time=self.last_closed_bar_time,
                    )
                elif regime == "range":
                    range_intent = get_range_intent(self.cfg.symbol, self.cfg.timeframe, self.cfg)
                    is_long = range_intent.direction == "long"
                    is_short = range_intent.direction == "short"
                    intent = TradeIntent(
                        valid=is_long or is_short,
                        is_long=is_long,
                        entry=range_intent.entry,
                        sl=range_intent.sl,
                        batch_id=f"{self.cfg.symbol}_{self.cfg.timeframe}_{closed_bar_time}_range",
                        reason=range_intent.reason,
                        debug=range_intent.debug,
                    )
                elif regime in ("range_breakout_pressure_up", "range_breakout_pressure_down"):
                    self.last_closed_bar_time = closed_bar_time
                    self.status.last_eval_bar_time = closed_bar_time
                    self.status.last_eval_reason = f"skip_{regime}"
                    self.status.loop_state = f"skip_{regime}"
                    self.status.last_msg = f"skip_{regime}"
                    logging.info("REGIME_SKIP regime=%s debug=%s", regime, regime_debug)
                    self._log_strategy_reason_change(self.status.last_msg)
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue
                else:
                    self.status.loop_state = f"skip:{regime}"
                    self.status.last_msg = f"skip:{regime}"
                    self._log_strategy_reason_change(self.status.last_msg)
                    self._log_strategy_heartbeat()
                    self.stop_event.wait(self.cfg.entry_check_seconds)
                    continue

                intent.debug = {**(intent.debug or {}), "regime": regime, "regime_debug": regime_debug}

                # Always mark current closed bar as processed, even when no signal.
                self.last_closed_bar_time = closed_bar_time
                self.status.last_eval_bar_time = closed_bar_time
                self.status.last_eval_reason = intent.reason
                self.status.last_msg = intent.reason
                logging.info(
                    "BAR_RESULT symbol=%s tf=%s closed_bar_time=%s reason=%s valid=%s batch_id=%s",
                    self.cfg.symbol,
                    self.cfg.timeframe,
                    closed_bar_time,
                    intent.reason,
                    intent.valid,
                    intent.batch_id,
                )
                if intent.debug:
                    logging.info("INTENT_DEBUG %s", intent.debug)
                self._log_strategy_reason_change(intent.reason)

                if intent.valid and intent.batch_id != self.last_batch_id:
                    spread_points = 0
                    try:
                        spread_points = int(
                            client.get_symbol_diagnostics(self.cfg.symbol).spread_points
                        )
                    except Exception:
                        spread_points = self.cfg.max_spread_points

                    features = build_feature_snapshot(
                        signal_reason=intent.reason,
                        intent_debug=intent.debug,
                        spread_points=spread_points,
                        max_spread_points=self.cfg.max_spread_points,
                        regime=regime,
                    )
                    base_confidence = confidence_score(
                        features=features,
                        is_long=bool(intent.is_long),
                        max_spread=self.cfg.max_spread_points,
                    )

                    selector = {
                        "block": False,
                        "bonus": 0,
                        "reason": "selector_disabled",
                        "samples": 0,
                        "avg_r": 0.0,
                        "winrate": 0.0,
                    }
                    if self.cfg.ai_selector_enable:
                        selector = selector_adjustment(
                            signal_reason=intent.reason,
                            regime=regime,
                            path=self.cfg.ai_memory_path,
                            min_samples=self.cfg.ai_selector_min_samples,
                        )

                    if selector["block"] and bool(getattr(self.cfg, "ai_selector_blocking", False)):
                        self.status.last_msg = f"ai_selector_blocked:{selector['reason']}"
                        self._log_strategy_reason_change(self.status.last_msg)
                        logging.info(
                            "AI_SELECTOR_BLOCK batch_id=%s strategy=%s regime=%s reason=%s samples=%s avg_r=%.2f winrate=%.2f",
                            intent.batch_id,
                            intent.reason,
                            regime,
                            selector["reason"],
                            selector["samples"],
                            selector["avg_r"],
                            selector["winrate"],
                        )
                        self._log_strategy_heartbeat()
                        self.stop_event.wait(self.cfg.entry_check_seconds)
                        continue
                    if selector["block"]:
                        logging.info(
                            "AI_SELECTOR_OBSERVE block=true but bypassed batch_id=%s strategy=%s regime=%s reason=%s",
                            intent.batch_id,
                            intent.reason,
                            regime,
                            selector["reason"],
                        )

                    confidence = max(0, min(100, base_confidence + int(selector["bonus"])))

                    logging.info(
                        "AI_SELECTOR strategy=%s regime=%s base=%s bonus=%s final=%s selector_reason=%s samples=%s avg_r=%.2f winrate=%.2f",
                        intent.reason,
                        regime,
                        base_confidence,
                        selector["bonus"],
                        confidence,
                        selector["reason"],
                        selector["samples"],
                        selector["avg_r"],
                        selector["winrate"],
                    )

                    ai_decision = "pass"
                    if (
                        self.cfg.ai_enable_scoring
                        and bool(getattr(self.cfg, "ai_block_on_confidence", False))
                        and confidence < self.cfg.ai_min_confidence
                    ):
                        ai_decision = "blocked"
                        self.status.last_msg = f"ai_score_blocked:{confidence}<{self.cfg.ai_min_confidence}"
                        self._log_strategy_reason_change(self.status.last_msg)
                        logging.info(
                            "AI_SCORE score=%s decision=%s min=%s reason=%s features=%s",
                            confidence,
                            ai_decision,
                            self.cfg.ai_min_confidence,
                            intent.reason,
                            features,
                        )
                        logging.info(
                            "AI_SCORE_SKIP batch_id=%s score=%s min=%s reason=%s",
                            intent.batch_id,
                            confidence,
                            self.cfg.ai_min_confidence,
                            intent.reason,
                        )
                        self._log_strategy_heartbeat()
                        self.stop_event.wait(self.cfg.entry_check_seconds)
                        continue

                    logging.info(
                        "AI_SCORE score=%s decision=%s min=%s reason=%s features=%s",
                        confidence,
                        ai_decision,
                        self.cfg.ai_min_confidence,
                        intent.reason,
                        features,
                    )
                    self.signal_count += 1
                    self.status.signal_count = self.signal_count
                    logging.info("SIGNAL_COUNT=%s day=%s", self.signal_count, self.current_day)
                    self.last_batch_id = intent.batch_id
                    if self.cfg.paper_mode:
                        entry = 0.0
                        tp1 = 0.0
                        tp2 = 0.0
                        tp3 = 0.0
                        tick = client.get_tick(self.cfg.symbol)
                        if tick is not None:
                            entry = float(tick.ask if intent.is_long else tick.bid)
                            r_value = abs(entry - float(intent.sl))
                            if r_value > 0:
                                tp1 = entry + r_value if intent.is_long else entry - r_value
                                tp2 = entry + (2 * r_value) if intent.is_long else entry - (2 * r_value)
                                tp3 = entry + (3 * r_value) if intent.is_long else entry - (3 * r_value)

                            trade = PaperTrade(
                                batch_id=intent.batch_id,
                                symbol=self.cfg.symbol,
                                timeframe=self.cfg.timeframe,
                                is_long=intent.is_long,
                                entry=entry,
                                sl=float(intent.sl),
                                tp1=float(tp1),
                                tp2=float(tp2),
                                tp3=float(tp3),
                                created_bar_time=closed_bar_time,
                                signal_reason=intent.reason,
                                confidence_score=confidence,
                                features=features,
                                initial_r=abs(entry - float(intent.sl)),
                            )
                            upsert_paper_trade(trade)
                            log_trade_open(trade=trade, path=self.cfg.ai_memory_path)
                            self._refresh_paper_stats()

                        logging.info(
                            "PAPER_SIGNAL batch_id=%s side=%s entry=%.5f sl=%.5f tp1=%.5f tp2=%.5f tp3=%.5f reason=%s",
                            intent.batch_id,
                            "BUY" if intent.is_long else "SELL",
                            entry,
                            intent.sl,
                            tp1,
                            tp2,
                            tp3,
                            intent.reason,
                        )
                        self.status.last_msg = "paper_signal_logged"
                        self._log_strategy_reason_change(self.status.last_msg)
                    else:
                        st, res = open_batch(
                            symbol=self.cfg.symbol,
                            magic=self.cfg.magic,
                            batch_id=intent.batch_id,
                            is_long=intent.is_long,
                            sl=intent.sl,
                            risk_percent=self.cfg.risk_percent,
                            be_buf_points=self.cfg.be_buffer_points,
                            tp1_r_multiple=self.cfg.tp1_r_multiple,
                            tp2_r_multiple=self.cfg.tp2_r_multiple,
                            tp3_r_multiple=self.cfg.tp3_r_multiple,
                            tp1_size_ratio=self.cfg.tp1_size_ratio,
                            tp2_size_ratio=self.cfg.tp2_size_ratio,
                            tp3_size_ratio=self.cfg.tp3_size_ratio,
                        )
                        if res.ok:
                            self.state = st
                            self.status.last_msg = f"Opened {intent.batch_id}"
                            self._log_strategy_reason_change(self.status.last_msg)
                        else:
                            self.status.last_msg = f"Entry failed: {res.msg}"
                            self._log_strategy_reason_change(self.status.last_msg)
                            logging.error(self.status.last_msg)

                self._log_strategy_heartbeat()
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
                        be_trigger_r=self.cfg.be_trigger_r,
                        trail_atr_mult=self.cfg.trail_atr_mult,
                        trail_step_atr_mult=self.cfg.trail_step_atr_mult,
                        atr_period=self.cfg.atr_period,
                        timeframe=self.cfg.timeframe,
                    )
                    if not self.state.batch_id and prev.batch_id:
                        # batch finished -> compute batch profit by summing deals with batch_id
                        now = client.broker_datetime_utc(self.cfg.symbol)
                        day_start = now.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
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
                        logging.info(
                            f"BATCH_PNL id={prev.batch_id} pnl={pnl} consec_losses={self.consec_losses}"
                        )
                else:
                    # try recover from open positions if state empty
                    pass
            except Exception as e:
                logging.exception(f"MANAGE_LOOP_ERROR: {e}")
            self.stop_event.wait(self.cfg.manage_seconds)
