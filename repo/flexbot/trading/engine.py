import logging
import threading
import time
from dataclasses import dataclass
from collections import Counter
from datetime import datetime, timezone
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
    load_paper_trades,
)
from flexbot.ai.features import build_feature_snapshot
from flexbot.ai.scoring import confidence_score
from flexbot.ai.memory import log_trade_open, log_trade_close
from flexbot.ai.optimizer import analyze_memory
from flexbot.ai.regime import detect_regime
from flexbot.ai.selector import selector_adjustment
from flexbot.ai.context_scorer import ContextScorer
from flexbot.ai.pattern_scorer import PatternScorer
from flexbot.ai.strategy_edge_scorer import StrategyEdgeScorer
from flexbot.ai.learning_pipeline import LearningPipeline
from flexbot.reporting.run_summary import save_run_summary


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
    session_block_count: int = 0
    spread_block_count: int = 0
    processed_bars: int = 0
    loop_checks: int = 0
    bars_seen: int = 0
    bars_in_session: int = 0
    bars_spread_ok: int = 0
    bars_evaluated: int = 0
    candidate_bars: int = 0
    signal_bars: int = 0
    run_id: str = ""
    current_run_total: int = 0
    current_run_closed: int = 0
    all_time_total: int = 0


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

        self.run_started_at = datetime.now(timezone.utc)
        self.run_id = self.run_started_at.strftime("%Y%m%dT%H%M%SZ")
        self.build_version = "flexbot"
        self.status = EngineStatus(run_id=self.run_id)
        self._last_diag_err_ts = 0.0
        self._last_market_closed_ts = 0.0
        self._last_heartbeat = 0.0
        self._last_strategy_reason: str | None = None
        self._entry_thread: threading.Thread | None = None
        self._manage_thread: threading.Thread | None = None
        self._last_optimizer_check = 0.0
        self.session_block_count = 0
        self.spread_block_count = 0
        self.processed_bars = 0
        self.loop_checks = 0
        self.bars_seen = 0
        self.bars_in_session = 0
        self.bars_spread_ok = 0
        self.bars_evaluated = 0
        self.candidate_bars = 0
        self.signal_bars = 0
        self._last_block_diag = 0.0
        self._last_learning_refresh = 0.0
        self.eval_window_bars = 100
        self.window_processed_bars = 0
        self.window_candidate_signals = 0
        self.window_true_signals = 0
        self.window_near_signals = 0
        self.window_reject_reasons: Counter[str] = Counter()
        self.context_scorer = ContextScorer(
            store_learning_path=self.cfg.store_learning_path,
            cfg=self.cfg,
            weight=self.cfg.context_score_weight,
        )
        self.pattern_scorer = PatternScorer(
            store_learning_path=self.cfg.store_learning_path,
            cfg=self.cfg,
            weight=self.cfg.pattern_score_weight,
        )
        self.learning_pipeline = LearningPipeline(cfg=self.cfg)
        self._learning_lock = threading.Lock()
        self._learning_thread: threading.Thread | None = None
        self.strategy_edge_scorer = StrategyEdgeScorer(
            store_learning_path=self.cfg.store_learning_path,
            cfg=self.cfg,
            weight=self.cfg.strategy_edge_weight,
        )
        self.last_signal_ts: float = 0.0
        self.range_no_signal_count: int = 0

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
            if getattr(self.cfg, "learning_pipeline_mode", "manual") == "startup":
                self._refresh_learning_tables_if_needed(force=True)

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
        self._write_run_summary()
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
        stats = load_paper_stats(run_id=self.run_id)
        self.status.paper_total = int(stats.get("total", 0))
        self.status.paper_open = int(stats.get("open", 0))
        self.status.paper_closed = int(stats.get("closed", 0))
        self.status.paper_winrate = float(stats.get("winrate", 0.0))
        self.status.paper_avg_r = float(stats.get("avg_r", 0.0))
        self.status.paper_tp1 = int(stats.get("tp1", 0))
        self.status.paper_tp2 = int(stats.get("tp2", 0))
        self.status.paper_tp3 = int(stats.get("tp3", 0))
        self.status.paper_sl = int(stats.get("losses", 0))
        self.status.current_run_total = int(stats.get("current_run_total", self.status.paper_total))
        self.status.current_run_closed = int(stats.get("current_run_closed", self.status.paper_closed))
        self.status.all_time_total = int(stats.get("all_time_total", self.status.paper_total))

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

    def _refresh_learning_tables_if_needed(self, force: bool = False):
        now_ts = time.time()
        refresh_s = max(60, int(self.cfg.learning_refresh_minutes) * 60)
        if (not force) and (now_ts - self._last_learning_refresh) < refresh_s:
            return
        self._last_learning_refresh = now_ts
        if not self._learning_lock.acquire(blocking=False):
            logging.info("LEARNING_PIPELINE_RUNNING skip_refresh=true")
            return
        try:
            if self.cfg.enable_statistical_learning or self.cfg.enable_pattern_learning:
                result = self.learning_pipeline.run(symbol=self.cfg.symbol)
                logging.info(
                    "LEARNING_PIPELINE_DONE history_rows=%s feature_rows=%s outcome_rows=%s context_rows=%s pattern_rows=%s strategy_rows=%s history_path=%s features_path=%s outcomes_path=%s context_path=%s pattern_path=%s summary_path=%s",
                    result.history_rows,
                    result.feature_rows,
                    result.outcome_rows,
                    result.context_rows,
                    result.pattern_rows,
                    result.strategy_rows,
                    result.history_path,
                    result.features_path,
                    result.outcomes_path,
                    result.context_path,
                    result.pattern_path,
                    result.summary_path,
                )
            self.context_scorer.refresh()
            self.pattern_scorer.refresh()
            self.strategy_edge_scorer.refresh()
            logging.info(
                "LEARNING_TABLES_REFRESHED path=%s", self.cfg.store_learning_path
            )
        except Exception as e:
            logging.warning("LEARNING_TABLE_REFRESH_FAILED err=%s", e)
        finally:
            self._learning_lock.release()


    def _log_candidate_eval(self, *, intent, regime: str, closed_bar_time: int, decision: str, reject_reason: str):
        try:
            spread_points = int(client.get_symbol_diagnostics(self.cfg.symbol).spread_points)
        except Exception:
            spread_points = self.cfg.max_spread_points
        side = "long" if bool(getattr(intent, "is_long", False)) else "short"
        reason_l = str(getattr(intent, "reason", "") or "").lower()
        if not bool(getattr(intent, "is_long", False)) and not bool(getattr(intent, "is_short", False)):
            side = "short" if ("short" in reason_l or "top" in reason_l) else "long"

        features = build_feature_snapshot(
            signal_reason=intent.reason,
            intent_debug=intent.debug,
            spread_points=spread_points,
            max_spread_points=self.cfg.max_spread_points,
            regime=regime,
            strategy_name=intent.reason or "candidate",
            side=side,
            symbol=self.cfg.symbol,
            timeframe=self.cfg.timeframe,
            bar_time=closed_bar_time,
        )
        base_confidence = confidence_score(features=features, is_long=(side == "long"), max_spread=self.cfg.max_spread_points)
        setup_score = int(round(base_confidence * float(self.cfg.setup_score_weight)))
        context_score, _ = (0, "context_disabled")
        if self.cfg.enable_statistical_learning and self.cfg.enable_context_score:
            context_score, _ = self.context_scorer.score(lookup=features, min_samples=self.cfg.min_samples_context)
        pattern_score, _ = (0, "pattern_disabled")
        if self.cfg.enable_pattern_learning and self.cfg.enable_pattern_score:
            pattern_score, _ = self.pattern_scorer.score(lookup=features, min_samples=self.cfg.min_samples_pattern)
        strategy_edge_score, _ = (0, "strategy_edge_disabled")
        if self.cfg.enable_strategy_edge_table:
            strategy_edge_score, _ = self.strategy_edge_scorer.score(lookup=features, min_samples=self.cfg.strategy_edge_min_samples)
        selector_bonus = 0
        spread_penalty = 0 if spread_points < self.cfg.max_spread_points else 8
        bad_session_penalty = 3 if features.get("session_name") in ("Asia",) else 0
        side_inconsistent = bool(getattr(self.cfg, "block_side_inconsistent_features", True)) and features.get("intended_side") in ("long", "short") and features.get("intended_side") != side
        side_penalty = 30 if side_inconsistent else 0
        context_score = max(-15, min(15, context_score))
        pattern_score = max(-15, min(15, pattern_score))
        strategy_edge_score = max(-20, min(20, strategy_edge_score))
        raw_score_pre = setup_score + context_score + pattern_score + strategy_edge_score + selector_bonus
        raw_score_final = raw_score_pre - spread_penalty - bad_session_penalty - side_penalty
        final_score = max(0, min(100, int(round(raw_score_final))))
        logging.info(
            "CANDIDATE_EVAL stage=%s symbol=%s tf=%s regime=%s strategy=%s side=%s setup_score=%s context_score=%s pattern_score=%s strategy_edge_score=%s selector_bonus=%s spread_penalty=%s bad_session_penalty=%s side_penalty=%s raw_score_pre=%s raw_score_final=%s final_score=%s decision=%s reject_reason=%s",
            ("pre_filter" if str(decision).startswith("skip_") else "final"), self.cfg.symbol, self.cfg.timeframe, regime, intent.reason, side, setup_score, context_score, pattern_score, strategy_edge_score, selector_bonus, spread_penalty, bad_session_penalty, side_penalty, raw_score_pre, raw_score_final, final_score, decision, reject_reason,
        )

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
                self.spread_block_count += 1
                self.status.spread_block_count = self.spread_block_count
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

    def _log_filter_diagnostics(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_block_diag) < 60:
            return
        self._last_block_diag = now

        total_bar_checks = self.bars_seen + self.session_block_count + self.spread_block_count
        if total_bar_checks <= 0:
            return
        session_pct = (self.session_block_count / total_bar_checks) * 100.0
        spread_pct = (self.spread_block_count / total_bar_checks) * 100.0
        evaluated_pct = (self.bars_evaluated / max(self.bars_seen, 1)) * 100.0
        logging.info(
            "ENTRY_FILTERS loop_checks=%s bars_seen=%s bars_in_session=%s bars_spread_ok=%s bars_evaluated=%s(%.1f%%) candidate_bars=%s signal_bars=%s session_blocked=%s(%.1f%%) spread_blocked=%s(%.1f%%)",
            self.loop_checks,
            self.bars_seen,
            self.bars_in_session,
            self.bars_spread_ok,
            self.bars_evaluated,
            evaluated_pct,
            self.candidate_bars,
            self.signal_bars,
            self.session_block_count,
            session_pct,
            self.spread_block_count,
            spread_pct,
        )

    def _write_run_summary(self) -> None:
        if self.loop_checks <= 0:
            return
        summary = {
            "run_id": self.run_id,
            "run_start_time": self.run_started_at.isoformat(),
            "build_version": self.build_version,
            "symbol": self.cfg.symbol,
            "timeframe": self.cfg.timeframe,
            "checks_total": self.loop_checks,
            "loop_checks": self.loop_checks,
            "bars_seen": self.bars_seen,
            "bars_in_session": self.bars_in_session,
            "bars_spread_ok": self.bars_spread_ok,
            "bars_evaluated": self.bars_evaluated,
            "candidate_bars": self.candidate_bars,
            "signal_bars": self.signal_bars,
            "processed_bars": self.processed_bars,
            "processed_bars_pct": round((self.processed_bars / max(self.bars_seen, 1)) * 100.0, 2),
            "session_blocked": self.session_block_count,
            "session_blocked_pct": round((self.session_block_count / max(self.loop_checks, 1)) * 100.0, 2),
            "spread_blocked": self.spread_block_count,
            "spread_blocked_pct": round((self.spread_block_count / max(self.loop_checks, 1)) * 100.0, 2),
            "paper_total": self.status.paper_total,
            "paper_open": self.status.paper_open,
            "paper_closed": self.status.paper_closed,
            "paper_winrate": self.status.paper_winrate,
            "paper_avg_r": self.status.paper_avg_r,
            "current_run_total": self.status.current_run_total,
            "current_run_closed": self.status.current_run_closed,
            "all_time_total": self.status.all_time_total,
        }
        target = save_run_summary(summary=summary, report_dir=self.cfg.store_reports_path)
        logging.info("RUN_SUMMARY_SAVED path=%s summary=%s", target, summary)

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

    def _is_candidate_reason(self, reason: str) -> bool:
        if not reason:
            return False
        if "near_signal" in reason:
            return True
        return reason.startswith(("PRO_", "RANGE_"))

    def _track_signal_flow_window(self, reason: str, valid: bool):
        self.window_processed_bars += 1
        is_candidate = bool(valid) or self._is_candidate_reason(reason)
        if is_candidate:
            self.window_candidate_signals += 1
            self.candidate_bars += 1
            self.status.candidate_bars = self.candidate_bars
        if valid:
            self.window_true_signals += 1
            self.signal_bars += 1
            self.status.signal_bars = self.signal_bars
        if "near_signal" in (reason or ""):
            self.window_near_signals += 1
        if not valid:
            self.window_reject_reasons[reason or "unknown"] += 1

        if self.window_processed_bars < self.eval_window_bars:
            return

        candidate_rate = (self.window_candidate_signals / self.window_processed_bars) * 100.0
        pass_through = (
            (self.window_true_signals / self.window_candidate_signals) * 100.0
            if self.window_candidate_signals
            else 0.0
        )
        near_to_true = (
            self.window_near_signals / max(self.window_true_signals, 1)
            if self.window_near_signals
            else 0.0
        )
        top_rejects = self.window_reject_reasons.most_common(3)
        logging.info(
            "FLOW_WINDOW bars=%s candidate=%s(%.1f%%) true=%s pass_through=%.1f%% near=%s near_to_true=%.2f top_rejects=%s",
            self.window_processed_bars,
            self.window_candidate_signals,
            candidate_rate,
            self.window_true_signals,
            pass_through,
            self.window_near_signals,
            near_to_true,
            top_rejects,
        )

        self.window_processed_bars = 0
        self.window_candidate_signals = 0
        self.window_true_signals = 0
        self.window_near_signals = 0
        self.window_reject_reasons.clear()

    def _entry_loop(self):
        while not self.stop_event.is_set():
            try:
                self.loop_checks += 1
                self.status.loop_checks = self.loop_checks
                self._update_guards()
                if getattr(self.cfg, "learning_pipeline_mode", "manual") == "background":
                    self._refresh_learning_tables_if_needed()
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
                if (not self.cfg.disable_session_filter) and (hour < self.cfg.session_start_hour or hour > self.cfg.session_end_hour):
                    self.session_block_count += 1
                    self.status.session_block_count = self.session_block_count
                    self.status.loop_state = "session_blocked"
                    self.status.last_msg = "session_blocked"
                    self._log_strategy_reason_change("session_blocked")
                    self._log_filter_diagnostics()
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
                self.bars_seen += 1
                self.bars_in_session += 1
                self.bars_spread_ok += 1
                self.status.bars_seen = self.bars_seen
                self.status.bars_in_session = self.bars_in_session
                self.status.bars_spread_ok = self.bars_spread_ok
                self.processed_bars += 1
                self.bars_evaluated += 1
                self.status.bars_evaluated = self.bars_evaluated
                self.status.processed_bars = self.processed_bars
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
                    same_bar_priority=str(getattr(self.cfg, "same_bar_priority", "conservative")),
                )
                for trade in updates:
                    if trade.status != "open":
                        rr_realized = float(getattr(trade, "result_r", 0.0))
                        log_trade_close(
                            trade=trade,
                            result_r=rr_realized,
                            path=self.cfg.ai_memory_path,
                        )
                        logging.info(
                            "PAPER_CLOSE batch_id=%s final_status=%s exit_reason=%s result_r=%.2f mfe_r=%.2f mae_r=%.2f",
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
                    if range_intent.reason in ("range_idle", "no_signal") and bool((range_intent.debug or {}).get("range_confirmed", False)):
                        self.range_no_signal_count += 1
                    else:
                        self.range_no_signal_count = 0
                    if self.range_no_signal_count > 20:
                        regime = "dead"
                        self.range_no_signal_count = 0
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
                    if regime in ("trend", "trend_overextended"):
                        logging.info(
                            "TREND_SIDE_DIAG long_score=%s short_score=%s min=%s effective_min=%s long_gap=%s short_gap=%s",
                            intent.debug.get("trend_score_long"),
                            intent.debug.get("trend_score_short"),
                            intent.debug.get("trend_min_score"),
                            intent.debug.get("effective_min_score"),
                            intent.debug.get("long_score_gap"),
                            intent.debug.get("short_score_gap"),
                        )
                    if regime == "range":
                        logging.info(
                            "RANGE_SIDE_DIAG near_top=%s near_bottom=%s fake_break_top=%s fake_break_bottom=%s reclaim_top=%s reclaim_bottom=%s",
                            intent.debug.get("near_top"),
                            intent.debug.get("near_bottom"),
                            intent.debug.get("fake_break_top"),
                            intent.debug.get("fake_break_bottom"),
                            intent.debug.get("reclaim_top"),
                            intent.debug.get("reclaim_bottom"),
                        )
                self._log_strategy_reason_change(intent.reason)
                self._track_signal_flow_window(reason=intent.reason, valid=bool(intent.valid))
                self._log_filter_diagnostics()
                if not intent.valid:
                    self._log_candidate_eval(intent=intent, regime=regime, closed_bar_time=closed_bar_time, decision="skip_invalid_intent", reject_reason=intent.reason)

                if (not intent.valid) and any(
                    tag in (intent.reason or "")
                    for tag in ("trend_near_signal", "range_not_confirmed", "range_idle", "range_breakout_pressure")
                ):
                    try:
                        spread_points = int(client.get_symbol_diagnostics(self.cfg.symbol).spread_points)
                    except Exception:
                        spread_points = self.cfg.max_spread_points
                    reason_l = (intent.reason or "").lower()
                    side_guess = "short" if ("short" in reason_l or "top" in reason_l) else "long"
                    candidate_features = build_feature_snapshot(
                        signal_reason=intent.reason,
                        intent_debug=intent.debug,
                        spread_points=spread_points,
                        max_spread_points=self.cfg.max_spread_points,
                        regime=regime,
                        strategy_name=intent.reason or "candidate",
                        side=side_guess,
                        symbol=self.cfg.symbol,
                        timeframe=self.cfg.timeframe,
                        bar_time=closed_bar_time,
                    )
                    context_score, context_reason = (0, "context_disabled")
                    if self.cfg.enable_statistical_learning and self.cfg.enable_context_score:
                        context_score, context_reason = self.context_scorer.score(
                            lookup=candidate_features,
                            min_samples=self.cfg.min_samples_context,
                        )
                    pattern_score, pattern_reason = (0, "pattern_disabled")
                    if self.cfg.enable_pattern_learning and self.cfg.enable_pattern_score:
                        pattern_score, pattern_reason = self.pattern_scorer.score(
                            lookup=candidate_features,
                            min_samples=self.cfg.min_samples_pattern,
                        )
                    logging.info(
                        "CANDIDATE_AI_SCORE regime=%s reason=%s side=%s context_score=%s pattern_score=%s context_reason=%s pattern_reason=%s",
                        regime,
                        intent.reason,
                        side_guess,
                        context_score,
                        pattern_score,
                        context_reason,
                        pattern_reason,
                    )

                if intent.valid and intent.batch_id != self.last_batch_id:
                    now_ts = time.time()
                    min_gap_s = max(0, int(self.cfg.min_minutes_between_signals)) * 60
                    if self.last_signal_ts > 0 and (now_ts - self.last_signal_ts) < min_gap_s:
                        self.status.last_msg = "skip_trade_spacing"
                        self._log_strategy_reason_change(self.status.last_msg)
                        self.stop_event.wait(self.cfg.entry_check_seconds)
                        continue
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
                        strategy_name=intent.reason,
                        side=("long" if intent.is_long else "short"),
                        symbol=self.cfg.symbol,
                        timeframe=self.cfg.timeframe,
                        bar_time=closed_bar_time,
                    )
                    if not features.get("feature_side_consistent", True):
                        logging.warning(
                            "FEATURE_SIDE_MISMATCH batch_id=%s strategy=%s side=%s trend_ok_long=%s trend_ok_short=%s htf_ok_long=%s htf_ok_short=%s trend_ok=%s htf_ok=%s",
                            intent.batch_id,
                            intent.reason,
                            features.get("side"),
                            features.get("trend_ok_long"),
                            features.get("trend_ok_short"),
                            features.get("htf_ok_long"),
                            features.get("htf_ok_short"),
                            features.get("trend_ok"),
                            features.get("htf_ok"),
                        )
                    side_inconsistent = not features.get("feature_side_consistent", True)
                    if side_inconsistent and bool(getattr(self.cfg, "block_side_inconsistent_features", True)):
                        self.status.last_msg = "side_inconsistent_features"
                        self._log_strategy_reason_change(self.status.last_msg)
                        continue

                    base_confidence = confidence_score(
                        features=features,
                        is_long=bool(intent.is_long),
                        max_spread=self.cfg.max_spread_points,
                    )
                    setup_score = int(round(base_confidence * float(self.cfg.setup_score_weight)))

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

                    selector_bonus = int(selector["bonus"])
                    context_score = 0
                    context_reason = "context_disabled"
                    if self.cfg.enable_statistical_learning and self.cfg.enable_context_score:
                        context_score, context_reason = self.context_scorer.score(
                            lookup=features,
                            min_samples=self.cfg.min_samples_context,
                        )

                    pattern_score = 0
                    pattern_reason = "pattern_disabled"
                    if self.cfg.enable_pattern_learning and self.cfg.enable_pattern_score:
                        pattern_score, pattern_reason = self.pattern_scorer.score(
                            lookup=features,
                            min_samples=self.cfg.min_samples_pattern,
                        )
                    strategy_edge_score = 0
                    strategy_edge_reason = "strategy_edge_disabled"
                    if self.cfg.enable_strategy_edge_table:
                        strategy_edge_score, strategy_edge_reason = self.strategy_edge_scorer.score(
                            lookup=features,
                            min_samples=self.cfg.strategy_edge_min_samples,
                        )

                    spread_penalty = 0 if spread_points < self.cfg.max_spread_points else 8
                    bad_session_penalty = 3 if features.get("session_name") in ("Asia",) else 0
                    side_penalty = 30 if side_inconsistent else 0
                    context_score = max(-15, min(15, context_score))
                    pattern_score = max(-15, min(15, pattern_score))
                    strategy_edge_score = max(-20, min(20, strategy_edge_score))
                    raw_score_pre = (
                        setup_score
                        + context_score
                        + pattern_score
                        + strategy_edge_score
                        + selector_bonus
                    )
                    raw_score_final = raw_score_pre - spread_penalty - bad_session_penalty - side_penalty
                    final_score = max(0, min(100, int(round(raw_score_final))))
                    confidence = int(final_score)

                    logging.info(
                        "AI_SELECTOR strategy=%s regime=%s base=%s bonus=%s final=%s selector_reason=%s samples=%s avg_r=%.2f winrate=%.2f",
                        intent.reason,
                        regime,
                        base_confidence,
                        selector_bonus,
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
                    min_required = self.cfg.min_final_score_paper if self.cfg.paper_mode else self.cfg.min_final_score_live
                    decision = "live_signal"
                    if self.cfg.paper_mode:
                        decision = "paper_signal"
                    reject_reason = ""
                    if final_score < min_required:
                        decision = "skip_low_final_score"
                        reject_reason = "low_final_score"
                    logging.info(
                        "LIVE_DECISION regime=%s strategy=%s side=%s setup_score=%s context_score=%s pattern_score=%s strategy_edge_score=%s selector_bonus=%s spread_penalty=%s bad_session_penalty=%s side_penalty=%s raw_score_pre=%s raw_score_final=%s final_score=%s decision=%s context_reason=%s pattern_reason=%s strategy_edge_reason=%s",
                        regime,
                        intent.reason,
                        "long" if intent.is_long else "short",
                        setup_score,
                        context_score,
                        pattern_score,
                        strategy_edge_score,
                        selector_bonus,
                        spread_penalty,
                        bad_session_penalty,
                        side_penalty,
                        raw_score_pre,
                        raw_score_final,
                        final_score,
                        decision,
                        context_reason,
                        pattern_reason,
                        strategy_edge_reason,
                    )
                    self._log_candidate_eval(intent=intent, regime=regime, closed_bar_time=closed_bar_time, decision=decision, reject_reason=reject_reason)
                    if decision == "skip_low_final_score":
                        self.status.last_msg = f"skip_low_final_score:{final_score}<{min_required}"
                        self._log_strategy_reason_change(self.status.last_msg)
                        self.stop_event.wait(self.cfg.entry_check_seconds)
                        continue
                    if self.cfg.paper_mode:
                        open_same = [t for t in load_paper_trades() if t.status == "open" and t.symbol == self.cfg.symbol and t.timeframe == self.cfg.timeframe]
                        if len(open_same) >= int(self.cfg.max_open_paper_trades):
                            decision = "paper_open_position_blocked"
                            logging.info("LIVE_DECISION decision=%s final_score=%s min_required=%s", decision, final_score, min_required)
                            continue
                        entry = 0.0
                        tp1 = 0.0
                        tp2 = 0.0
                        tp3 = 0.0
                        tick = client.get_tick(self.cfg.symbol)
                        if tick is None:
                            self.status.last_msg = "tick_missing"
                            self._log_strategy_reason_change(self.status.last_msg)
                            logging.info("LIVE_DECISION decision=tick_missing final_score=%s min_required=%s", final_score, min_required)
                            continue
                        entry = float(tick.ask if intent.is_long else tick.bid)
                        r_value = abs(entry - float(intent.sl))
                        if r_value > 0:
                            tp1 = entry + (float(self.cfg.tp1_r_multiple) * r_value) if intent.is_long else entry - (float(self.cfg.tp1_r_multiple) * r_value)
                            tp2 = entry + (float(self.cfg.tp2_r_multiple) * r_value) if intent.is_long else entry - (float(self.cfg.tp2_r_multiple) * r_value)
                            tp3 = entry + (float(self.cfg.tp3_r_multiple) * r_value) if intent.is_long else entry - (float(self.cfg.tp3_r_multiple) * r_value)

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
                                run_id=self.run_id,
                                run_start_time=self.run_started_at.isoformat(),
                                build_version=self.build_version,
                            )
                        trade.tp1_size_ratio = float(self.cfg.tp1_size_ratio)
                        trade.tp2_size_ratio = float(self.cfg.tp2_size_ratio)
                        trade.tp3_size_ratio = float(self.cfg.tp3_size_ratio)
                        upsert_paper_trade(trade)
                        self.signal_count += 1
                        self.status.signal_count = self.signal_count
                        self.last_signal_ts = now_ts
                        self.last_batch_id = intent.batch_id
                        logging.info("SIGNAL_COUNT=%s day=%s", self.signal_count, self.current_day)
                        log_trade_open(trade=trade, path=self.cfg.ai_memory_path)
                        self._refresh_paper_stats()

                        logging.info(
                            "PAPER_SIGNAL run_id=%s strategy=%s side=%s entry=%.5f sl=%.5f tp1=%.5f tp2=%.5f tp3=%.5f final_score=%s context_score=%s pattern_score=%s strategy_edge_score=%s",
                            self.run_id,
                            intent.reason,
                            "BUY" if intent.is_long else "SELL",
                            entry,
                            intent.sl,
                            tp1,
                            tp2,
                            tp3,
                            final_score,
                            context_score,
                            pattern_score,
                            strategy_edge_score,
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
                            self.signal_count += 1
                            self.status.signal_count = self.signal_count
                            self.last_signal_ts = now_ts
                            self.last_batch_id = intent.batch_id
                            logging.info("SIGNAL_COUNT=%s day=%s", self.signal_count, self.current_day)
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
