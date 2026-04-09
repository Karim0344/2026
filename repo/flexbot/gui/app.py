import json
import logging
import os
import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from flexbot.core.config import BotConfig
from flexbot.core.logging_util import setup_logger
from flexbot.trading.engine import TradingEngine


class TkLogHandler(logging.Handler):
    def __init__(self, q: queue.Queue):
        super().__init__()
        self.q = q

    def emit(self, record: logging.LogRecord):
        self.q.put(self.format(record))


def _load_json_config() -> dict:
    try:
        here = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        cfg_path = os.path.join(here, "config.json")
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_json_config(data: dict) -> None:
    try:
        here = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        cfg_path = os.path.join(here, "config.json")
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("FlexBot Python v1 (Rebuild) - Survival Mode")
        self.root.geometry("760x620")

        setup_logger("flexbot.log")
        self.log_queue: queue.Queue[str] = queue.Queue()
        handler = TkLogHandler(self.log_queue)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
        logging.getLogger().addHandler(handler)
        logging.info("APP_START")

        self.cfg = BotConfig()
        jc = _load_json_config()
        self.cfg.apply_overrides(jc)
        self.engine: TradingEngine | None = None
        self._busy = False
        self.advanced_visible = False

        self._build()
        self._ui_loop()

    def _build(self):
        frm = ttk.Frame(self.root, padding=12)
        frm.pack(fill="both", expand=True)

        title = ttk.Label(
            frm,
            text="FlexBot Python v1 (Rebuild) — Execution + Risk Engine",
            font=("Arial", 14, "bold"),
        )
        title.pack(pady=(0, 8))

        grid = ttk.Frame(frm)
        grid.pack(fill="x")

        def add_row(r, label, widget):
            ttk.Label(grid, text=label).grid(
                row=r, column=0, sticky="w", padx=(0, 10), pady=4
            )
            widget.grid(row=r, column=1, sticky="ew", pady=4)
            grid.grid_columnconfigure(1, weight=1)

        self.symbol_var = tk.StringVar(value=self.cfg.symbol)
        add_row(0, "Symbol:", ttk.Entry(grid, textvariable=self.symbol_var))

        self.tf_var = tk.StringVar(value=self.cfg.timeframe)
        add_row(
            1,
            "Timeframe:",
            ttk.Combobox(
                grid,
                textvariable=self.tf_var,
                values=["M5", "M15", "H1", "H4"],
                state="readonly",
            ),
        )

        self.paper_var = tk.BooleanVar(
            value=bool(getattr(self.cfg, "paper_mode", False))
        )
        add_row(2, "Paper mode:", ttk.Checkbutton(grid, variable=self.paper_var))

        self.advanced_toggle_btn = ttk.Button(
            frm,
            text="▶ Advanced Settings",
            command=self._toggle_advanced,
        )
        self.advanced_toggle_btn.pack(anchor="w", pady=(8, 4))

        self.advanced_frame = ttk.LabelFrame(frm, text="Advanced Settings", padding=10)
        advanced_grid = ttk.Frame(self.advanced_frame)
        advanced_grid.pack(fill="x")

        def add_adv_row(r, label, widget):
            ttk.Label(advanced_grid, text=label).grid(
                row=r, column=0, sticky="w", padx=(0, 10), pady=3
            )
            widget.grid(row=r, column=1, sticky="ew", pady=3)
            advanced_grid.grid_columnconfigure(1, weight=1)

        self.term_var = tk.StringVar(value=getattr(self.cfg, "terminal_path", ""))
        term_frame = ttk.Frame(advanced_grid)
        term_entry = ttk.Entry(term_frame, textvariable=self.term_var)
        term_entry.pack(side="left", fill="x", expand=True)

        def browse():
            p = filedialog.askopenfilename(
                title="Select terminal64.exe",
                filetypes=[
                    ("terminal64.exe", "terminal64.exe"),
                    ("Executables", "*.exe"),
                    ("All files", "*.*"),
                ],
            )
            if p:
                self.term_var.set(p)

        ttk.Button(term_frame, text="Browse", command=browse).pack(side="left", padx=6)
        add_adv_row(0, "MT5 terminal path:", term_frame)

        self.login_var = tk.StringVar(value=str(self.cfg.mt5_login or ""))
        add_adv_row(
            1, "MT5 login:", ttk.Entry(advanced_grid, textvariable=self.login_var)
        )

        self.password_var = tk.StringVar(value=self.cfg.mt5_password)
        add_adv_row(
            2,
            "MT5 password:",
            ttk.Entry(advanced_grid, textvariable=self.password_var, show="*"),
        )

        self.server_var = tk.StringVar(value=self.cfg.mt5_server)
        add_adv_row(
            3, "MT5 server:", ttk.Entry(advanced_grid, textvariable=self.server_var)
        )

        self.risk_var = tk.DoubleVar(value=self.cfg.risk_percent)
        add_adv_row(
            4, "Risk % per batch:", ttk.Entry(advanced_grid, textvariable=self.risk_var)
        )

        self.daily_var = tk.DoubleVar(value=self.cfg.daily_stop_percent)
        add_adv_row(
            5, "Daily stop %:", ttk.Entry(advanced_grid, textvariable=self.daily_var)
        )

        self.spread_var = tk.IntVar(value=self.cfg.max_spread_points)
        add_adv_row(
            6,
            "Max spread (points):",
            ttk.Entry(advanced_grid, textvariable=self.spread_var),
        )

        self.magic_var = tk.IntVar(value=self.cfg.magic)
        add_adv_row(
            7, "Magic number:", ttk.Entry(advanced_grid, textvariable=self.magic_var)
        )
        self.breakout_var = tk.BooleanVar(
            value=bool(getattr(self.cfg, "require_breakout", False))
        )
        add_adv_row(
            8,
            "Require breakout:",
            ttk.Checkbutton(advanced_grid, variable=self.breakout_var),
        )

        btns = ttk.Frame(frm)
        btns.pack(fill="x", pady=10)

        self.start_btn = ttk.Button(btns, text="Start", command=self.start)
        self.start_btn.pack(side="left", padx=5)

        self.stop_btn = ttk.Button(
            btns, text="Stop", command=self.stop, state="disabled"
        )
        self.stop_btn.pack(side="left", padx=5)

        self.status_lbl = ttk.Label(frm, text="Status: idle")
        self.status_lbl.pack(anchor="w", pady=(10, 2))

        self.metrics_lbl = ttk.Label(
            frm, text="Equity: - | Daily DD: - | Loss streak: -"
        )
        self.metrics_lbl.pack(anchor="w")

        self.log_box = tk.Text(frm, height=18)
        self.log_box.pack(fill="both", expand=True, pady=(10, 0))
        self.log_box.insert("end", "Logs: zie flexbot.log\n")
        self.log_box.configure(state="disabled")

    def _apply_cfg(self):
        self.cfg.symbol = self.symbol_var.get().strip()
        self.cfg.timeframe = self.tf_var.get().strip()
        self.cfg.terminal_path = self.term_var.get().strip()
        self.cfg.mt5_login = (
            int(self.login_var.get().strip()) if self.login_var.get().strip() else None
        )
        self.cfg.mt5_password = self.password_var.get()
        self.cfg.mt5_server = self.server_var.get().strip()
        self.cfg.paper_mode = bool(self.paper_var.get())
        self.cfg.risk_percent = float(self.risk_var.get())
        self.cfg.daily_stop_percent = float(self.daily_var.get())
        self.cfg.max_spread_points = int(self.spread_var.get())
        self.cfg.magic = int(self.magic_var.get())
        self.cfg.require_breakout = bool(self.breakout_var.get())

        _save_json_config(self.cfg.to_dict())

    def _toggle_advanced(self):
        self.advanced_visible = not self.advanced_visible
        if self.advanced_visible:
            self.advanced_toggle_btn.configure(text="▼ Advanced Settings")
            self.advanced_frame.pack(fill="x", pady=(0, 8))
        else:
            self.advanced_toggle_btn.configure(text="▶ Advanced Settings")
            self.advanced_frame.pack_forget()

    @staticmethod
    def _human_status(raw_status: str) -> str:
        status = (raw_status or "").strip()
        low = status.lower()
        if low == "idle":
            return "Inactief"
        if low in {"engine started"}:
            return "Verbonden met MT5"
        if low in {"paper_signal_logged"}:
            return "Paper mode actief"
        if low in {"guards_blocked"}:
            return "Trading geblokkeerd door risk guard"
        if low in {"market_closed/no_ticks"}:
            return "Geen tickdata beschikbaar"
        if low.startswith("waiting") or "no_signal" in low or "bar" in low:
            return "Wachten op marktdata"
        if low.startswith("opened"):
            return "Trade geopend"
        return "Wachten op marktdata"

    def _set_busy(self, busy: bool):
        self._busy = busy
        self.start_btn.configure(state="disabled" if busy else "normal")
        if busy and (self.engine is None or not self.engine.status.running):
            self.stop_btn.configure(state="disabled")

    def start(self):
        if self._busy:
            return
        if self.engine and self.engine.status.running:
            return
        self._set_busy(True)
        self._append_ui_log("Starting engine...")
        logging.info("GUI_START_CLICK")

        def _worker():
            try:
                self._apply_cfg()
                self.engine = TradingEngine(self.cfg)
                self.engine.start()

                def _ok():
                    self._set_busy(False)
                    self.start_btn.configure(state="disabled")
                    self.stop_btn.configure(state="normal")
                    self._append_ui_log("ENGINE started")

                self.root.after(0, _ok)
            except Exception as e:
                logging.exception("START_FAILED")
                err_msg = str(e)

                def _fail(msg=err_msg):
                    self._set_busy(False)
                    self.stop_btn.configure(state="disabled")
                    messagebox.showerror("Start failed", msg)
                    self._append_ui_log(f"START_FAILED: {msg}")

                self.root.after(0, _fail)

        threading.Thread(target=_worker, daemon=True).start()

    def stop(self):
        if self._busy:
            return
        if not self.engine:
            self.stop_btn.configure(state="disabled")
            return

        self._set_busy(True)
        self._append_ui_log("Stopping engine...")
        logging.info("GUI_STOP_CLICK")

        def _worker():
            stopped_ok = False
            try:
                self.engine.stop()
                msg = "ENGINE stopped"
                stopped_ok = True
            except Exception as e:
                logging.exception("STOP_FAILED")
                msg = f"STOP_FAILED: {e}"

            def _done(final_msg=msg, stop_succeeded=stopped_ok):
                self._set_busy(False)
                if stop_succeeded:
                    self.start_btn.configure(state="normal")
                    self.status_lbl.configure(text="Status: Gestopt")
                self.stop_btn.configure(state="disabled")
                self._append_ui_log(final_msg)

            self.root.after(0, _done)

        threading.Thread(target=_worker, daemon=True).start()

    def _append_ui_log(self, msg: str):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", msg + "\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _drain_logs(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self._append_ui_log(msg)
        except queue.Empty:
            pass

    def _ui_loop(self):
        try:
            self._drain_logs()
            if self.engine and self.engine.status.running:
                st = self.engine.status
                self.status_lbl.configure(
                    text=(
                        f"Status: loop={st.loop_state} | "
                        f"reason={st.last_eval_reason} | "
                        f"bar={st.last_eval_bar_time}"
                    )
                )
                self.metrics_lbl.configure(
                    text=(
                        f"Equity: {st.equity:.2f} | "
                        f"Daily DD: {st.daily_dd * 100:.2f}% | "
                        f"Loss streak: {st.consec_losses} | "
                        f"Signals: {st.signal_count}\n"
                        f"Paper total: {st.paper_total} | "
                        f"Open: {st.paper_open} | "
                        f"Closed: {st.paper_closed} | "
                        f"Winrate: {st.paper_winrate:.2f}% | "
                        f"Avg R: {st.paper_avg_r:.2f}\n"
                        f"TP1: {st.paper_tp1} | "
                        f"TP2: {st.paper_tp2} | "
                        f"TP3: {st.paper_tp3} | "
                        f"SL: {st.paper_sl}"
                    )
                )
            elif self.engine and not self.engine.status.running:
                self.status_lbl.configure(text="Status: Gestopt")
        except Exception:
            pass
        self.root.after(500, self._ui_loop)


def run_app():
    root = tk.Tk()
    try:
        ttk.Style().theme_use("clam")
    except Exception:
        pass
    App(root)
    root.mainloop()
