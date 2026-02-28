import json
import logging
import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from flexbot.core.config import BotConfig
from flexbot.core.logging_util import setup_logger
from flexbot.trading.engine import TradingEngine


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
        logging.info("APP_START")

        self.cfg = BotConfig()
        jc = _load_json_config()
        if jc.get("terminal_path"):
            self.cfg.terminal_path = str(jc.get("terminal_path"))
        if jc.get("auto_resolve_symbol") is not None:
            self.cfg.auto_resolve_symbol = bool(jc.get("auto_resolve_symbol"))
        if jc.get("symbol"):
            self.cfg.symbol = str(jc.get("symbol"))
        if jc.get("paper_mode") is not None:
            self.cfg.paper_mode = bool(jc.get("paper_mode"))
        if jc.get("mt5_login"):
            self.cfg.mt5_login = int(jc.get("mt5_login"))
        if jc.get("mt5_server"):
            self.cfg.mt5_server = str(jc.get("mt5_server"))
        if jc.get("mt5_password"):
            self.cfg.mt5_password = str(jc.get("mt5_password"))
        self.engine: TradingEngine | None = None
        self._busy = False

        self._build()
        self._ui_loop()

    def _build(self):
        frm = ttk.Frame(self.root, padding=12)
        frm.pack(fill="both", expand=True)

        title = ttk.Label(frm, text="FlexBot Python v1 (Rebuild) — Execution + Risk Engine", font=("Arial", 14, "bold"))
        title.pack(pady=(0, 8))

        grid = ttk.Frame(frm)
        grid.pack(fill="x")

        def add_row(r, label, widget):
            ttk.Label(grid, text=label).grid(row=r, column=0, sticky="w", padx=(0, 10), pady=4)
            widget.grid(row=r, column=1, sticky="ew", pady=4)
            grid.grid_columnconfigure(1, weight=1)

        self.symbol_var = tk.StringVar(value=self.cfg.symbol)
        add_row(0, "Symbol (exact MT5 name):", ttk.Entry(grid, textvariable=self.symbol_var))

        self.term_var = tk.StringVar(value=getattr(self.cfg, "terminal_path", ""))
        term_frame = ttk.Frame(grid)
        term_entry = ttk.Entry(term_frame, textvariable=self.term_var)
        term_entry.pack(side="left", fill="x", expand=True)

        def browse():
            p = filedialog.askopenfilename(
                title="Select terminal64.exe",
                filetypes=[("terminal64.exe", "terminal64.exe"), ("Executables", "*.exe"), ("All files", "*.*")],
            )
            if p:
                self.term_var.set(p)

        ttk.Button(term_frame, text="Browse", command=browse).pack(side="left", padx=6)
        add_row(1, "MT5 terminal path (optional):", term_frame)

        self.paper_var = tk.BooleanVar(value=bool(getattr(self.cfg, "paper_mode", False)))
        add_row(2, "Paper mode (no orders):", ttk.Checkbutton(grid, variable=self.paper_var))

        self.tf_var = tk.StringVar(value=self.cfg.timeframe)
        add_row(3, "Timeframe:", ttk.Combobox(grid, textvariable=self.tf_var, values=["M5", "M15", "H1", "H4"], state="readonly"))

        self.risk_var = tk.DoubleVar(value=self.cfg.risk_percent)
        add_row(4, "Risk % per batch:", ttk.Entry(grid, textvariable=self.risk_var))

        self.daily_var = tk.DoubleVar(value=self.cfg.daily_stop_percent)
        add_row(5, "Daily stop %:", ttk.Entry(grid, textvariable=self.daily_var))

        self.spread_var = tk.IntVar(value=self.cfg.max_spread_points)
        add_row(6, "Max spread (points):", ttk.Entry(grid, textvariable=self.spread_var))

        self.magic_var = tk.IntVar(value=self.cfg.magic)
        add_row(7, "Magic number:", ttk.Entry(grid, textvariable=self.magic_var))

        btns = ttk.Frame(frm)
        btns.pack(fill="x", pady=10)

        self.start_btn = ttk.Button(btns, text="Start", command=self.start)
        self.start_btn.pack(side="left", padx=5)

        self.stop_btn = ttk.Button(btns, text="Stop", command=self.stop, state="disabled")
        self.stop_btn.pack(side="left", padx=5)

        self.status_lbl = ttk.Label(frm, text="Status: idle")
        self.status_lbl.pack(anchor="w", pady=(10, 2))

        self.metrics_lbl = ttk.Label(frm, text="Equity: - | Daily DD: - | Loss streak: -")
        self.metrics_lbl.pack(anchor="w")

        self.log_box = tk.Text(frm, height=18)
        self.log_box.pack(fill="both", expand=True, pady=(10, 0))
        self.log_box.insert("end", "Logs: zie flexbot.log\n")
        self.log_box.configure(state="disabled")

    def _apply_cfg(self):
        self.cfg.symbol = self.symbol_var.get().strip()
        self.cfg.timeframe = self.tf_var.get().strip()
        self.cfg.terminal_path = self.term_var.get().strip()
        self.cfg.paper_mode = bool(self.paper_var.get())
        self.cfg.risk_percent = float(self.risk_var.get())
        self.cfg.daily_stop_percent = float(self.daily_var.get())
        self.cfg.max_spread_points = int(self.spread_var.get())
        self.cfg.magic = int(self.magic_var.get())

        _save_json_config(
            {
                "terminal_path": self.cfg.terminal_path,
                "auto_resolve_symbol": self.cfg.auto_resolve_symbol,
                "symbol": self.cfg.symbol,
                "paper_mode": self.cfg.paper_mode,
                "mt5_login": self.cfg.mt5_login,
                "mt5_server": self.cfg.mt5_server,
                "mt5_password": self.cfg.mt5_password,
            }
        )

    def _set_busy(self, busy: bool):
        self._busy = busy
        self.start_btn.configure(state="disabled" if busy else "normal")
        if busy and (self.engine is None or not self.engine.status.running):
            self.stop_btn.configure(state="disabled")

    def start(self):
        if self._busy:
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
            try:
                self.engine.stop()
                msg = "ENGINE stopped"
            except Exception as e:
                logging.exception("STOP_FAILED")
                msg = f"STOP_FAILED: {e}"

            def _done(final_msg=msg):
                self._set_busy(False)
                self.start_btn.configure(state="normal")
                self.stop_btn.configure(state="disabled")
                self._append_ui_log(final_msg)

            self.root.after(0, _done)

        threading.Thread(target=_worker, daemon=True).start()

    def _append_ui_log(self, msg: str):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", msg + "\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _ui_loop(self):
        try:
            if self.engine and self.engine.status.running:
                st = self.engine.status
                self.status_lbl.configure(text=f"Status: {st.last_msg}")
                self.metrics_lbl.configure(text=f"Equity: {st.equity:.2f} | Daily DD: {st.daily_dd*100:.2f}% | Loss streak: {st.consec_losses}")
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
