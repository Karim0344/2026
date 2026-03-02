#!/usr/bin/env python3
"""Simple MT5 connectivity smoke test for FlexBot."""

import argparse
import json
from pathlib import Path


def load_config(repo_root: Path) -> dict:
    cfg = repo_root / "config.json"
    if not cfg.exists():
        return {}
    try:
        return json.loads(cfg.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="MT5 smoke test for FlexBot")
    parser.add_argument("--symbol", default="XAUUSD", help="Symbol to verify tick stream")
    parser.add_argument("--terminal-path", default="", help="Path to terminal64.exe (optional)")
    parser.add_argument("--login", type=int, default=None, help="MT5 login id")
    parser.add_argument("--password", default="", help="MT5 password")
    parser.add_argument("--server", default="", help="MT5 server name")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    cfg = load_config(repo_root)

    symbol = args.symbol or cfg.get("symbol") or "XAUUSD"
    terminal_path = args.terminal_path or cfg.get("terminal_path") or ""
    login = args.login if args.login is not None else cfg.get("mt5_login")
    password = args.password or cfg.get("mt5_password") or ""
    server = args.server or cfg.get("mt5_server") or ""

    client = None
    try:
        import MetaTrader5 as mt5
        from flexbot.mt5 import client as mt5_client

        client = mt5_client
        terminal_used = client.initialize(
            terminal_path=terminal_path,
            login=login,
            password=password,
            server=server,
        )
        term_info = mt5.terminal_info()
        account = mt5.account_info()
        resolved = client.resolve_symbol(symbol, auto_resolve=True)
        tick = mt5.symbol_info_tick(resolved)

        print("MT5 smoke test")
        print(f"terminal_path_used: {terminal_used}")
        print(f"terminal_company: {getattr(term_info, 'company', 'N/A')}")
        print(f"terminal_connected: {getattr(term_info, 'connected', 'N/A')}")
        print(f"account_login: {getattr(account, 'login', 'N/A')}")
        print(f"account_server: {getattr(account, 'server', 'N/A')}")
        print(f"symbol_requested: {symbol}")
        print(f"symbol_resolved: {resolved}")
        print(f"tick_available: {bool(tick and getattr(tick, 'bid', 0) > 0 and getattr(tick, 'ask', 0) > 0)}")
        if tick:
            print(f"tick_bid_ask: {getattr(tick, 'bid', 0)} / {getattr(tick, 'ask', 0)}")
        return 0
    except ModuleNotFoundError as exc:
        print(f"ERROR: {exc}. Install MetaTrader5 package in the same Python environment.")
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    finally:
        if client is not None:
            client.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
