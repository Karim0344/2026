import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import MetaTrader5 as mt5

_TF_MAP = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4,
}

_IPC_TIMEOUT_ERRORS = {-10005}
_AUTH_ERRORS = {-6, -2}
_LAST_NO_TICK_WARN_AT: dict[str, float] = {}


def _validate_session_state(context: str = "") -> tuple[object, object]:
    term_info = mt5.terminal_info()
    acct_info = mt5.account_info()
    if term_info is None or acct_info is None:
        last_err = mt5.last_error()
        raise RuntimeError(
            "MT5 session validation failed"
            f"{f' ({context})' if context else ''}: terminal_info/account_info unavailable, "
            f"last_error={last_err}. Ensure MT5 terminal is open and logged in."
        )
    return term_info, acct_info


@dataclass
class SymbolDiagnostics:
    symbol: str
    digits: int
    point: float
    spread_points: int
    vol_min: float
    vol_step: float
    vol_max: float
    stops_level: int
    freeze_level: int
    trade_mode: int
    filling_mode: int


def _masked_auth(login: Optional[int], server: str) -> str:
    if login is None and not server:
        return "none"
    return f"login={login if login is not None else '-'} server={server or '-'}"


def initialize(
    terminal_path: str = "",
    login: Optional[int] = None,
    password: str = "",
    server: str = "",
    timeout_ms: int = 60_000,
    retries: int = 4,
) -> str:
    """Initialize MT5 IPC bridge and optionally authorize account.

    Returns terminal path used by the MT5 package when available.
    """
    p = (terminal_path or "").strip()
    use_path = None
    if p:
        if os.path.exists(p):
            use_path = p
        else:
            logging.warning(
                'TERMINAL_PATH_INVALID path="%s" -> fallback to no-path attach', p
            )

    init_kwargs = {"timeout": int(timeout_ms)}
    if use_path:
        init_kwargs["path"] = use_path

    auth_present = login is not None or bool(password) or bool(server)
    terminal_used = use_path or "<auto>"

    logging.info(
        "MT5_INIT_BEGIN terminal=%s timeout_ms=%s retries=%s auth=%s",
        terminal_used,
        timeout_ms,
        retries,
        _masked_auth(login, server),
    )

    last_err = None
    for attempt in range(1, retries + 1):
        ok = mt5.initialize(**init_kwargs)
        if ok:
            try:
                info, account = _validate_session_state(context="post-initialize")
                terminal_used = getattr(info, "path", terminal_used)

                if auth_present:
                    auth_ok = mt5.login(
                        login=login or 0, password=password or "", server=server or ""
                    )
                    if not auth_ok:
                        auth_err = mt5.last_error()
                        err_code = int(auth_err[0]) if auth_err else 0
                        if err_code in _AUTH_ERRORS:
                            raise RuntimeError(
                                "MT5 login failed due to authorization data. "
                                f"login/server={_masked_auth(login, server)} error={auth_err}"
                            )
                        raise RuntimeError(f"MT5 login failed: {auth_err}")
                    info, account = _validate_session_state(context="post-login")
                    terminal_used = getattr(info, "path", terminal_used)

                logging.info(
                    "MT5_INIT_OK terminal=%s account_login=%s server=%s",
                    terminal_used,
                    getattr(account, "login", "-"),
                    getattr(account, "server", "-"),
                )
                return terminal_used
            except Exception:
                mt5.shutdown()
                raise

        last_err = mt5.last_error()
        err_code = int(last_err[0]) if last_err else 0
        logging.warning(
            "MT5_INIT_ATTEMPT_FAILED attempt=%s/%s error=%s", attempt, retries, last_err
        )

        if err_code in _IPC_TIMEOUT_ERRORS and attempt < retries:
            time.sleep(min(1.5 * attempt, 5.0))
            continue

        if use_path is not None and attempt == 1:
            logging.warning("MT5_INIT_PATH_FALLBACK path=%s", use_path)
            use_path = None
            init_kwargs.pop("path", None)
            continue

        break

    raise RuntimeError(
        f"MT5 initialize failed after {retries} attempts: {last_err}. "
        "Ensure terminal is running, logged in, Algo Trading enabled, and check terminal path/account settings."
    )


def shutdown() -> None:
    try:
        mt5.shutdown()
        logging.info("MT5_SHUTDOWN")
    except Exception:
        pass


def tf_to_mt5(timeframe: str) -> int:
    if timeframe not in _TF_MAP:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return _TF_MAP[timeframe]


def resolve_symbol(symbol: str, auto_resolve: bool = True) -> str:
    """Return a usable symbol name. If symbol not found or has no ticks and auto_resolve,
    attempt to find a close alternative (broker suffixes like XAUUSD#, XAUUSDm, GOLD).
    """
    info = mt5.symbol_info(symbol)
    if info is None:
        if not auto_resolve:
            raise RuntimeError(f"Symbol not found in MT5: {symbol}")
        # candidate scan
        all_syms = mt5.symbols_get()
        if not all_syms:
            raise RuntimeError(f"Symbol not found in MT5: {symbol}")
        sym_u = symbol.upper()
        if "XAU" in sym_u or "GOLD" in sym_u:
            cand = [
                s.name
                for s in all_syms
                if ("XAU" in s.name.upper() or "GOLD" in s.name.upper())
            ]
        else:
            base = re.sub(r"[^A-Z0-9]", "", sym_u)
            cand = [
                s.name
                for s in all_syms
                if base in re.sub(r"[^A-Z0-9]", "", s.name.upper())
            ]
        cand = cand[:50]
        # pick first with valid tick
        for name in cand:
            try:
                if mt5.symbol_select(name, True):
                    t = mt5.symbol_info_tick(name)
                    if t and getattr(t, "bid", 0) > 0 and getattr(t, "ask", 0) > 0:
                        logging.warning("Auto-resolved symbol: %s -> %s", symbol, name)
                        return name
            except Exception:
                continue
        logging.error("Symbol not found: %s. Candidates tried: %s", symbol, cand[:20])
        raise RuntimeError(
            f"Symbol not found in MT5: {symbol}. Check Market Watch exact name."
        )
    # ensure visible
    if not info.visible:
        mt5.symbol_select(symbol, True)
    # if no tick and auto_resolve, try alternatives with same base
    t = mt5.symbol_info_tick(symbol)
    if (
        t is None or getattr(t, "bid", 0) <= 0 or getattr(t, "ask", 0) <= 0
    ) and auto_resolve:
        all_syms = mt5.symbols_get() or []
        sym_u = symbol.upper()
        if "XAU" in sym_u or "GOLD" in sym_u:
            cand = [
                s.name
                for s in all_syms
                if ("XAU" in s.name.upper() or "GOLD" in s.name.upper())
            ]
        else:
            base = re.sub(r"[^A-Z0-9]", "", sym_u)
            cand = [
                s.name
                for s in all_syms
                if base in re.sub(r"[^A-Z0-9]", "", s.name.upper())
            ]
        cand = cand[:50]
        for name in cand:
            try:
                if mt5.symbol_select(name, True):
                    tt = mt5.symbol_info_tick(name)
                    if tt and getattr(tt, "bid", 0) > 0 and getattr(tt, "ask", 0) > 0:
                        logging.warning(
                            "Auto-resolved symbol (tick): %s -> %s", symbol, name
                        )
                        return name
            except Exception:
                continue
    return symbol


def ensure_symbol(symbol: str) -> None:
    info = mt5.symbol_info(symbol)
    if info is None:
        try:
            all_syms = mt5.symbols_get()
            if all_syms:
                cand = [
                    s.name
                    for s in all_syms
                    if ("XAU" in s.name.upper() or "GOLD" in s.name.upper())
                ]
                cand = cand[:20]
                logging.error(
                    "Symbol not found: %s. Possible gold symbols: %s", symbol, cand
                )
        except Exception:
            pass
        raise RuntimeError(
            f"Symbol not found in MT5: {symbol}. "
            "Fix: check exact symbol name in Market Watch (some brokers use suffix like XAUUSD#, XAUUSDm, GOLD)."
        )
    if not info.visible:
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"Could not select symbol: {symbol}")


def _log_no_tick_once(symbol: str, message: str, interval_s: float = 120.0) -> None:
    now = time.time()
    last = _LAST_NO_TICK_WARN_AT.get(symbol, 0.0)
    if now - last >= interval_s:
        _LAST_NO_TICK_WARN_AT[symbol] = now
        logging.warning(message)


def get_symbol_diagnostics(symbol: str) -> SymbolDiagnostics:
    symbol = resolve_symbol(symbol, auto_resolve=True)
    info = mt5.symbol_info(symbol)
    if info is None:
        raise RuntimeError(f"Symbol not found in MT5: {symbol}")

    tick = None
    for _ in range(6):
        tick = mt5.symbol_info_tick(symbol)
        if (
            tick is not None
            and getattr(tick, "bid", 0.0) > 0
            and getattr(tick, "ask", 0.0) > 0
        ):
            break
        time.sleep(0.5)

    if tick is None or getattr(tick, "bid", 0.0) <= 0 or getattr(tick, "ask", 0.0) <= 0:
        _log_no_tick_once(
            symbol,
            f"NO_TICK symbol={symbol} market_closed_or_not_streaming=True. "
            "Open Market Watch -> Show All, open symbol chart, or wait for market open.",
        )
        raise RuntimeError(
            f"Symbol tick not available: {symbol}. "
            "Fix: in MT5 open Market Watch -> Show All, then click the symbol chart so quotes stream. "
            "Also verify the exact symbol name (some brokers use suffix like XAUUSD#, XAUUSDm, GOLD). "
            "If the market is closed, wait until it opens."
        )

    spread_points = (
        int(round((tick.ask - tick.bid) / info.point))
        if info.point
        else int(getattr(info, "spread", 0))
    )

    stops_level = getattr(info, "stops_level", None)
    if stops_level is None:
        stops_level = int(getattr(info, "trade_stops_level", 0))
    freeze_level = getattr(info, "freeze_level", None)
    if freeze_level is None:
        freeze_level = int(getattr(info, "trade_freeze_level", 0))

    filling_mode = getattr(info, "filling_mode", None)
    if filling_mode is None:
        filling_mode = int(getattr(info, "trade_fill_mode", 0))
    return SymbolDiagnostics(
        symbol=symbol,
        digits=info.digits,
        point=info.point,
        spread_points=spread_points,
        vol_min=info.volume_min,
        vol_step=info.volume_step,
        vol_max=info.volume_max,
        stops_level=int(stops_level),
        freeze_level=int(freeze_level),
        trade_mode=info.trade_mode,
        filling_mode=int(filling_mode),
    )


def account_equity() -> float:
    ai = mt5.account_info()
    if ai is None:
        raise RuntimeError(f"account_info failed: {mt5.last_error()}")
    return float(ai.equity)


def broker_datetime_utc(symbol: str) -> datetime:
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(int(tick.time), tz=timezone.utc)


def copy_rates(symbol: str, timeframe: str, bars: int):
    tf = tf_to_mt5(timeframe)
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, bars)
    return rates


def positions(symbol: str, magic: int | None = None):
    pos = mt5.positions_get(symbol=symbol)
    if pos is None:
        return []
    if magic is None:
        return list(pos)
    return [p for p in pos if int(p.magic) == int(magic)]


def history_deals(from_dt: datetime, to_dt: datetime):
    deals = mt5.history_deals_get(from_dt, to_dt)
    if deals is None:
        return []
    return list(deals)


def round_volume(vol: float, vol_step: float) -> float:
    if vol_step <= 0:
        return vol
    steps = int(vol / vol_step)
    return round(steps * vol_step, 8)


def get_tick(symbol: str):
    return mt5.symbol_info_tick(symbol)
