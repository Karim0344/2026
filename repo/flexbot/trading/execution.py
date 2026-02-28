import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import MetaTrader5 as mt5
from flexbot.mt5 import client
from flexbot.trading.state import BatchState, save_state
from flexbot.trading.risk import calc_lot

@dataclass
class ExecResult:
    ok: bool
    msg: str

def _allowed_filling(symbol: str):
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_RETURN
    # Use whatever broker allows; prefer RETURN then IOC.
    # Some symbols only allow one mode.
    return info.filling_mode if hasattr(info, "filling_mode") else mt5.ORDER_FILLING_RETURN

def _send(request: dict):
    logging.info(f"ORDER_SEND request={request}")
    res = mt5.order_send(request)
    if res is None:
        logging.error(f"ORDER_SEND result=None last_error={mt5.last_error()}")
        return None
    logging.info(f"ORDER_SEND retcode={res.retcode} comment={res.comment} order={res.order} deal={res.deal}")
    return res

def _fetch_position_ticket_by_comment(symbol: str, magic: int, comment: str) -> int:
    pos = client.positions(symbol=symbol, magic=magic)
    for p in pos:
        if (p.comment or "") == comment:
            return int(p.ticket)
    return 0

def open_batch(symbol: str, magic: int, batch_id: str, is_long: bool,
               sl: float, risk_percent: float, be_buf_points: int) -> tuple[BatchState, ExecResult]:
    client.ensure_symbol(symbol)
    tick = client.get_tick(symbol)
    if tick is None:
        return BatchState(), ExecResult(False, "no_tick")

    entry = float(tick.ask if is_long else tick.bid)

    # Risk split across 3 tickets
    equity = client.account_equity()
    risk_total = equity * (risk_percent / 100.0)
    risk_per = risk_total / 3.0

    # Calculate lot per ticket
    try:
        lot_res = calc_lot(symbol, risk_per, entry, sl)
        lot = lot_res.lot
    except Exception as e:
        return BatchState(), ExecResult(False, f"lot_calc_failed: {e}")

    info = mt5.symbol_info(symbol)
    if info is None:
        return BatchState(), ExecResult(False, "symbol_info_failed")

    # Compute TP levels by R multiples
    R = abs(entry - sl)
    if R <= 0:
        return BatchState(), ExecResult(False, "invalid_R")
    tp1 = entry + (1.0 * R) if is_long else entry - (1.0 * R)
    tp2 = entry + (2.0 * R) if is_long else entry - (2.0 * R)
    tp3 = entry + (3.0 * R) if is_long else entry - (3.0 * R)

    deviation = 20
    filling = _allowed_filling(symbol)

    # Use unique comments per ticket
    c1 = f"FlexBot|{batch_id}|TP1"
    c2 = f"FlexBot|{batch_id}|TP2"
    c3 = f"FlexBot|{batch_id}|TP3"

    order_type = mt5.ORDER_TYPE_BUY if is_long else mt5.ORDER_TYPE_SELL

    def make_req(comment: str, tp: float):
        return {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(lot),
            "type": order_type,
            "price": entry,
            "sl": float(sl),
            "tp": float(tp),
            "deviation": deviation,
            "magic": int(magic),
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }

    # Send 3 orders
    for i, (comment, tp) in enumerate([(c1, tp1), (c2, tp2), (c3, tp3)], start=1):
        res = _send(make_req(comment, tp))
        if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
            # try fallback filling modes if needed
            if res is not None:
                logging.error(f"Order failed retcode={res.retcode} comment={res.comment}")
            return BatchState(), ExecResult(False, f"order_failed_{i}")

    # Fetch position tickets (may appear shortly)
    import time
    time.sleep(0.5)
    t1 = _fetch_position_ticket_by_comment(symbol, magic, c1)
    t2 = _fetch_position_ticket_by_comment(symbol, magic, c2)
    t3 = _fetch_position_ticket_by_comment(symbol, magic, c3)

    state = BatchState(
        batch_id=batch_id,
        symbol=symbol,
        is_long=is_long,
        entry_price=entry,
        sl_price=float(sl),
        tp1=float(tp1),
        tp2=float(tp2),
        tp3=float(tp3),
        pos1_ticket=t1,
        pos2_ticket=t2,
        pos3_ticket=t3,
        be_applied=False,
    )
    save_state(state)
    logging.info(f"BATCH_OPENED id={batch_id} long={is_long} entry={entry} sl={sl} lot={lot} tp1/2/3={tp1},{tp2},{tp3}")
    return state, ExecResult(True, "batch_opened")
