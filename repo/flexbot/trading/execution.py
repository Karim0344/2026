import logging
import time
from dataclasses import dataclass

import MetaTrader5 as mt5

from flexbot.mt5 import client
from flexbot.trading.risk import calc_lot
from flexbot.trading.state import BatchState, save_state


@dataclass
class ExecResult:
    ok: bool
    msg: str


def _allowed_filling(symbol: str):
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_RETURN
    return info.filling_mode if hasattr(info, "filling_mode") else mt5.ORDER_FILLING_RETURN


def _send(request: dict):
    logging.info("ORDER_SEND request=%s", request)
    res = mt5.order_send(request)
    if res is None:
        logging.error("ORDER_SEND result=None last_error=%s", mt5.last_error())
        return None
    logging.info(
        "ORDER_SEND retcode=%s comment=%s order=%s deal=%s",
        res.retcode,
        res.comment,
        res.order,
        res.deal,
    )
    return res


def _fetch_position_ticket_by_comment(symbol: str, magic: int, comment: str) -> int:
    pos = client.positions(symbol=symbol, magic=magic)
    for p in pos:
        if (p.comment or "") == comment:
            return int(p.ticket)
    return 0


def _close_position(ticket: int, deviation: int = 20) -> bool:
    pos = mt5.positions_get(ticket=ticket)
    if pos is None or len(pos) == 0:
        return True
    p = pos[0]
    tick = client.get_tick(p.symbol)
    if tick is None:
        logging.error("ROLLBACK_CLOSE_NO_TICK ticket=%s", ticket)
        return False
    is_buy = int(p.type) == mt5.POSITION_TYPE_BUY
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": p.symbol,
        "position": int(ticket),
        "volume": float(p.volume),
        "type": mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY,
        "price": float(tick.bid if is_buy else tick.ask),
        "deviation": deviation,
        "magic": int(p.magic),
        "comment": "FlexBot|ROLLBACK",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": _allowed_filling(p.symbol),
    }
    res = _send(request)
    return res is not None and res.retcode == mt5.TRADE_RETCODE_DONE


def open_batch(
    symbol: str,
    magic: int,
    batch_id: str,
    is_long: bool,
    sl: float,
    risk_percent: float,
    be_buf_points: int,
    tp1_r_multiple: float = 1.0,
    tp2_r_multiple: float = 2.2,
    tp3_r_multiple: float = 3.2,
    tp1_size_ratio: float = 0.30,
    tp2_size_ratio: float = 0.35,
    tp3_size_ratio: float = 0.35,
) -> tuple[BatchState, ExecResult]:
    del be_buf_points
    client.ensure_symbol(symbol)
    tick = client.get_tick(symbol)
    if tick is None:
        return BatchState(), ExecResult(False, "no_tick")

    entry = float(tick.ask if is_long else tick.bid)

    equity = client.account_equity()
    risk_total = equity * (risk_percent / 100.0)
    size_sum = tp1_size_ratio + tp2_size_ratio + tp3_size_ratio
    if size_sum <= 0:
        return BatchState(), ExecResult(False, "invalid_tp_size_ratios")
    tp1_size_ratio /= size_sum
    tp2_size_ratio /= size_sum
    tp3_size_ratio /= size_sum

    try:
        total_lot = calc_lot(symbol, risk_total, entry, sl).lot
    except Exception as e:
        return BatchState(), ExecResult(False, f"lot_calc_failed: {e}")

    if mt5.symbol_info(symbol) is None:
        return BatchState(), ExecResult(False, "symbol_info_failed")

    r_value = abs(entry - sl)
    if r_value <= 0:
        return BatchState(), ExecResult(False, "invalid_R")

    tp1 = entry + (tp1_r_multiple * r_value) if is_long else entry - (tp1_r_multiple * r_value)
    tp2 = entry + (tp2_r_multiple * r_value) if is_long else entry - (tp2_r_multiple * r_value)
    tp3 = entry + (tp3_r_multiple * r_value) if is_long else entry - (tp3_r_multiple * r_value)

    deviation = 20
    filling = _allowed_filling(symbol)
    order_type = mt5.ORDER_TYPE_BUY if is_long else mt5.ORDER_TYPE_SELL

    legs = [
        (f"FlexBot|{batch_id}|TP1", tp1, total_lot * tp1_size_ratio),
        (f"FlexBot|{batch_id}|TP2", tp2, total_lot * tp2_size_ratio),
        (f"FlexBot|{batch_id}|TP3", tp3, total_lot * tp3_size_ratio),
    ]

    opened_comments: list[str] = []

    for i, (comment, tp, leg_lot) in enumerate(legs, start=1):
        if leg_lot <= 0:
            continue
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(leg_lot),
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
        res = _send(req)
        if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
            logging.error("BATCH_LEG_FAILED idx=%s batch_id=%s", i, batch_id)
            time.sleep(0.3)
            rollback_failed = False
            for opened_comment in opened_comments:
                t = _fetch_position_ticket_by_comment(symbol, magic, opened_comment)
                if t > 0 and not _close_position(t, deviation=deviation):
                    rollback_failed = True
                    logging.error("ROLLBACK_FAILED ticket=%s comment=%s", t, opened_comment)
            suffix = "rollback_failed" if rollback_failed else "rolled_back"
            return BatchState(), ExecResult(False, f"order_failed_{i}:{suffix}")
        opened_comments.append(comment)

    time.sleep(0.5)
    t1 = _fetch_position_ticket_by_comment(symbol, magic, legs[0][0])
    t2 = _fetch_position_ticket_by_comment(symbol, magic, legs[1][0])
    t3 = _fetch_position_ticket_by_comment(symbol, magic, legs[2][0])

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
    logging.info(
        "BATCH_OPENED id=%s long=%s entry=%s sl=%s lot_total=%s tp1/2/3=%s,%s,%s",
        batch_id,
        is_long,
        entry,
        sl,
        total_lot,
        tp1,
        tp2,
        tp3,
    )
    return state, ExecResult(True, "batch_opened")
