import logging
from datetime import datetime

import MetaTrader5 as mt5

from flexbot.mt5 import client
from flexbot.trading.state import BatchState, clear_state, save_state


def _pos_exists(ticket: int) -> bool:
    if ticket <= 0:
        return False
    p = mt5.positions_get(ticket=ticket)
    return p is not None and len(p) > 0


def _get_pos(ticket: int):
    p = mt5.positions_get(ticket=ticket)
    if p is None or len(p) == 0:
        return None
    return p[0]


def _modify_sl(ticket: int, new_sl: float, new_tp: float | None = None) -> bool:
    pos = _get_pos(ticket)
    if pos is None:
        return False
    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "position": int(ticket),
        "symbol": pos.symbol,
        "sl": float(new_sl),
        "tp": float(pos.tp) if new_tp is None else float(new_tp),
        "magic": int(pos.magic),
        "comment": "FlexBot_SLTP",
    }
    res = mt5.order_send(request)
    if res is None:
        logging.error("SLTP failed ticket=%s result=None err=%s", ticket, mt5.last_error())
        return False
    if res.retcode != mt5.TRADE_RETCODE_DONE:
        logging.error("SLTP failed ticket=%s retcode=%s comment=%s", ticket, res.retcode, res.comment)
        return False
    return True


def _deal_profit_for_comment(
    symbol: str, comment_contains: str, from_dt: datetime, to_dt: datetime
) -> float:
    deals = client.history_deals(from_dt, to_dt)
    profit = 0.0
    for d in deals:
        if getattr(d, "symbol", "") != symbol:
            continue
        c = getattr(d, "comment", "") or ""
        if comment_contains in c:
            profit += float(getattr(d, "profit", 0.0))
    return profit


def manage_batch(
    state: BatchState,
    be_buffer_points: int,
    trail_atr_mult: float,
    trail_step_atr_mult: float,
    atr_period: int,
    timeframe: str,
) -> BatchState:
    if not state.batch_id or not state.symbol:
        return state

    symbol = state.symbol
    info = mt5.symbol_info(symbol)
    if info is None:
        return state
    point = info.point
    tick = client.get_tick(symbol)
    if tick is None:
        return state

    tp1_comment = f"FlexBot|{state.batch_id}|TP1"
    now = client.broker_datetime_utc(symbol)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    tp1_open = _pos_exists(state.pos1_ticket)
    tp1_price_hit = (float(tick.bid) >= state.tp1) if state.is_long else (float(tick.ask) <= state.tp1)

    if (not state.be_applied) and (not tp1_open):
        prof = _deal_profit_for_comment(symbol, tp1_comment, day_start, now)
        tp1_confirmed = prof > 0 or tp1_price_hit
        if tp1_confirmed:
            be = (
                state.entry_price + (be_buffer_points * point)
                if state.is_long
                else state.entry_price - (be_buffer_points * point)
            )
            changed = False
            for t in [state.pos2_ticket, state.pos3_ticket]:
                pos = _get_pos(t)
                if pos is None:
                    continue
                cur_sl = float(pos.sl)
                if state.is_long and be > cur_sl:
                    if _modify_sl(t, be):
                        logging.info("BE_APPLIED ticket=%s new_sl=%s", t, be)
                        changed = True
                elif (not state.is_long) and (cur_sl == 0.0 or be < cur_sl):
                    if _modify_sl(t, be):
                        logging.info("BE_APPLIED ticket=%s new_sl=%s", t, be)
                        changed = True
            state.be_applied = True
            if changed:
                save_state(state)

    if state.be_applied:
        rates = client.copy_rates(symbol, timeframe, max(atr_period + 5, 200))
        if rates is not None and len(rates) >= atr_period + 2:
            import pandas as pd

            df = pd.DataFrame(rates)
            high = df["high"]
            low = df["low"]
            close = df["close"]
            prev_close = close.shift(1)
            tr = pd.concat(
                [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
                axis=1,
            ).max(axis=1)
            atr = float(tr.rolling(atr_period).mean().iloc[-2])
            swing_lookback = min(6, len(df) - 2)
            look = df.iloc[-(swing_lookback + 1): -1]
            swing_low = float(look["low"].min())
            swing_high = float(look["high"].max())
        else:
            atr = 0.0
            swing_low = 0.0
            swing_high = 0.0

        if atr and atr > 0:
            trail_dist = trail_atr_mult * atr
            step = trail_step_atr_mult * atr
            bid = float(tick.bid)
            ask = float(tick.ask)

            for t in [state.pos2_ticket, state.pos3_ticket]:
                pos = _get_pos(t)
                if pos is None:
                    continue
                cur_sl = float(pos.sl)
                if state.is_long:
                    atr_sl = bid - trail_dist
                    new_sl = max(atr_sl, swing_low)
                    if new_sl > (cur_sl + step):
                        if _modify_sl(t, new_sl):
                            logging.info("TRAIL_UPDATE ticket=%s sl %s->%s", t, cur_sl, new_sl)
                else:
                    atr_sl = ask + trail_dist
                    new_sl = min(atr_sl, swing_high)
                    if cur_sl == 0.0 or new_sl < (cur_sl - step):
                        if _modify_sl(t, new_sl):
                            logging.info("TRAIL_UPDATE ticket=%s sl %s->%s", t, cur_sl, new_sl)

    open_any = _pos_exists(state.pos1_ticket) or _pos_exists(state.pos2_ticket) or _pos_exists(state.pos3_ticket)
    if not open_any:
        logging.info("BATCH_DONE id=%s", state.batch_id)
        clear_state()
        return BatchState()

    return state
