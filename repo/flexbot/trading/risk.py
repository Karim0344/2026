import math
from dataclasses import dataclass
import MetaTrader5 as mt5

@dataclass
class LotCalcResult:
    lot: float
    loss_per_lot: float

def calc_lot(symbol: str, risk_money: float, entry: float, sl: float) -> LotCalcResult:
    info = mt5.symbol_info(symbol)
    if info is None:
        raise RuntimeError("symbol_info failed")

    point = info.point
    if point <= 0:
        raise RuntimeError("invalid point")

    stop_dist = abs(entry - sl)
    stop_points = stop_dist / point
    if stop_points <= 0:
        raise RuntimeError("invalid stop distance")

    tick_value = mt5.symbol_info_double(symbol, mt5.SYMBOL_TRADE_TICK_VALUE)
    tick_size = mt5.symbol_info_double(symbol, mt5.SYMBOL_TRADE_TICK_SIZE)
    if tick_value is None or tick_size is None or tick_value <= 0 or tick_size <= 0:
        # fallback: use point as tick_size
        tick_size = point
        tick_value = mt5.symbol_info_double(symbol, mt5.SYMBOL_TRADE_TICK_VALUE)
        if tick_value is None or tick_value <= 0:
            raise RuntimeError("tick_value unavailable")

    value_per_point_per_lot = tick_value * (point / tick_size)

    loss_per_lot = stop_points * value_per_point_per_lot
    if loss_per_lot <= 0:
        raise RuntimeError("loss_per_lot invalid")

    lot_raw = risk_money / loss_per_lot

    vol_min = info.volume_min
    vol_step = info.volume_step
    vol_max = info.volume_max

    # round down to step
    steps = math.floor(lot_raw / vol_step) if vol_step > 0 else lot_raw
    lot = steps * vol_step if vol_step > 0 else lot_raw
    lot = max(vol_min, min(lot, vol_max))

    return LotCalcResult(lot=float(round(lot, 8)), loss_per_lot=float(loss_per_lot))
