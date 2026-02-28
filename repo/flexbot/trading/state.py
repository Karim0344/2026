from dataclasses import dataclass, asdict
from typing import Optional
import json
import os

@dataclass
class BatchState:
    batch_id: str = ""
    symbol: str = ""
    is_long: bool = False
    entry_price: float = 0.0
    sl_price: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    pos1_ticket: int = 0
    pos2_ticket: int = 0
    pos3_ticket: int = 0
    be_applied: bool = False

def load_state(path: str = "state.json") -> BatchState:
    if not os.path.exists(path):
        return BatchState()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return BatchState(**data)
    except Exception:
        return BatchState()

def save_state(state: BatchState, path: str = "state.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(asdict(state), f, ensure_ascii=False, indent=2)

def clear_state(path: str = "state.json") -> None:
    if os.path.exists(path):
        os.remove(path)
