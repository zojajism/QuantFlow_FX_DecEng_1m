# file: src/strategy/pivot_corr_engine.py
# English-only comments

from datetime import datetime
import hashlib
import threading
from typing import Any, Dict, List, Optional

import psycopg  # psycopg3
from psycopg.types.json import Json

# Access CandleBuffer via package-qualified path (absolute imports)
from buffers import buffer_initializer as buffers  # (kept if you need it elsewhere)
from buffers.candle_buffer import Keys

# Shared registry provider (adjust if your project exposes it differently)
from pivots.pivot_registry_provider import get_pivot_registry


# --------------------------------------------------------------------
# Core State Classes
# --------------------------------------------------------------------

class SignalMemory:
    """
    Keeps a memory of processed signals (to avoid duplicates).
    """
    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def remember(self, uid: str) -> bool:
        """Return True if new, False if already seen."""
        with self._lock:
            if uid in self._seen:
                return False
            self._seen.add(uid)
            return True


# --------------------------------------------------------------------
# Decision Event Entry Point
# --------------------------------------------------------------------

def run_decision_event(
    *,
    exchange: str,
    symbols: List[str],
    timeframe: str,
    event_time: datetime,
    signal_memory: SignalMemory,
    groups: Optional[Dict[str, List[str]]] = None,  # OPTIONAL simple grouping
    conn: Optional[psycopg.Connection] = None,      # DB connection for audit inserts
) -> None:
    """
    Runs per candle-close event to analyze pivots and produce decisions.
    If 'groups' is provided, it iterates by group; otherwise, iterates the flat 'symbols' list.
    Also appends one audit row per processed pivot into decision_audit_loops.
    """
    reg = get_pivot_registry()

    if groups:
        # Iterate by relation group
        for group_name, group_symbols in groups.items():
            _process_symbol_list(
                reg, exchange, timeframe, group_symbols, signal_memory,
                group_name=group_name, event_time=event_time, conn=conn
            )
    else:
        # Flat iteration
        _process_symbol_list(
            reg, exchange, timeframe, symbols, signal_memory,
            group_name=None, event_time=event_time, conn=conn
        )


# --------------------------------------------------------------------
# Internals
# --------------------------------------------------------------------

def _process_symbol_list(
    reg,
    exchange: str,
    timeframe: str,
    symbols: List[str],
    signal_memory: SignalMemory,
    *,
    group_name: Optional[str],
    event_time: datetime,
    conn: Optional[psycopg.Connection],
) -> None:
    rows: List[tuple] = []
    for symbol in symbols:
        pb = reg.get(exchange, symbol, timeframe)
        if pb is None:
            continue

        # Read pivots (newest -> oldest). Adjust to your actual logic.
        pivots = _collect_pivots_from_registry(exchange, symbol, timeframe, "HIGH", max_lookback=50)

        # Rank is per symbol's list (1 = newest)
        rank = 0
        for p in pivots:
            rank += 1

            # Be tolerant to schema differences
            pivot_time = p.get("close_time") or p.get("time")
            level = p.get("level")
            hit = bool(p.get("hit"))
            uid = p.get("uid") or hashlib.sha1(f"{symbol}:{pivot_time}".encode()).hexdigest()

            # Dedup by uid (optional memory)
            if not signal_memory.remember(uid):
                # We still want to audit the loop; you can flip this True/False policy if needed
                duplicate_suppressed = True
            else:
                duplicate_suppressed = False

            # Build a simple comparisons summary (placeholder)
            comparisons = Json([])  # put detailed per-symbol comparisons later if needed

            # Minimal, consistent audit row per pivot
            rows.append((
                event_time,          # event_id
                timeframe,           # timeframe
                symbol,              # ref_symbol
                "HIGH",              # ref_pivot_type (we're collecting HIGH above)
                pivot_time,          # ref_pivot_time
                uid,                 # ref_pivot_uid
                hit,                 # ref_hit
                3,                   # match_window_candles (default window ±3)
                rank,                # n_rank
                1,                   # found_count (this loop processes single ref pivot)
                1 if hit else 0,     # hit_count
                1 if hit else 0,     # confirmations_count (minimal placeholder)
                1 if not hit else 0, # candidates_count
                False,               # decision_ready (placeholder)
                [],                  # candidate_symbols (TEXT[])
                hashlib.sha1(f"{symbol}:{pivot_time}:{level}".encode()).hexdigest(),  # decision_signature
                False,               # signal_emitted (default false)
                duplicate_suppressed,# duplicate_suppressed
                comparisons,         # comparisons JSONB
                None,                # notes
            ))

            # --- Your current console print (kept) ---
            print(f"[DecisionEngine] {symbol} pivot @ {level} (hit={hit}, close_time={pivot_time})")

    if conn and rows:
        _insert_audit_rows(conn, rows)


def _insert_audit_rows(conn: psycopg.Connection, rows: List[tuple]) -> None:
    """
    Batch insert of audit rows into decision_audit_loops.
    Matches the schema in sql/001_create_decision_audit_loops.sql.
    """
    sql = """
        INSERT INTO decision_audit_loops (
            event_id, timeframe, ref_symbol, ref_pivot_type, ref_pivot_time, ref_pivot_uid,
            ref_hit, match_window_candles, n_rank, found_count, hit_count,
            confirmations_count, candidates_count, decision_ready, candidate_symbols,
            decision_signature, signal_emitted, duplicate_suppressed, comparisons, notes
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def _collect_pivots_from_registry(
    exchange: str,
    symbol: str,
    timeframe: str,
    pivot_type: str,
    max_lookback: int,
) -> List[Dict[str, Any]]:
    """
    Read pivots from PivotBufferRegistry (newest -> oldest) and shape them
    as dicts compatible with the signal engine.
    """
    reg = get_pivot_registry()
    pb = reg.get(exchange, symbol, timeframe)
    if pb is None:
        return []

    out: List[Dict[str, Any]] = []

    if pivot_type.upper() == "HIGH":
        it = pb.iter_peaks_newest_first()
    else:
        it = pb.iter_lows_newest_first()

    for p in it:
        # Map to a tolerant dict (you can extend with open_time/close_time later)
        out.append({
            "time": p.time,                        # if your pivot object exposes 'time'
            "hit": p.hit,
            "level": p.level,
            "uid": getattr(p, "uid", None),
            # "close_time": p.close_time if available in your object
        })
        if len(out) >= max_lookback:
            break

    return out
