# file: src/strategy/pivot_corr_engine.py
# English-only comments

from datetime import datetime, timedelta
import hashlib
import threading
from typing import Any, Dict, List, Optional, Tuple

import psycopg  # psycopg3
from psycopg.types.json import Json

# Access CandleBuffer via package-qualified path (absolute imports)
from buffers.candle_buffer import Keys  # kept if you need elsewhere

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
# Public API
# --------------------------------------------------------------------

def run_decision_event(
    *,
    exchange: str,
    symbols: List[str],
    timeframe: str,
    event_time: datetime,
    signal_memory: SignalMemory,
    groups: Optional[Dict[str, List[str]]] = None,  # two groups: same-direction & opposite-direction
    conn: Optional[psycopg.Connection] = None,      # DB connection for audit inserts
    max_lookback: int = 50,                         # how many ref pivots (newest->oldest) to scan
    window: int = 3                                 # +/- candles window for time matching
) -> None:
    """
    Runs per candle-close event to analyze pivots and produce decisions.
    For each ref symbol and for both pivot types (HIGH/LOW):
      - iterate ref pivots (newest -> oldest, up to max_lookback)
      - compare with peers in same group using SAME pivot type
      - compare with peers in the opposite group using OPPOSITE pivot type
      - for each (ref, peer, ref_pivot) combination, write 1 row into decision_audit_loops
    """
    reg = get_pivot_registry()

    # Resolve two logical groups from `groups` (names don't matter).
    same_group, opposite_group = _resolve_two_groups(groups, symbols)

    # Pre-cache peer pivot lists for both types to save repeated registry reads
    # cache[(symbol, "HIGH")] = list[dict]; cache[(symbol, "LOW")] = list[dict]
    cache: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    def pivots_for(sym: str, ptype: str) -> List[Dict[str, Any]]:
        key = (sym, ptype)
        if key not in cache:
            cache[key] = _collect_pivots_from_registry(exchange, sym, timeframe, ptype, max_lookback=max_lookback)
        return cache[key]

    # We will gather rows and insert in batch
    batch_rows: List[tuple] = []

    print(f"Symbols:{symbols}")
    print(f"Same Group:{same_group}")
    print(f"Opp Group:{opposite_group}")

    # For each ref symbol
    for ref_symbol in symbols:
        print("===========================")
        print(f"Ref Symbil: {ref_symbol}")
        # Determine which group the ref belongs to
        if ref_symbol in same_group:
            peers_same = [s for s in same_group if s != ref_symbol]
            peers_opp  = list(opposite_group)
            #print("A")
            #print(f"peers_same: {peers_same}")
            #print(f"peers_opp: {peers_opp}")

        elif ref_symbol in opposite_group:
            peers_same = [s for s in opposite_group if s != ref_symbol]
            peers_opp  = list(same_group)
            #print("B")
            #print(f"peers_same: {peers_same}")
            #print(f"peers_opp: {peers_opp}")
        else:
            # If a symbol is not in any group, just compare against all others as "same"
            peers_same = [s for s in symbols if s != ref_symbol]
            peers_opp  = []
            #print("C")
            #print(f"peers_same: {peers_same}")
            #print(f"peers_opp: {peers_opp}")


              
        # For both pivot types on the ref symbol
        for ref_type in ("HIGH", "LOW"):
            ref_pivots = pivots_for(ref_symbol, ref_type)
            
            #print("For High/Low")
            #print(f"Ref Type:{ref_type}")
            #print(f"Ref Pivots: {ref_pivots}")

            # Iterate newest -> oldest (already newest-first)
            for rank, rp in enumerate(ref_pivots, start=1):
                ref_time = rp.get("close_time") or rp.get("time")
                ref_hit  = bool(rp.get("hit"))
                ref_uid  = rp.get("uid") or _sha1(f"{ref_symbol}:{ref_time}:{ref_type}")

                #print(f"ref_time:{ref_time}")
                #print(f"ref_hit:{ref_hit}")
                #print(f"rp:{rp}")
                
                # SAME-GROUP: compare with same pivot type
                rows_same, counters_same = _compare_ref_with_peers(
                    ref_symbol=ref_symbol,
                    ref_type=ref_type,
                    ref_time=ref_time,
                    peers=peers_same,
                    peer_type=ref_type,               # same direction
                    pivots_fetcher=pivots_for,
                    window=window
                )
                if (ref_symbol =="EUR/USD"):
                    print("Same Side....")
                    print(f"ref_time: {ref_time}")
                    print(f"rows_same:{rows_same}")
                    print(f"counters_same:{counters_same}")


                # OPPOSITE-GROUP: compare with opposite pivot type
                opposite_type = "LOW" if ref_type == "HIGH" else "HIGH"
                rows_opp, counters_opp = _compare_ref_with_peers(
                    ref_symbol=ref_symbol,
                    ref_type=ref_type,
                    ref_time=ref_time,
                    peers=peers_opp,
                    peer_type=opposite_type,          # opposite direction
                    pivots_fetcher=pivots_for,
                    window=window
                )
                
                if (ref_symbol =="EUR/USD"):
                    print("Opp Side....")
                    print(f"ref_time: {ref_time}")
                    print(f"rows_opp:{rows_opp}")
                    print(f"counters_opp:{counters_opp}")

                # Build the aggregated "comparisons" JSON and counts
                comparisons = rows_same + rows_opp   # list of dicts
                found_count = len(comparisons)
                hit_count   = counters_same["hit"] + counters_opp["hit"]
                conf_count  = hit_count              # simplification: confirmations = hits
                cand_count  = found_count - hit_count
                decision_ready = conf_count >= 3     # your rule of minimum confirmations

                decision_signature = _sha1(f"{ref_symbol}:{ref_type}:{ref_time}:{found_count}:{hit_count}")

                # Duplicate suppression doesn't prevent audit rows; it just marks the flag.
                duplicate_suppressed = not signal_memory.remember(decision_signature)

                # Candidate symbols list for audit (peer symbols only)
                candidate_symbols = [c["symbol"] for c in comparisons]

                # Build one audit row for THIS (ref_symbol, ref_type, ref_pivot)
                batch_rows.append((
                    event_time,                     # event_id
                    timeframe,                      # timeframe
                    ref_symbol,                     # ref_symbol
                    ref_type,                       # ref_pivot_type
                    ref_time,                       # ref_pivot_time
                    ref_uid,                        # ref_pivot_uid
                    ref_hit,                        # ref_hit
                    window,                         # match_window_candles
                    rank,                           # n_rank (1=newest)
                    found_count,                    # found_count
                    hit_count,                      # hit_count
                    conf_count,                     # confirmations_count
                    cand_count,                     # candidates_count
                    decision_ready,                 # decision_ready
                    candidate_symbols,              # candidate_symbols (TEXT[])
                    decision_signature,             # decision_signature
                    False,                          # signal_emitted (placeholder)
                    duplicate_suppressed,           # duplicate_suppressed
                    Json(comparisons),              # comparisons JSONB
                    None                            # notes
                ))

    # Insert once per loop
    if conn and batch_rows:
        _insert_audit_rows(conn, batch_rows)


# --------------------------------------------------------------------
# Peer comparison helpers
# --------------------------------------------------------------------

def _compare_ref_with_peers(
    *,
    ref_symbol: str,
    ref_type: str,
    ref_time: datetime,
    peers: List[str],
    peer_type: str,
    pivots_fetcher,
    window: int
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    For a given ref (symbol, type, time), scan each peer list and try to find a
    pivot of `peer_type` within +/- `window` minutes around `ref_time`.

    Returns:
      - comparisons: list[dict] per peer, including found/time/hit/delta/level/uid/symbol/type
      - counters: {"hit": int}
    """
    comparisons: List[Dict[str, Any]] = []
    hit_counter = 0

    for peer in peers:
        # Pull newest-first pivots for this peer and type
        piv_list = pivots_fetcher(peer, peer_type)

        # Find the closest pivot around ref_time within +/- window minutes
        matched = _find_pivot_in_window(piv_list, ref_time, window_minutes=window)

        if matched is None:
            comparisons.append({
                "symbol": peer,
                "type": peer_type,
                "found": False,
                "time": None,
                "hit": False,
                "delta_min": None,
                "level": None,
                "uid": None,
            })
            continue

        mp = matched
        m_time  = mp.get("close_time") or mp.get("time")
        m_hit   = bool(mp.get("hit"))
        m_level = mp.get("level")
        m_uid   = mp.get("uid") or _sha1(f"{peer}:{m_time}:{peer_type}")
        delta_m = _minutes_diff(m_time, ref_time)

        if m_hit:
            hit_counter += 1

        comparisons.append({
            "symbol": peer,
            "type": peer_type,
            "found": True,
            "time": m_time,
            "hit": m_hit,
            "delta_min": delta_m,
            "level": m_level,
            "uid": m_uid,
        })

    return comparisons, {"hit": hit_counter}


def _find_pivot_in_window(
    pivots: List[Dict[str, Any]],
    ref_time: datetime,
    window_minutes: int
) -> Optional[Dict[str, Any]]:
    """
    Find the pivot whose time is within +/- window_minutes of ref_time.
    If multiple candidates exist, choose the one with the smallest |delta|.
    `pivots` are expected newest-first.
    """
    best: Optional[Tuple[int, Dict[str, Any]]] = None  # (abs_delta, pivot)

    for p in pivots:
        t = p.get("close_time") or p.get("time")
        if not isinstance(t, datetime):
            continue
        delta = int(abs((t - ref_time).total_seconds()) // 60)
        if delta <= window_minutes:
            if best is None or delta < best[0]:
                best = (delta, p)
                if delta == 0:
                    break  # exact match is best

    return best[1] if best else None


# --------------------------------------------------------------------
# Registry access & DB IO
# --------------------------------------------------------------------

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
        # Tolerant mapping: prefer close_time if your object has it; otherwise use p.time
        close_t = getattr(p, "close_time", None)
        time_t  = getattr(p, "time", None)
        out.append({
            "time": time_t,
            "close_time": close_t or time_t,
            "hit": bool(getattr(p, "is_hit", False)),
            "level": getattr(p, "level", None),
            "uid": getattr(p, "uid", None),
        })
        if len(out) >= max_lookback:
            break

    return out


# --------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------

def _sha1(s: str) -> str:
    import hashlib as _h
    return _h.sha1(s.encode()).hexdigest()

def _minutes_diff(t1: datetime, t2: datetime) -> int:
    return int(abs((t1 - t2).total_seconds()) // 60)


def _resolve_two_groups(groups: Optional[Dict[str, List[str]]], symbols: List[str]) -> Tuple[List[str], List[str]]:
    """
    Resolve two logical groups:
      - same_group: group containing symbols that move together
      - opposite_group: the other group (mirror / opposite)

    Names do NOT matter. If groups is None or malformed, fall back to:
      same_group = symbols, opposite_group = []
    """
    if not groups:
        return list(symbols), []

    vals = [list(v) for v in groups.values() if v]
    if not vals:
        return list(symbols), []

    if len(vals) == 1:
        return vals[0], [s for s in symbols if s not in vals[0]]

    # Use the first two non-empty lists
    return vals[0], vals[1]
