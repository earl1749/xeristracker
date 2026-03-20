"""
Loads all data/*.json files and builds the program/token registry sets
that the rest of the app depends on.
"""

from __future__ import annotations

from typing import Dict, Set, Any

from utils.json_loader import load_json, load_programs as _load_programs_raw

# ── Token labels ──────────────────────────────────────────────────────────────
KNOWN_TOKEN_LABELS: Dict[str, str] = load_json("data/tokens.json", default={})

# ── Instruction discriminators ───────────────────────────────────────────────
_raw_disc: Dict[str, str] = load_json("data/discriminators.json", default={})
DISCRIMINATORS: Dict[bytes, str] = {
    bytes.fromhex(k): v for k, v in _raw_disc.items()
}

# ── System programs ──────────────────────────────────────────────────────────
_system_list: list = load_json("data/system_programs.json", default=[])
SYSTEM_PROGRAMS: Set[str] = set(_system_list)

# ── Programs registry (from programs.json) ───────────────────────────────────
_PROGRAMS_CACHE: Dict[str, Any] = _load_programs_raw()

EXCHANGE_REGISTRY: Dict[str, Dict] = _PROGRAMS_CACHE.get("known_programs", {})

DEX_PROGRAMS: Set[str] = {
    pid for pid, v in EXCHANGE_REGISTRY.items()
    if v.get("role") in ("market", "hybrid")
}
LIMIT_ORDER_PROGRAMS: Set[str] = {
    pid for pid, v in EXCHANGE_REGISTRY.items()
    if v.get("role") in ("limit", "hybrid")
}
ALL_KNOWN_PROGRAMS: Set[str] = set(EXCHANGE_REGISTRY.keys())

AGGREGATOR_PROGRAMS: Set[str] = set(_PROGRAMS_CACHE.get("aggregator_programs", []))
SWAP_PROGRAMS:       Set[str] = set(_PROGRAMS_CACHE.get("swap_programs",       []))
ALL_SWAP_PROGRAMS:   Set[str] = AGGREGATOR_PROGRAMS | SWAP_PROGRAMS

TOKEN_PROGRAMS: Set[str] = set(_PROGRAMS_CACHE.get("token_programs", [
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
]))


def exchange_name(pid: str) -> str:
    """Return a human-readable exchange name for a program ID."""
    entry = EXCHANGE_REGISTRY.get(pid)
    return entry["name"] if entry else f"Unknown ({pid[:8]}…)"