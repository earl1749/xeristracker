from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ──────────────────────────────────────────────
# Enums
# ──────────────────────────────────────────────

class OrderType(Enum):
    MARKET_BUY   = "MARKET_BUY"
    MARKET_SELL  = "MARKET_SELL"
    LIMIT_BUY    = "LIMIT_BUY"
    LIMIT_SELL   = "LIMIT_SELL"
    CANCEL_LIMIT = "CANCEL_LIMIT"
    TRANSFER     = "TRANSFER"
    UNKNOWN      = "UNKNOWN"


# ──────────────────────────────────────────────
# AMM trade result (used by ConstantProductAMM)
# ──────────────────────────────────────────────

@dataclass
class AMMTradeProjection:
    new_price_usd:      float
    new_market_cap_usd: float
    price_impact_pct:   float
    tokens_received:    float = 0.0
    sol_received:       float = 0.0


@dataclass
class TradeResult:
    direction:               str
    spot_price_before_usd:   float
    spot_price_after_usd:    float
    avg_execution_price_usd: float
    price_impact_pct:        float
    slippage_pct:            float
    market_cap_before_usd:   float
    market_cap_after_usd:    float
    tokens_received:         float = 0.0
    sol_received:            float = 0.0
    sol_spent:               float = 0.0
    tokens_sold:             float = 0.0


# ──────────────────────────────────────────────
# Domain models
# ──────────────────────────────────────────────

@dataclass
class LimitOrder:
    signature:      str
    wallet:         str
    order_type:     OrderType
    token_amount:   float
    usd_value:      float
    predicted_mcap: float
    target_price:   float
    timestamp:      float
    is_active:      bool = True

@dataclass
class MarketState:
    current_price: float = 0.0
    current_market_cap: float = 0.0
    total_supply: float = 0.0

    sol_price_usd: float = 0.0

    pool_token_reserve: float = 0.0
    pool_quote_reserve: float = 0.0
    pool_liquidity_usd: float = 0.0

    quote_mint: str = ""
    quote_symbol: str = ""
    quote_to_usd: float = 0.0

    price_reference: float = 0.0
    last_price_update: float = 0.0
    last_alert_up_time: float = 0.0
    last_alert_down_time: float = 0.0
    last_alert_direction: str = ""
