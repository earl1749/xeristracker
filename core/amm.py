from __future__ import annotations

from math import sqrt
from typing import Dict, Optional

from core.models import AMMTradeProjection, TradeResult


class ConstantProductAMM:
    """
    Constant-product AMM (x * y = k).

    Parameters
    ----------
    token_reserve : float   – token units currently in the pool
    sol_reserve   : float   – SOL units currently in the pool
    total_supply  : float   – token total supply (for market-cap calc)
    sol_price_usd : float   – current SOL/USD price
    fee_rate      : float   – e.g. 0.0025 for 0.25 %
    """

    def __init__(
        self,
        token_reserve: float,
        sol_reserve: float,
        total_supply: float,
        sol_price_usd: float,
        fee_rate: float = 0.0,
    ) -> None:
        if token_reserve <= 0 or sol_reserve <= 0:
            raise ValueError("AMM reserves must be > 0")
        if total_supply <= 0:
            raise ValueError("total_supply must be > 0")
        if sol_price_usd <= 0:
            raise ValueError("sol_price_usd must be > 0")
        if not (0.0 <= fee_rate < 1.0):
            raise ValueError("fee_rate must be in [0, 1)")

        self.token_reserve = float(token_reserve)
        self.sol_reserve   = float(sol_reserve)
        self.total_supply  = float(total_supply)
        self.sol_price_usd = float(sol_price_usd)
        self.fee_rate      = float(fee_rate)

    # ── Properties ──────────────────────────────────────────────────────────

    @property
    def k(self) -> float:
        return self.token_reserve * self.sol_reserve

    @property
    def price_sol(self) -> float:
        return self.sol_reserve / self.token_reserve

    @property
    def price_usd(self) -> float:
        return self.price_sol * self.sol_price_usd

    @property
    def market_cap_usd(self) -> float:
        return self.price_usd * self.total_supply

    @property
    def liquidity_usd(self) -> float:
        return (self.token_reserve * self.price_usd) + (self.sol_reserve * self.sol_price_usd)

    # ── Snapshot / clone ────────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, float]:
        return {
            "token_reserve": self.token_reserve,
            "sol_reserve":   self.sol_reserve,
            "price_sol":     self.price_sol,
            "price_usd":     self.price_usd,
            "market_cap_usd": self.market_cap_usd,
            "liquidity_usd": self.liquidity_usd,
            "k":             self.k,
        }

    def clone(self) -> "ConstantProductAMM":
        return ConstantProductAMM(
            token_reserve=self.token_reserve,
            sol_reserve=self.sol_reserve,
            total_supply=self.total_supply,
            sol_price_usd=self.sol_price_usd,
            fee_rate=self.fee_rate,
        )

    # ── Buy: user sends SOL, receives tokens ────────────────────────────────

    def buy_with_sol(self, sol_in: float, mutate: bool = False) -> AMMTradeProjection:
        if sol_in <= 0:
            return AMMTradeProjection(
                new_price_usd=self.price_usd,
                new_market_cap_usd=self.market_cap_usd,
                price_impact_pct=0.0,
            )

        old_price  = self.price_usd
        eff_sol_in = sol_in * (1.0 - self.fee_rate)

        new_sol   = self.sol_reserve + eff_sol_in
        new_token = self.k / new_sol
        tokens_out = self.token_reserve - new_token

        new_price = (new_sol / new_token) * self.sol_price_usd
        new_mcap  = new_price * self.total_supply
        impact    = ((new_price - old_price) / old_price) * 100.0

        if mutate:
            self.sol_reserve   = new_sol
            self.token_reserve = new_token

        return AMMTradeProjection(
            new_price_usd=new_price,
            new_market_cap_usd=new_mcap,
            price_impact_pct=impact,
            tokens_received=tokens_out,
        )

    # ── Sell: user sends tokens, receives SOL ───────────────────────────────

    def sell_tokens(self, token_in: float, mutate: bool = False) -> AMMTradeProjection:
        if token_in <= 0:
            return AMMTradeProjection(
                new_price_usd=self.price_usd,
                new_market_cap_usd=self.market_cap_usd,
                price_impact_pct=0.0,
            )

        old_price   = self.price_usd
        eff_tok_in  = token_in * (1.0 - self.fee_rate)

        new_token = self.token_reserve + eff_tok_in
        new_sol   = self.k / new_token
        sol_out   = self.sol_reserve - new_sol

        new_price = (new_sol / new_token) * self.sol_price_usd
        new_mcap  = new_price * self.total_supply
        impact    = ((new_price - old_price) / old_price) * 100.0

        if mutate:
            self.token_reserve = new_token
            self.sol_reserve   = new_sol

        return AMMTradeProjection(
            new_price_usd=new_price,
            new_market_cap_usd=new_mcap,
            price_impact_pct=impact,
            sol_received=sol_out,
        )

    # ── Volume needed to reach a target mcap ────────────────────────────────

    def sol_needed_for_target_mcap(self, target_mcap_usd: float) -> float:
        if target_mcap_usd <= self.market_cap_usd:
            return 0.0
        ratio = target_mcap_usd / self.market_cap_usd
        return self.sol_reserve * (sqrt(ratio) - 1.0) / (1.0 - self.fee_rate)

    def token_needed_for_target_mcap(self, target_mcap_usd: float) -> float:
        if target_mcap_usd >= self.market_cap_usd or target_mcap_usd <= 0:
            return 0.0
        ratio = target_mcap_usd / self.market_cap_usd
        return self.token_reserve * ((1.0 / sqrt(ratio)) - 1.0) / (1.0 - self.fee_rate)

    # ── Market depth ────────────────────────────────────────────────────────

    def market_depth(self, pct: float) -> Dict[str, float]:
        """Return quote / token needed to move spot price by `pct` percent."""
        p = pct / 100.0
        if p <= 0 or p >= 1:
            return {
                "sol_for_up_move":      0.0,
                "usd_for_up_move":      0.0,
                "tokens_for_down_move": 0.0,
                "usd_for_down_move":    0.0,
            }

        gross_sol_up    = self.sol_reserve   * (sqrt(1.0 + p) - 1.0)       / (1.0 - self.fee_rate)
        gross_tok_down  = self.token_reserve * ((1.0 / sqrt(1.0 - p)) - 1.0) / (1.0 - self.fee_rate)

        return {
            "sol_for_up_move":      gross_sol_up,
            "usd_for_up_move":      gross_sol_up   * self.sol_price_usd,
            "tokens_for_down_move": gross_tok_down,
            "usd_for_down_move":    gross_tok_down * self.price_usd,
        }

    # ── Factory: build from market-cap + liquidity ──────────────────────────

    @staticmethod
    def from_mcap_and_liquidity(
        market_cap_usd: float,
        liquidity_usd:  float,
        total_supply:   float,
        sol_price_usd:  float,
        fee_rate:       float = 0.0,
    ) -> "ConstantProductAMM":
        """Build an approximate balanced pool from observable market data."""
        if market_cap_usd <= 0 or liquidity_usd <= 0:
            raise ValueError("market_cap_usd and liquidity_usd must be > 0")
        price_usd     = market_cap_usd / total_supply
        token_reserve = (liquidity_usd / 2.0) / price_usd
        sol_reserve   = (liquidity_usd / 2.0) / sol_price_usd
        return ConstantProductAMM(
            token_reserve=token_reserve,
            sol_reserve=sol_reserve,
            total_supply=total_supply,
            sol_price_usd=sol_price_usd,
            fee_rate=fee_rate,
        )