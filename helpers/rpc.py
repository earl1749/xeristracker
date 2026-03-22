from __future__ import annotations

import asyncio
import io
import json
import random
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

matplotlib.use("Agg")

from config.settings import (
    GROQ_API_KEY, GROQ_MODEL, GROQ_URL, MINT, RPC_URL, WSOL_MINT,
)
from config.data_registy import KNOWN_TOKEN_LABELS
from helpers.formatters import _pick_best_pair, format_usd


# ── Timeframe map (also used by commands) ─────────────────────────────────────
TIMEFRAME_MAP = {
    "1m":  {"label": "1M",  "gt_timeframe": "minute", "aggregate": 1,  "resolution": 1},
    "5m":  {"label": "5M",  "gt_timeframe": "minute", "aggregate": 5,  "resolution": 5},
    "15m": {"label": "15M", "gt_timeframe": "minute", "aggregate": 15, "resolution": 15},
    "1h":  {"label": "1H",  "gt_timeframe": "hour",   "aggregate": 1,  "resolution": 60},
    "1d":  {"label": "1D",  "gt_timeframe": "day",    "aggregate": 1,  "resolution": 1440},
}


# ═════════════════════════════════════════════════════════════════════════════
# RPC helpers
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_tx(signature: str, retries: int = 3) -> Optional[Dict]:
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
        "params":  [signature, {"encoding": "jsonParsed",
                                "maxSupportedTransactionVersion": 0,
                                "commitment": "confirmed"}],
    }
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(RPC_URL, json=payload)
            if r.status_code == 200:
                result = r.json()
                if "error" not in result:
                    tx = result.get("result")
                    if tx:
                        return tx
        except Exception as e:
            print(f"   fetch attempt {attempt + 1}: {e}")
        if attempt < retries - 1:
            await asyncio.sleep(2 ** attempt + random.random())
    return None


async def fetch_price_for_ca(ca: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r    = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
            data = r.json()
        p = _pick_best_pair(data.get("pairs") or [])
        if not p:
            return {}
        return {
            "price":      float(p.get("priceUsd") or 0),
            "mcap":       float(p.get("fdv") or p.get("marketCap") or 0),
            "volume_24h": float((p.get("volume")    or {}).get("h24") or 0),
            "change_24h": float((p.get("priceChange") or {}).get("h24") or 0),
            "liquidity":  float((p.get("liquidity")  or {}).get("usd") or 0),
            "dex":        p.get("dexId", "unknown"),
            "pair_addr":  p.get("pairAddress", ""),
            "name":       p.get("baseToken", {}).get("name",   "Unknown"),
            "symbol":     p.get("baseToken", {}).get("symbol", "???"),
        }
    except Exception as e:
        print(f"❌ DexScreener error: {e}")
        return {}


async def fetch_top_holders(ca: str) -> list:
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method":  "getTokenLargestAccounts",
        "params":  [ca, {"commitment": "confirmed"}],
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(RPC_URL, json=payload)
        return r.json().get("result", {}).get("value", [])
    except Exception as e:
        print(f"❌ Holder fetch error: {e}")
        return []


async def fetch_token_metadata(ca: str) -> dict:
    result = {
        "deployer": None, "mint_authority": None, "freeze_authority": None,
        "created_at": None, "token_age_days": None, "decimals": 6, "supply": 0,
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r    = await client.post(RPC_URL, json={
                "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                "params":  [ca, {"encoding": "jsonParsed", "commitment": "confirmed"}],
            })
            info = (r.json().get("result", {}).get("value", {})
                    .get("data", {}).get("parsed", {}).get("info", {}))
            result["mint_authority"]   = info.get("mintAuthority")
            result["freeze_authority"] = info.get("freezeAuthority")
            result["decimals"]         = info.get("decimals", 6)
            result["supply"]           = int(info.get("supply", "0")) / (10 ** result["decimals"])

        async with httpx.AsyncClient(timeout=20.0) as client:
            r    = await client.post(RPC_URL, json={
                "jsonrpc": "2.0", "id": 1,
                "method":  "getSignaturesForAddress",
                "params":  [ca, {"limit": 1000, "commitment": "confirmed"}],
            })
            sigs = r.json().get("result", [])
            if sigs:
                bt = sigs[-1].get("blockTime")
                if bt:
                    dt = datetime.fromtimestamp(bt, tz=timezone.utc)
                    result["created_at"]     = dt.strftime("%Y-%m-%d %H:%M UTC")
                    result["token_age_days"] = (datetime.now(timezone.utc) - dt).days
    except Exception as e:
        print(f"⚠️ Token metadata error: {e}")
    return result


async def fetch_pumpfun_metadata(ca: str) -> dict:
    result = {
        "is_pumpfun": False, "creator": None, "description": None, "graduated": False,
        "reply_count": 0, "name": None, "symbol": None, "image_url": None,
        "telegram": None, "twitter": None, "website": None,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"https://frontend-api.pump.fun/coins/{ca}")
            if r.status_code == 200:
                d = r.json()
                result.update({
                    "is_pumpfun":   True,
                    "creator":      d.get("creator"),
                    "description":  (d.get("description") or "")[:300],
                    "graduated":    d.get("complete", False),
                    "reply_count":  d.get("reply_count", 0),
                    "name":         d.get("name"),
                    "symbol":       d.get("symbol"),
                    "image_url":    d.get("image_uri"),
                    "telegram":     d.get("telegram"),
                    "twitter":      d.get("twitter"),
                    "website":      d.get("website"),
                })
    except Exception as e:
        print(f"⚠️ Pump.fun error: {e}")
    return result


async def scan_socials(ca: str, token_name: str, token_symbol: str) -> dict:
    results = {"twitter": None, "website": None, "twitter_handle": None}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r    = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
            data = r.json()
            pairs = data.get("pairs") or []
            if pairs:
                info = pairs[0].get("info") or {}
                for s in info.get("socials") or []:
                    if s.get("type") == "twitter":
                        results["twitter"] = s.get("url")
                        handle = (s.get("url") or "").rstrip("/").split("/")[-1]
                        results["twitter_handle"] = f"@{handle}" if handle else None
                for w in info.get("websites") or []:
                    if w.get("url"):
                        results["website"] = w["url"]
                        break
    except Exception as e:
        print(f"⚠️ Social scan error: {e}")
    return results


async def fetch_deployer_history(deployer_wallet: str) -> dict:
    result = {"wallet": deployer_wallet, "total_prev": 0, "wallet_age_days": None}
    if not deployer_wallet:
        return result
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r    = await client.post(RPC_URL, json={
                "jsonrpc": "2.0", "id": 1,
                "method":  "getSignaturesForAddress",
                "params":  [deployer_wallet, {"limit": 1000, "commitment": "confirmed"}],
            })
            sigs = r.json().get("result", [])
            if sigs:
                bt = sigs[-1].get("blockTime")
                if bt:
                    dt = datetime.fromtimestamp(bt, tz=timezone.utc)
                    result["wallet_age_days"] = (datetime.now(timezone.utc) - dt).days
            result["total_prev"] = max(0, len(sigs) - 1)
    except Exception as e:
        print(f"⚠️ Deployer history error: {e}")
    return result


async def fetch_all_intelligence(ca: str, name: str, symbol: str) -> dict:
    print(f"   🔎 Gathering intelligence for {ca[:16]}...")
    token_meta, pumpfun, socials = await asyncio.gather(
        fetch_token_metadata(ca), fetch_pumpfun_metadata(ca), scan_socials(ca, name, symbol)
    )
    if pumpfun.get("twitter") and not socials.get("twitter"):
        socials["twitter"] = pumpfun["twitter"]
        handle = pumpfun["twitter"].rstrip("/").split("/")[-1]
        socials["twitter_handle"] = f"@{handle}"
    if pumpfun.get("website") and not socials.get("website"):
        socials["website"] = pumpfun["website"]
    if pumpfun.get("telegram"):
        socials["telegram"] = pumpfun["telegram"]
    deployer_wallet = token_meta.get("mint_authority") or pumpfun.get("creator")
    deployer = await fetch_deployer_history(deployer_wallet) if deployer_wallet else {}
    return {"token_meta": token_meta, "pumpfun": pumpfun, "socials": socials, "deployer": deployer}


# ═════════════════════════════════════════════════════════════════════════════
# AI analysis
# ═════════════════════════════════════════════════════════════════════════════

async def groq_analyze(ca: str, price_data: dict, holders: list, intelligence: dict) -> dict:
    socials    = intelligence.get("socials", {})
    token_meta = intelligence.get("token_meta", {})
    pumpfun    = intelligence.get("pumpfun", {})
    deployer   = intelligence.get("deployer", {})
    total_supply = 1_000_000_000
    amounts      = [float(h.get("uiAmount") or 0) for h in holders] if holders else []
    top10_pct    = sum(amounts[:10]) / total_supply * 100 if amounts else 0.0
    top5_pct     = sum(amounts[:5])  / total_supply * 100 if amounts else 0.0
    biggest      = (amounts[0] / total_supply * 100) if amounts else 0.0
    holder_lines = "\n".join(
        f"  #{i+1}: {float(h.get('uiAmount',0)):,.0f} tokens "
        f"({float(h.get('uiAmount',0))/total_supply*100:.2f}%)"
        for i, h in enumerate(holders[:10])
    ) if holders else "  No holder data"
    deployer_age = deployer.get("wallet_age_days")
    prompt = f"""You are an elite crypto risk analyst. Return ONLY raw JSON — no markdown.

CONTRACT: {ca}
Token: {price_data.get('name','Unknown')} ({price_data.get('symbol','???')})
Price: ${price_data.get('price',0):.8f}  MCap: ${price_data.get('mcap',0):,.0f}  24h Vol: ${price_data.get('volume_24h',0):,.0f}
Liquidity: ${price_data.get('liquidity',0):,.0f}  DEX: {price_data.get('dex','unknown')}
Mint Authority: {token_meta.get('mint_authority') or 'REVOKED'}
Freeze Authority: {token_meta.get('freeze_authority') or 'REVOKED'}
Token Age: {token_meta.get('token_age_days','?')} days
Deployer Age: {f"{deployer_age} days" if deployer_age else "Unknown"}
Pump.fun: {pumpfun.get('is_pumpfun',False)}  Graduated: {pumpfun.get('graduated',False)}
Holders Top5: {top5_pct:.2f}%  Top10: {top10_pct:.2f}%  Biggest: {biggest:.2f}%
{holder_lines}
Twitter: {socials.get('twitter_handle') or 'NOT FOUND'}
Website: {socials.get('website') or 'NOT FOUND'}

Return: {{"risk_score":<0-10>,"rug_label":"<LIKELY SAFE|PROCEED WITH CAUTION|HIGH RISK|LIKELY RUG>","summary":"<2 sentences>","team_analysis":"<2-3 sentences>","red_flags":["<max 5>"],"green_flags":["<max 4>"],"holder_analysis":"<2 sentences>","liquidity_analysis":"<2 sentences>","social_analysis":"<2 sentences>","mint_freeze_risk":"<1 sentence>","trade_advice":"<1 sentence>"}}"""
    try:
        async with httpx.AsyncClient(timeout=35.0) as client:
            r = await client.post(
                GROQ_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 1200, "temperature": 0.3},
            )
            if r.status_code == 200:
                raw = r.json()["choices"][0]["message"]["content"]
                return json.loads(raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip())
            return {"error": f"Groq API error {r.status_code}"}
    except Exception as e:
        return {"error": str(e)}


# ═════════════════════════════════════════════════════════════════════════════
# GeckoTerminal / chart
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_geckoterminal(ca: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ca}/pools",
                headers={"Accept": "application/json;version=20230302"},
            )
        if r.status_code != 200:
            return {}
        pools = r.json().get("data", []) or []
        if not pools:
            return {}

        def score(p):
            a = p.get("attributes", {}) or {}
            return (
                float(a.get("reserve_in_usd") or 0),
                float((a.get("volume_usd") or {}).get("h24") or 0),
                float(a.get("fdv_usd") or 0),
            )

        best  = max(pools, key=score)
        attrs = best.get("attributes", {}) or {}
        return {
            "pool_address":    best.get("id", "").replace("solana_", ""),
            "name":            attrs.get("name", "Unknown"),
            "price_usd":       float(attrs.get("base_token_price_usd") or 0),
            "price_change_5m": float((attrs.get("price_change_percentage") or {}).get("m5")  or 0),
            "price_change_1h": float((attrs.get("price_change_percentage") or {}).get("h1")  or 0),
            "price_change_24h":float((attrs.get("price_change_percentage") or {}).get("h24") or 0),
            "volume_24h":      float((attrs.get("volume_usd") or {}).get("h24") or 0),
            "liquidity":       float(attrs.get("reserve_in_usd") or 0),
            "fdv":             float(attrs.get("fdv_usd") or 0),
            "market_cap":      float(attrs.get("market_cap_usd") or 0),
            "buys_24h":        int((attrs.get("transactions") or {}).get("h24", {}).get("buys")  or 0),
            "sells_24h":       int((attrs.get("transactions") or {}).get("h24", {}).get("sells") or 0),
        }
    except Exception as e:
        print(f"❌ GeckoTerminal error: {e}")
        return {}


async def fetch_geckoterminal_ohlcv(
    pool_address: str, timeframe: str = "minute", aggregate: int = 1, limit: int = 100
) -> Optional[list]:
    try:
        url = (f"https://api.geckoterminal.com/api/v2/networks/solana/pools/"
               f"{pool_address}/ohlcv/{timeframe}?aggregate={aggregate}&limit={limit}")
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(url, headers={"Accept": "application/json;version=20230302"})
        if r.status_code != 200:
            return None
        rows = r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if not rows:
            return None
        candles = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            try:
                candles.append({
                    "unixTime": int(row[0]), "o": float(row[1]), "h": float(row[2]),
                    "l": float(row[3]),      "c": float(row[4]), "v": float(row[5]),
                })
            except Exception:
                continue
        candles.sort(key=lambda x: x["unixTime"])
        if len(candles) < 5:
            return None
        print(f"✅ GeckoTerminal OHLCV fetched {len(candles)} candles")
        return candles
    except Exception as e:
        print(f"❌ GeckoTerminal OHLCV error: {e}")
        return None


async def generate_chart_image(
    ca: str, timeframe: str, token_name: str, pool_address: str
) -> Optional[bytes]:
    tf_cfg  = TIMEFRAME_MAP.get(timeframe, TIMEFRAME_MAP["15m"])
    candles = await fetch_geckoterminal_ohlcv(pool_address, tf_cfg["gt_timeframe"], tf_cfg["aggregate"], 100)
    if not candles:
        return None
    try:
        BG = "#0d1117"; GRID = "#21262d"; GREEN = "#26a641"; RED = "#da3633"
        VOL_GREEN = "#1a4d2e"; VOL_RED = "#4d1a1a"; TEXT = "#e6edf3"; SUBTEXT = "#8b949e"

        timestamps = [c["unixTime"] for c in candles]
        opens  = [float(c["o"]) for c in candles]; highs  = [float(c["h"]) for c in candles]
        lows   = [float(c["l"]) for c in candles]; closes = [float(c["c"]) for c in candles]
        volumes= [float(c["v"]) for c in candles]; n = len(candles); xs = list(range(n))

        fig, (ax, ax_vol) = plt.subplots(2, 1, figsize=(13, 7),
            gridspec_kw={"height_ratios": [3, 1], "hspace": 0.03}, facecolor=BG)

        for a in (ax, ax_vol):
            a.set_facecolor(BG); a.tick_params(colors=SUBTEXT, labelsize=7.5)
            for spine in a.spines.values(): spine.set_color(GRID)
            a.yaxis.grid(True, color=GRID, linewidth=0.4, linestyle="--", alpha=0.5)
            a.xaxis.grid(False)

        low_min = min(lows); high_max = max(highs)
        whole_range = max(high_max - low_min, 1e-12)
        min_body = whole_range * 0.0015; width = 0.55

        for i in xs:
            o = opens[i]; h = highs[i]; l = lows[i]; c = closes[i]
            color = GREEN if c >= o else RED
            ax.plot([i, i], [l, h], color=color, linewidth=0.9, zorder=2)
            bh = max(abs(c - o), min_body); bb = (o + c) / 2 - bh / 2
            ax.bar(i, bh, bottom=bb, width=width, color=color, linewidth=0, zorder=3)

        for i in xs:
            ax_vol.bar(i, volumes[i], width=width,
                       color=VOL_GREEN if closes[i] >= opens[i] else VOL_RED, linewidth=0, alpha=0.9)

        last_close = closes[-1]
        fmt = ("%.10f" if last_close < 0.000001 else "%.8f" if last_close < 0.0001
               else "%.6f" if last_close < 0.01 else "%.4f")
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter(fmt)); ax.yaxis.tick_right()
        ax_vol.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}K" if x >= 1000 else f"{x:.0f}"))
        ax_vol.yaxis.tick_right()

        step = max(1, n // 8); tick_xs = xs[::step]; tick_labels = []
        resolution = tf_cfg["resolution"]
        for i in tick_xs:
            dt = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
            if   resolution >= 1440: tick_labels.append(dt.strftime("%m/%d"))
            elif resolution >= 60:   tick_labels.append(dt.strftime("%d %H:%M"))
            else:                    tick_labels.append(dt.strftime("%H:%M"))
        ax_vol.set_xticks(tick_xs); ax_vol.set_xticklabels(tick_labels, color=SUBTEXT, fontsize=7)
        ax.set_xticks([])

        first_close = closes[0]
        pct         = ((last_close - first_close) / first_close * 100) if first_close > 0 else 0.0
        pct_color   = GREEN if pct >= 0 else RED; pct_sign = "+" if pct >= 0 else ""

        fig.text(0.01, 0.97,  f"{token_name}", color=TEXT,      fontsize=13, fontweight="bold", va="top")
        fig.text(0.01, 0.925, f"{fmt % last_close}   {pct_sign}{pct:.2f}%   {tf_cfg['label']}",
                 color=pct_color, fontsize=10, va="top")
        fig.text(0.99, 0.97,  "GeckoTerminal · XerisBot", color=SUBTEXT, fontsize=8, va="top", ha="right")

        ax.set_xlim(-0.6, n - 0.4); ax_vol.set_xlim(-0.6, n - 0.4)
        price_range = max(high_max - low_min, max(last_close * 0.02, 1e-12))
        pad = price_range * 0.06; ax.set_ylim(low_min - pad, high_max + pad)

        plt.tight_layout(rect=[0, 0, 1, 0.92])
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=140, bbox_inches="tight", facecolor=BG, edgecolor="none")
        plt.close(fig); buf.seek(0)
        print(f"✅ Chart rendered ({len(candles)} candles)")
        return buf.read()
    except Exception as e:
        print(f"❌ Chart render error: {e}")
        try: plt.close("all")
        except Exception: pass
        return None
