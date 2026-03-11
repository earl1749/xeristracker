import asyncio
import aiohttp
import json
from datetime import datetime
from typing import Dict, List, Optional
import time
import os
from dotenv import load_dotenv

load_dotenv()

class PumpEndingScannerHelius:
    def __init__(self, discord_webhook_url: str, helius_api_key: str):
        self.webhook_url = discord_webhook_url
        self.helius_api_key = helius_api_key
        self.helius_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
        self.pumpfun_program = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
        self.seen_tokens = set()
        
    async def fetch_recent_pump_tokens(self, session: aiohttp.ClientSession) -> List[Dict]:
        try:
            url = f"https://api.helius.xyz/v0/addresses/{self.pumpfun_program}/transactions?api-key={self.helius_api_key}&limit=100"
            
            headers = {
                "Accept": "application/json"
            }
            
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list):
                        print(f"✅ Found {len(data)} recent pump.fun transactions")
                        return data
                    return []
                else:
                    print(f"Helius API returned status {response.status}")
                    return []
        except Exception as e:
            print(f"Error fetching from Helius: {e}")
            return []
    
    async def get_token_metadata_pumpfun(self, session: aiohttp.ClientSession, mint: str) -> Optional[Dict]:
        try:
            url = f"https://frontend-api.pump.fun/coins/{mint}"
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://pump.fun/",
                "Origin": "https://pump.fun"
            }
            
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and isinstance(data, dict):
                        return data
                return None
        except:
            return None
    
    def check_liquidity_locked(self, metadata: Dict) -> bool:
        is_migrated = metadata.get('raydium_pool', False)
        lp_burn = metadata.get('lp_burned', False)
        complete = metadata.get('complete', False)
        
        liquidity = metadata.get('liquidity', 0)
        if liquidity > 5000:
            return True
        
        return is_migrated or lp_burn or complete
    
    async def send_discord_alert(self, metadata: Dict, mint: str):
        try:
            name = metadata.get('name', 'Unknown')
            symbol = metadata.get('symbol', '???')
            ca = mint
            description = metadata.get('description', 'No description provided')
            mcap = metadata.get('usd_market_cap', 0)
            liquidity = metadata.get('liquidity', 0)
            holder_count = metadata.get('holder_count', 0)
            volume_24h = metadata.get('volume_24h', 0)
            image_url = metadata.get('image_uri', '')
            
            creator = metadata.get('creator', 'Unknown')
            
            is_locked = self.check_liquidity_locked(metadata)
            
            embed = {
                "title": f"🎯 TOKEN ENDING IN 'PUMP' FOUND!",
                "description": f"**{name} (${symbol})**\n\n{description[:400] if description else 'No description'}",
                "color": 0xFF1493,
                "fields": [
                    {
                        "name": "💰 Market Cap",
                        "value": f"${mcap:,.2f}",
                        "inline": True
                    },
                    {
                        "name": "💧 Liquidity",
                        "value": f"${liquidity:,.2f}" + (" 🔒" if is_locked else " ⚠️ UNLOCKED"),
                        "inline": True
                    },
                    {
                        "name": "👥 Holders",
                        "value": str(holder_count),
                        "inline": True
                    },
                    {
                        "name": "📈 24h Volume",
                        "value": f"${volume_24h:,.2f}",
                        "inline": True
                    },
                    {
                        "name": "👤 Creator",
                        "value": f"`{creator[:8]}...{creator[-8:]}`" if len(creator) > 20 else f"`{creator}`",
                        "inline": True
                    },
                    {
                        "name": "📝 Contract Address (CA)",
                        "value": f"`{ca}`\n**Ends in: ...{ca[-4:].upper()} ✨**",
                        "inline": False
                    },
                    {
                        "name": "🔗 Links",
                        "value": f"[Pump.fun](https://pump.fun/{ca}) | [Solscan](https://solscan.io/token/{ca}) | [Birdeye](https://birdeye.so/token/{ca})",
                        "inline": False
                    }
                ],
                "thumbnail": {
                    "url": image_url if image_url else ""
                },
                "footer": {
                    "text": f"Detected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')} • CA ends in PUMP"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
            async with aiohttp.ClientSession() as session:
                webhook_data = {
                    "embeds": [embed],
                    "username": "PUMP Ending Scanner",
                    "avatar_url": "https://pump.fun/icon.png"
                }
                
                async with session.post(self.webhook_url, json=webhook_data) as response:
                    if response.status in [200, 204]:
                        print(f"✅ Alert sent for {symbol} (CA: ...{ca[-4:]})")
                    else:
                        print(f"❌ Failed to send alert: {response.status}")
        
        except Exception as e:
            print(f"Error sending Discord alert: {e}")
    
    async def scan_loop(self):
        print("🎯 Starting 'pump' ending scanner...")
        print("Looking for tokens with CA ending in 'pump'...")
        print("-" * 50)
        
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    transactions = await self.fetch_recent_pump_tokens(session)
                    print(f"📥 Scanning {len(transactions)} transactions for 'pump' endings...")
                    
                    found_count = 0
                    
                    for tx in transactions:
                        try:
                            token_transfers = tx.get('tokenTransfers', [])
                            
                            for transfer in token_transfers:
                                mint = transfer.get('mint', '')
                                
                                if not mint or mint == 'So11111111111111111111111111111111111111112':
                                    continue
                                
                                if mint in self.seen_tokens:
                                    continue
                                
                                mint_lower = mint.lower()
                                
                                if mint_lower.endswith('pump'):
                                    print(f"🎯 PUMP ENDING FOUND: {mint}")
                                    
                                    self.seen_tokens.add(mint)
                                    found_count += 1
                                    
                                    metadata = await self.get_token_metadata_pumpfun(session, mint)
                                    
                                    if metadata:
                                        symbol = metadata.get('symbol', '???')
                                        liquidity = metadata.get('liquidity', 0)
                                        print(f"✅ Sending alert for {symbol} (Liq: ${liquidity:.0f})")
                                        await self.send_discord_alert(metadata, mint)
                                    else:
                                        print(f"⚠️  Metadata not available yet for {mint[:8]}...")
                                    
                                    await asyncio.sleep(1)
                            
                        except Exception as e:
                            print(f"Error processing transaction: {e}")
                            continue
                    
                    if found_count > 0:
                        print(f"🎉 Found {found_count} tokens ending in 'pump' this scan!")
                    else:
                        print("⏭️  No 'pump' endings found this scan")
                    
                    if len(self.seen_tokens) > 2000:
                        self.seen_tokens = set(list(self.seen_tokens)[-1000:])
                    
                    await asyncio.sleep(30)
                    
                except Exception as e:
                    print(f"❌ Error in scan loop: {e}")
                    await asyncio.sleep(60)

async def main():
    DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
    HELIUS_API_KEY = os.getenv('HELIUS_API_KEY')
    
    if not DISCORD_WEBHOOK_URL:
        print("❌ ERROR: DISCORD_WEBHOOK_URL not found in environment!")
        print("\n📝 Please create a .env file with:")
        print("DISCORD_WEBHOOK_URL=your_webhook_url_here")
        print("HELIUS_API_KEY=your_helius_api_key_here")
        return
    
    if not HELIUS_API_KEY:
        print("❌ ERROR: HELIUS_API_KEY not found in environment!")
        print("\n📝 Get a free Helius API key at: https://www.helius.dev/")
        print("Then add it to your .env file:")
        print("HELIUS_API_KEY=your_helius_api_key_here")
        return
    
    scanner = PumpEndingScannerHelius(DISCORD_WEBHOOK_URL, HELIUS_API_KEY)
    
    await scanner.scan_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Scanner stopped by user")