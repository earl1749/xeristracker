import sys
import traceback

print("🔄 XerisTracker starting...", flush=True)

try:
    import asyncio
    print("✅ asyncio OK", flush=True)

    import httpx
    print("✅ httpx OK", flush=True)

    import websockets
    print("✅ websockets OK", flush=True)

    import matplotlib
    print("✅ matplotlib OK", flush=True)

    print("🔄 Loading config...", flush=True)
    from config.settings import MINT, DISCORD_CHANNEL
    print(f"✅ Config loaded — MINT={MINT[:8]}...  CH={DISCORD_CHANNEL}", flush=True)

    print("🔄 Loading registry...", flush=True)
    from config.data_registy import EXCHANGE_REGISTRY
    print(f"✅ Registry loaded — {len(EXCHANGE_REGISTRY)} programs", flush=True)

    print("🔄 Loading xeris...", flush=True)
    from xeris import main
    print("✅ xeris loaded", flush=True)

except ImportError as e:
    print(f"\n❌ IMPORT ERROR: {e}", flush=True)
    print("   → A required package is missing. Check requirements.txt", flush=True)
    traceback.print_exc()
    sys.exit(1)

except ValueError as e:
    print(f"\n❌ CONFIG ERROR: {e}", flush=True)
    print("   → A required environment variable is missing.", flush=True)
    print("   → Go to Railway → your service → Variables and add it.", flush=True)
    sys.exit(1)

except Exception as e:
    print(f"\n❌ STARTUP ERROR: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
    print("\n🚀 All systems go — launching bot...\n", flush=True)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped")
    except Exception as e:
        print(f"❌ Fatal runtime error: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)
