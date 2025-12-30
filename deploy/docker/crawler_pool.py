# crawler_pool.py - Smart browser pool with tiered management
import asyncio, json, hashlib, time
from contextlib import suppress
from typing import Dict, Optional, Set
from crawl4ai import AsyncWebCrawler, BrowserConfig
from utils import load_config, get_container_memory_percent
import logging

logger = logging.getLogger(__name__)
CONFIG = load_config()

# Pool tiers
PERMANENT: Optional[AsyncWebCrawler] = None  # Always-ready default browser
HOT_POOL: Dict[str, AsyncWebCrawler] = {}    # Frequent configs
COLD_POOL: Dict[str, AsyncWebCrawler] = {}   # Rare configs
RETIRED_POOL: Set[AsyncWebCrawler] = set()   # Dying instances waiting for cleanup
LAST_USED: Dict[str, float] = {}
USAGE_COUNT: Dict[str, int] = {}
LOCK = asyncio.Lock()

# Config
MEM_LIMIT = CONFIG.get("crawler", {}).get("memory_threshold_percent", 95.0)
BASE_IDLE_TTL = CONFIG.get("crawler", {}).get("pool", {}).get("idle_ttl_sec", 300)
MAX_USAGE_COUNT = 100  # Max requests before retirement
DEFAULT_CONFIG_SIG = None  # Cached sig for default config

def _sig(cfg: BrowserConfig) -> str:
    """Generate config signature."""
    payload = json.dumps(cfg.to_dict(), sort_keys=True, separators=(",",":"))
    return hashlib.sha1(payload.encode()).hexdigest()

def _is_default_config(sig: str) -> bool:
    """Check if config matches default."""
    return sig == DEFAULT_CONFIG_SIG

async def get_crawler(cfg: BrowserConfig) -> AsyncWebCrawler:
    """Get crawler from pool with tiered strategy."""
    sig = _sig(cfg)
    async with LOCK:
        # Check permanent browser for default config
        if PERMANENT and _is_default_config(sig):
            LAST_USED[sig] = time.time()
            USAGE_COUNT[sig] = USAGE_COUNT.get(sig, 0) + 1
            if not hasattr(PERMANENT, 'active_requests'):
                PERMANENT.active_requests = 0
            PERMANENT.active_requests += 1
            logger.info("🔥 Using permanent browser")
            return PERMANENT

        # Check hot pool
        if sig in HOT_POOL:
            # Check retirement age
            current_usage = USAGE_COUNT.get(sig, 0)
            if current_usage >= MAX_USAGE_COUNT:
                logger.info(f"👴 Retirement time for browser (sig={sig[:8]}, usage={current_usage})")
                old_crawler = HOT_POOL.pop(sig)
                RETIRED_POOL.add(old_crawler)
                USAGE_COUNT.pop(sig, None)
                LAST_USED.pop(sig, None)
                # Fall through to create new one
            else:
                LAST_USED[sig] = time.time()
                USAGE_COUNT[sig] = current_usage + 1
                crawler = HOT_POOL[sig]
                if not hasattr(crawler, 'active_requests'):
                    crawler.active_requests = 0
                crawler.active_requests += 1
                logger.info(f"♨️  Using hot pool browser (sig={sig[:8]})")
                return crawler

        # Check cold pool (promote to hot if used 3+ times)
        if sig in COLD_POOL:
            # Check retirement age for cold pool too
            current_usage = USAGE_COUNT.get(sig, 0)
            if current_usage >= MAX_USAGE_COUNT:
                logger.info(f"👴 Retirement time for cold browser (sig={sig[:8]}, usage={current_usage})")
                old_crawler = COLD_POOL.pop(sig)
                RETIRED_POOL.add(old_crawler)
                USAGE_COUNT.pop(sig, None)
                LAST_USED.pop(sig, None)
                # Fall through to create new one
            else:
                LAST_USED[sig] = time.time()
                USAGE_COUNT[sig] = current_usage + 1
                crawler = COLD_POOL[sig]
                
                if USAGE_COUNT[sig] >= 3:
                    logger.info(f"⬆️  Promoting to hot pool (sig={sig[:8]}, count={USAGE_COUNT[sig]})")
                    del COLD_POOL[sig] # Remove from cold
                    HOT_POOL[sig] = crawler # Add to hot
                    
                    # Track promotion in monitor
                    try:
                        from monitor import get_monitor
                        await get_monitor().track_janitor_event("promote", sig, {"count": USAGE_COUNT[sig]})
                    except:
                        pass
                else:
                    logger.info(f"❄️  Using cold pool browser (sig={sig[:8]})")

                if not hasattr(crawler, 'active_requests'):
                    crawler.active_requests = 0
                crawler.active_requests += 1
                return crawler

        # Memory check before creating new
        mem_pct = get_container_memory_percent()
        if mem_pct >= MEM_LIMIT:
            logger.error(f"💥 Memory pressure: {mem_pct:.1f}% >= {MEM_LIMIT}%")
            raise MemoryError(f"Memory at {mem_pct:.1f}%, refusing new browser")

        # Create new in cold pool
        logger.info(f"🆕 Creating new browser in cold pool (sig={sig[:8]}, mem={mem_pct:.1f}%)")
        crawler = AsyncWebCrawler(config=cfg, thread_safe=False)
        await crawler.start()
        # Initialize custom attribute
        crawler.active_requests = 1 
        
        COLD_POOL[sig] = crawler
        LAST_USED[sig] = time.time()
        USAGE_COUNT[sig] = 1
        return crawler

async def release_crawler(crawler: AsyncWebCrawler):
    """Release crawler back to pool (decrement active requests)."""
    async with LOCK:
        if hasattr(crawler, 'active_requests'):
            crawler.active_requests = max(0, crawler.active_requests - 1)

async def init_permanent(cfg: BrowserConfig):
    """Initialize permanent default browser."""
    global PERMANENT, DEFAULT_CONFIG_SIG
    async with LOCK:
        if PERMANENT:
            return
        DEFAULT_CONFIG_SIG = _sig(cfg)
        logger.info("🔥 Creating permanent default browser")
        PERMANENT = AsyncWebCrawler(config=cfg, thread_safe=False)
        await PERMANENT.start()
        PERMANENT.active_requests = 0
        LAST_USED[DEFAULT_CONFIG_SIG] = time.time()
        USAGE_COUNT[DEFAULT_CONFIG_SIG] = 0

async def close_all():
    """Close all browsers."""
    async with LOCK:
        tasks = []
        if PERMANENT:
            tasks.append(PERMANENT.close())
        tasks.extend([c.close() for c in HOT_POOL.values()])
        tasks.extend([c.close() for c in COLD_POOL.values()])
        tasks.extend([c.close() for c in RETIRED_POOL])
        await asyncio.gather(*tasks, return_exceptions=True)
        HOT_POOL.clear()
        COLD_POOL.clear()
        RETIRED_POOL.clear()
        LAST_USED.clear()
        USAGE_COUNT.clear()

async def janitor():
    """Adaptive cleanup based on memory pressure."""
    while True:
        mem_pct = get_container_memory_percent()

        # Adaptive intervals and TTLs
        if mem_pct > 80:
            interval, cold_ttl, hot_ttl = 10, 30, 120
        elif mem_pct > 60:
            interval, cold_ttl, hot_ttl = 30, 60, 300
        else:
            interval, cold_ttl, hot_ttl = 60, BASE_IDLE_TTL, BASE_IDLE_TTL * 2

        await asyncio.sleep(interval)

        now = time.time()
        async with LOCK:
            # 1. Cleanup Retired/Dying Pool
            if RETIRED_POOL:
                logger.info(f"💀 Janitor checking {len(RETIRED_POOL)} retired browsers")
                to_remove = []
                for crawler in RETIRED_POOL:
                    active = getattr(crawler, 'active_requests', 0)
                    if active == 0:
                        logger.info("⚰️  Burying retired browser (active_requests=0)")
                        with suppress(Exception):
                            await crawler.close()
                        to_remove.append(crawler)
                    else:
                        logger.info(f"⏳ Retired browser still active (requests={active})")
                
                for c in to_remove:
                    RETIRED_POOL.remove(c)

            # 2. Clean cold pool
            for sig in list(COLD_POOL.keys()):
                crawler = COLD_POOL[sig]
                active = getattr(crawler, 'active_requests', 0)
                # Only close if idle AND active_requests == 0
                if active == 0 and now - LAST_USED.get(sig, now) > cold_ttl:
                    idle_time = now - LAST_USED[sig]
                    logger.info(f"🧹 Closing cold browser (sig={sig[:8]}, idle={idle_time:.0f}s)")
                    with suppress(Exception):
                        await crawler.close()
                    COLD_POOL.pop(sig, None)
                    LAST_USED.pop(sig, None)
                    USAGE_COUNT.pop(sig, None)

                    # Track in monitor
                    try:
                        from monitor import get_monitor
                        await get_monitor().track_janitor_event("close_cold", sig, {"idle_seconds": int(idle_time), "ttl": cold_ttl})
                    except:
                        pass

            # 3. Clean hot pool (more conservative)
            for sig in list(HOT_POOL.keys()):
                crawler = HOT_POOL[sig]
                active = getattr(crawler, 'active_requests', 0)
                # Only close if idle AND active_requests == 0
                if active == 0 and now - LAST_USED.get(sig, now) > hot_ttl:
                    idle_time = now - LAST_USED[sig]
                    logger.info(f"🧹 Closing hot browser (sig={sig[:8]}, idle={idle_time:.0f}s)")
                    with suppress(Exception):
                        await crawler.close()
                    HOT_POOL.pop(sig, None)
                    LAST_USED.pop(sig, None)
                    USAGE_COUNT.pop(sig, None)

                    # Track in monitor
                    try:
                        from monitor import get_monitor
                        await get_monitor().track_janitor_event("close_hot", sig, {"idle_seconds": int(idle_time), "ttl": hot_ttl})
                    except:
                        pass

            # Log pool stats
            if mem_pct > 60 or len(RETIRED_POOL) > 0:
                logger.info(f"📊 Pool: hot={len(HOT_POOL)}, cold={len(COLD_POOL)}, retired={len(RETIRED_POOL)}, mem={mem_pct:.1f}%")
