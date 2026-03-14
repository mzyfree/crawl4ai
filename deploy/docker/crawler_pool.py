# crawler_pool.py - Smart browser pool with tiered management
import asyncio, gc, json, hashlib, time, os
import psutil
from contextlib import suppress
from typing import Dict, Optional
from crawl4ai import AsyncWebCrawler, BrowserConfig
from utils import load_config, get_container_memory_percent
import logging

logger = logging.getLogger(__name__)
CONFIG = load_config()

# Pool tiers
PERMANENT: Optional[AsyncWebCrawler] = None  # Always-ready default browser
HOT_POOL: Dict[str, AsyncWebCrawler] = {}    # Frequent configs
COLD_POOL: Dict[str, AsyncWebCrawler] = {}   # Rare configs
RETIRED_POOL: Dict[str, AsyncWebCrawler] = {} # Browsers marked for retirement
LAST_USED: Dict[str, float] = {}
LAST_IDLE: Dict[str, float] = {}  # Last time ref_count was 0
USAGE_COUNT: Dict[str, int] = {}
LOCK = asyncio.Lock()

# Config
MEM_LIMIT = CONFIG.get("crawler", {}).get("memory_threshold_percent", 95.0)
BASE_IDLE_TTL = CONFIG.get("crawler", {}).get("pool", {}).get("idle_ttl_sec", 300)
LEAK_DETECTION_THRESHOLD = int(os.getenv("CRAWL4AI_LEAK_DETECTION_THRESHOLD_SEC", "14400")) # 4 hours
RETIRED_FORCE_CLOSE_SEC = int(os.getenv("CRAWL4AI_RETIRED_FORCE_CLOSE_SEC", "300"))
DEFAULT_CONFIG_SIG = None  # Cached sig for default config

# Retirement Config (from env)
RETIREMENT_ENABLED = os.getenv("CRAWL4AI_BROWSER_RETIREMENT_ENABLED", "false").lower() == "true"
POOL_AUDIT_ENABLED = os.getenv("CRAWL4AI_POOL_AUDIT_ENABLED", "false").lower() == "true"
PERMANENT_BROWSER_DISABLED = os.getenv("CRAWL4AI_PERMANENT_BROWSER_DISABLED", "false").lower() == "true"

MAX_USAGE_COUNT = int(os.getenv("CRAWL4AI_BROWSER_MAX_USAGE", "100"))
MEMORY_RETIRE_THRESHOLD = int(os.getenv("CRAWL4AI_MEMORY_RETIRE_THRESHOLD", "75"))
MEMORY_RETIRE_MIN_USAGE = int(os.getenv("CRAWL4AI_MEMORY_RETIRE_MIN_USAGE", "10"))
PDF_TEMP_CLEANUP_HOURS = int(os.getenv("CRAWL4AI_PDF_TEMP_CLEANUP_HOURS", "1"))

# Global Stats for Audit
PDF_TOTAL_PROCESSED = 0
PDF_TOTAL_TRUNCATED = 0

# Pool lifecycle observability switches
POOL_LIFECYCLE_DEBUG = os.getenv("CRAWL4AI_POOL_LIFECYCLE_DEBUG", "false").lower() == "true"
try:
    POOL_LIFECYCLE_LOG_INTERVAL = max(
        1, int(os.getenv("CRAWL4AI_POOL_LIFECYCLE_LOG_INTERVAL", "200"))
    )
except ValueError:
    POOL_LIFECYCLE_LOG_INTERVAL = 200

POOL_TOTAL_ACQUIRED = 0
POOL_TOTAL_RELEASED = 0
POOL_ACTIVE_BORROWS = 0
POOL_SEQ = 0
POOL_CRAWLER_ACTIVE: Dict[int, int] = {}
JANITOR_LAST_TICK = 0.0
JANITOR_LOOP_COUNT = 0
JANITOR_LAST_ACTION = "never"
JANITOR_LAST_ERROR = ""
JANITOR_RUNNING = False


def get_janitor_state() -> Dict[str, object]:
    """Return janitor heartbeat and debug state for monitor endpoints."""
    return {
        "running": JANITOR_RUNNING,
        "last_tick": JANITOR_LAST_TICK,
        "loop_count": JANITOR_LOOP_COUNT,
        "last_action": JANITOR_LAST_ACTION,
        "last_error": JANITOR_LAST_ERROR,
    }


def _pool_lifecycle_log(event: str, sig: str, crawler: Optional[AsyncWebCrawler] = None):
    """Emit lightweight lifecycle logs for crawler borrow/release tracking."""
    global POOL_TOTAL_ACQUIRED, POOL_TOTAL_RELEASED, POOL_ACTIVE_BORROWS
    crawler_id = id(crawler) if crawler else None
    crawler_active = POOL_CRAWLER_ACTIVE.get(crawler_id, 0) if crawler_id else 0

    if POOL_LIFECYCLE_DEBUG:
        logger.info(
            "🔎 PoolLifecycle %s sig=%s active_borrows=%s crawler_id=%s crawler_active=%s totals(acq=%s, rel=%s)",
            event,
            sig[:8] if sig else "unknown",
            POOL_ACTIVE_BORROWS,
            crawler_id,
            crawler_active,
            POOL_TOTAL_ACQUIRED,
            POOL_TOTAL_RELEASED,
        )
        return

    total_events = POOL_TOTAL_ACQUIRED + POOL_TOTAL_RELEASED
    if total_events > 0 and total_events % POOL_LIFECYCLE_LOG_INTERVAL == 0:
        logger.info(
            "🔎 PoolLifecycle snapshot active_borrows=%s totals(acq=%s, rel=%s)",
            POOL_ACTIVE_BORROWS,
            POOL_TOTAL_ACQUIRED,
            POOL_TOTAL_RELEASED,
        )


def _sig(cfg: BrowserConfig) -> str:
    """Generate config signature."""
    payload = json.dumps(cfg.to_dict(), sort_keys=True, separators=(",",":"))
    return hashlib.sha1(payload.encode()).hexdigest()

def _is_default_config(sig: str) -> bool:
    """Check if config matches default."""
    return sig == DEFAULT_CONFIG_SIG

async def get_crawler(cfg: BrowserConfig) -> AsyncWebCrawler:
    """Get crawler from pool with tiered strategy."""
    global POOL_TOTAL_ACQUIRED, POOL_ACTIVE_BORROWS, POOL_SEQ
    sig = _sig(cfg)
    async with LOCK:
        # Use permanent browser if not disabled and config matches
        if not PERMANENT_BROWSER_DISABLED and PERMANENT and _is_default_config(sig):
            LAST_USED[sig] = time.time()
            USAGE_COUNT[sig] = USAGE_COUNT.get(sig, 0) + 1
            POOL_TOTAL_ACQUIRED += 1
            POOL_ACTIVE_BORROWS += 1
            POOL_SEQ += 1
            crawler_id = id(PERMANENT)
            POOL_CRAWLER_ACTIVE[crawler_id] = POOL_CRAWLER_ACTIVE.get(crawler_id, 0) + 1
            _pool_lifecycle_log(f"acquire#{POOL_SEQ}", sig, PERMANENT)
            logger.info("🔥 Using permanent browser")
            return PERMANENT

        # Check hot pool
        if sig in HOT_POOL:
            crawler = HOT_POOL[sig]
            usage = USAGE_COUNT.get(sig, 0)
            
            if not hasattr(crawler, 'active_requests'):
                crawler.active_requests = 0

            should_retire = False
            if RETIREMENT_ENABLED:
                if usage >= MAX_USAGE_COUNT:
                    should_retire = True
                    logger.info(f"👴 Retirement time for browser {sig[:8]}: Max usage reached ({usage})")
                elif usage >= MEMORY_RETIRE_MIN_USAGE:
                    try:
                        mem_percent = psutil.virtual_memory().percent
                        if mem_percent > MEMORY_RETIRE_THRESHOLD:
                            should_retire = True
                            logger.info(f"👴 Retirement time for browser {sig[:8]}: Memory high ({mem_percent}%)")
                    except Exception as e:
                        logger.warning(f"Failed to check memory for retirement: {e}")

            if should_retire:
                RETIRED_POOL[sig] = HOT_POOL.pop(sig)
            else:
                LAST_USED[sig] = time.time()
                USAGE_COUNT[sig] = usage + 1
                crawler.active_requests += 1
                # If it was idle before, now it's busy
                if crawler.active_requests == 1:
                    LAST_IDLE.pop(sig, None)
                POOL_TOTAL_ACQUIRED += 1
                POOL_ACTIVE_BORROWS += 1
                POOL_SEQ += 1
                crawler_id = id(crawler)
                POOL_CRAWLER_ACTIVE[crawler_id] = POOL_CRAWLER_ACTIVE.get(crawler_id, 0) + 1
                _pool_lifecycle_log(f"acquire#{POOL_SEQ}", sig, crawler)
                logger.info(f"♨️  Using hot pool browser (sig={sig[:8]}, usage={USAGE_COUNT[sig]}, active={crawler.active_requests})")
                return crawler

        # Check cold pool
        if sig in COLD_POOL:
            LAST_USED[sig] = time.time()
            USAGE_COUNT[sig] = USAGE_COUNT.get(sig, 0) + 1

            if USAGE_COUNT[sig] >= 3:
                logger.info(f"⬆️  Promoting to hot pool (sig={sig[:8]}, count={USAGE_COUNT[sig]})")
                HOT_POOL[sig] = COLD_POOL.pop(sig)
                crawler = HOT_POOL[sig]
                if not hasattr(crawler, 'active_requests'):
                    crawler.active_requests = 0
                crawler.active_requests += 1
                if crawler.active_requests == 1:
                    LAST_IDLE.pop(sig, None)
                POOL_TOTAL_ACQUIRED += 1
                POOL_ACTIVE_BORROWS += 1
                POOL_SEQ += 1
                crawler_id = id(crawler)
                POOL_CRAWLER_ACTIVE[crawler_id] = POOL_CRAWLER_ACTIVE.get(crawler_id, 0) + 1
                _pool_lifecycle_log(f"acquire#{POOL_SEQ}", sig, crawler)
                return crawler

            logger.info(f"❄️  Using cold pool browser (sig={sig[:8]})")
            crawler = COLD_POOL[sig]
            if not hasattr(crawler, 'active_requests'):
                crawler.active_requests = 0
            crawler.active_requests += 1
            if crawler.active_requests == 1:
                LAST_IDLE.pop(sig, None)
            POOL_TOTAL_ACQUIRED += 1
            POOL_ACTIVE_BORROWS += 1
            POOL_SEQ += 1
            crawler_id = id(crawler)
            POOL_CRAWLER_ACTIVE[crawler_id] = POOL_CRAWLER_ACTIVE.get(crawler_id, 0) + 1
            _pool_lifecycle_log(f"acquire#{POOL_SEQ}", sig, crawler)
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
        crawler.active_requests = 1
        COLD_POOL[sig] = crawler
        LAST_USED[sig] = time.time()
        USAGE_COUNT[sig] = 1
        POOL_TOTAL_ACQUIRED += 1
        POOL_ACTIVE_BORROWS += 1
        POOL_SEQ += 1
        crawler_id = id(crawler)
        POOL_CRAWLER_ACTIVE[crawler_id] = POOL_CRAWLER_ACTIVE.get(crawler_id, 0) + 1
        _pool_lifecycle_log(f"acquire#{POOL_SEQ}", sig, crawler)
        return crawler

async def release_crawler(crawler: AsyncWebCrawler):
    """Decrement active request count for a crawler."""
    global POOL_TOTAL_RELEASED, POOL_ACTIVE_BORROWS
    async with LOCK:
        pool_sig = None
        crawler_id = id(crawler)
        for pool in (HOT_POOL, COLD_POOL, RETIRED_POOL):
            for s, c in pool.items():
                if c == crawler:
                    pool_sig = s
                    break
            if pool_sig:
                break

        if hasattr(crawler, 'active_requests'):
            crawler.active_requests -= 1
            if crawler.active_requests < 0:
                crawler.active_requests = 0
            
            # If active requests dropped to 0, record idle time
            if crawler.active_requests == 0:
                # Find sig to update LAST_IDLE
                # Since we don't pass sig, we iterate pools. Not efficient but works for now.
                # Optimization: release_crawler should ideally accept sig.
                if pool_sig:
                    LAST_IDLE[pool_sig] = time.time()

            # Lifecycle accounting (always attempt to keep counters consistent)
            POOL_TOTAL_RELEASED += 1
            POOL_ACTIVE_BORROWS = max(0, POOL_ACTIVE_BORROWS - 1)
            current = POOL_CRAWLER_ACTIVE.get(crawler_id, 0)
            if current <= 1:
                POOL_CRAWLER_ACTIVE.pop(crawler_id, None)
            else:
                POOL_CRAWLER_ACTIVE[crawler_id] = current - 1
            _pool_lifecycle_log("release", pool_sig or "unknown", crawler)

async def init_permanent(cfg: BrowserConfig):
    """Initialize permanent default browser."""
    global PERMANENT, DEFAULT_CONFIG_SIG
    
    # Log retirement status once on startup
    if RETIREMENT_ENABLED:
        logger.info(f"✅ Browser retirement enabled (Max Usage: {MAX_USAGE_COUNT}, Mem Threshold: {MEMORY_RETIRE_THRESHOLD}%)")
    else:
        logger.info("ℹ️ Browser retirement disabled")

    async with LOCK:
        DEFAULT_CONFIG_SIG = _sig(cfg)
        if PERMANENT_BROWSER_DISABLED:
            logger.info("ℹ️ Permanent browser is DISABLED via config")
            return

        logger.info("🔥 Creating permanent default browser")
        PERMANENT = AsyncWebCrawler(config=cfg, thread_safe=False)
        await PERMANENT.start()
        LAST_USED[DEFAULT_CONFIG_SIG] = time.time()
        USAGE_COUNT[DEFAULT_CONFIG_SIG] = 0

async def close_all():
    """Close all browsers."""
    async with LOCK:
        tasks = []
        tasks.extend([c.close() for c in HOT_POOL.values()])
        tasks.extend([c.close() for c in COLD_POOL.values()])
        tasks.extend([c.close() for c in RETIRED_POOL.values()]) 
        await asyncio.gather(*tasks, return_exceptions=True)
        HOT_POOL.clear()
        COLD_POOL.clear()
        RETIRED_POOL.clear()
        LAST_USED.clear()
        USAGE_COUNT.clear()
        LAST_IDLE.clear()

async def janitor():
    """Adaptive cleanup based on memory pressure."""
    global JANITOR_LAST_TICK, JANITOR_LOOP_COUNT, JANITOR_LAST_ACTION, JANITOR_LAST_ERROR, JANITOR_RUNNING
    last_audit_time = 0
    last_cleanup_time = 0
    JANITOR_RUNNING = True
    JANITOR_LAST_ACTION = "started"
    while True:
        try:
            mem_pct = get_container_memory_percent()

            if mem_pct > 80:
                interval, cold_ttl, hot_ttl = 10, 30, 60
            elif mem_pct > 60:
                interval, cold_ttl, hot_ttl = 30, 60, 120
            else:
                interval, cold_ttl, hot_ttl = 60, BASE_IDLE_TTL, BASE_IDLE_TTL

            await asyncio.sleep(interval)

            now = time.time()
            JANITOR_LAST_TICK = now
            JANITOR_LOOP_COUNT += 1
            JANITOR_LAST_ACTION = f"scan mem={mem_pct:.1f}% hot={len(HOT_POOL)} cold={len(COLD_POOL)} retired={len(RETIRED_POOL)}"
            JANITOR_LAST_ERROR = ""
            async with LOCK:
                if POOL_AUDIT_ENABLED and now - last_audit_time >= 300:
                    def _pool_info(pool):
                        res = []
                        for sig, crawler in pool.items():
                            req = getattr(crawler, 'active_requests', 0)
                            usage = USAGE_COUNT.get(sig, 0)
                            idle_status = ""
                            if req > 0:
                                last_idle = LAST_IDLE.get(sig, now)
                                idle_status = f", busy={now - last_idle:.0f}s"
                            res.append(f"{sig[:8]}(req={req}, usage={usage}{idle_status})")
                        return res

                    logger.info(
                        f"🧐 [Pool Audit]\n"
                        f"  - PERMANENT: {'Active' if PERMANENT else 'None/Disabled'}\n"
                        f"  - HOT_POOL: {len(HOT_POOL)} {_pool_info(HOT_POOL)}\n"
                        f"  - COLD_POOL: {len(COLD_POOL)} {_pool_info(COLD_POOL)}\n"
                        f"  - RETIRED_POOL: {len(RETIRED_POOL)} {_pool_info(RETIRED_POOL)}\n"
                        f"  - PDF Stats: processed={PDF_TOTAL_PROCESSED}, truncated={PDF_TOTAL_TRUNCATED}\n"
                        f"  - System Memory: {mem_pct:.1f}%"
                    )
                    last_audit_time = now

                for pool_name, pool in [("HOT", HOT_POOL), ("COLD", COLD_POOL), ("RETIRED", RETIRED_POOL)]:
                    for sig in list(pool.keys()):
                        crawler = pool[sig]
                        if getattr(crawler, 'active_requests', 0) > 0:
                            start_busy_time = LAST_IDLE.get(sig, LAST_USED.get(sig, now))
                            busy_duration = now - start_busy_time
                            if busy_duration > LEAK_DETECTION_THRESHOLD:
                                logger.error(
                                    f"🚨 [Watchdog] LEAK DETECTED! Browser {sig[:8]} in {pool_name} busy for {busy_duration:.0f}s (> {LEAK_DETECTION_THRESHOLD}s). FORCE KILLING."
                                )
                                JANITOR_LAST_ACTION = f"watchdog_force_close {pool_name.lower()} sig={sig[:8]}"
                                with suppress(Exception):
                                    await crawler.close()
                                pool.pop(sig, None)
                                LAST_USED.pop(sig, None)
                                USAGE_COUNT.pop(sig, None)
                                LAST_IDLE.pop(sig, None)

                if now - last_cleanup_time >= 1800:
                    try:
                        import glob
                        tmp_files = glob.glob("/tmp/tmp*.pdf")
                        cleaned_count = 0
                        cutoff = now - (PDF_TEMP_CLEANUP_HOURS * 3600)
                        for path in tmp_files:
                            if os.path.getmtime(path) < cutoff:
                                os.remove(path)
                                cleaned_count += 1
                        if cleaned_count > 0:
                            logger.info(
                                f"🧹 Janitor: Cleaned {cleaned_count} stale PDF temp files (older than {PDF_TEMP_CLEANUP_HOURS}h)"
                            )
                            JANITOR_LAST_ACTION = f"cleanup_tmp cleaned={cleaned_count}"
                    except Exception as e:
                        logger.warning(f"Failed to cleanup stale PDF files: {e}")
                    last_cleanup_time = now

                for sig in list(COLD_POOL.keys()):
                    if now - LAST_USED.get(sig, now) > cold_ttl:
                        crawler = COLD_POOL[sig]
                        if not hasattr(crawler, 'active_requests') or crawler.active_requests == 0:
                            logger.info(f"🧹 Closing cold browser (idle, sig={sig[:8]})")
                            JANITOR_LAST_ACTION = f"close_cold sig={sig[:8]}"
                            with suppress(Exception):
                                await crawler.close()
                            COLD_POOL.pop(sig, None)
                            LAST_USED.pop(sig, None)
                            USAGE_COUNT.pop(sig, None)
                            LAST_IDLE.pop(sig, None)

                for sig in list(HOT_POOL.keys()):
                    if now - LAST_USED.get(sig, now) > hot_ttl:
                        crawler = HOT_POOL[sig]
                        if not hasattr(crawler, 'active_requests') or crawler.active_requests == 0:
                            logger.info(f"🧹 Closing hot browser (idle={now - LAST_USED[sig]:.0f}s, sig={sig[:8]})")
                            JANITOR_LAST_ACTION = f"close_hot sig={sig[:8]}"
                            with suppress(Exception):
                                await crawler.close()
                            HOT_POOL.pop(sig, None)
                            LAST_USED.pop(sig, None)
                            USAGE_COUNT.pop(sig, None)
                            LAST_IDLE.pop(sig, None)

                for sig in list(RETIRED_POOL.keys()):
                    crawler = RETIRED_POOL[sig]
                    last_used = LAST_USED.get(sig, now)
                    retired_age = now - last_used
                    is_idle = hasattr(crawler, 'active_requests') and crawler.active_requests == 0
                    force_stuck = retired_age > RETIRED_FORCE_CLOSE_SEC
                    if is_idle or force_stuck:
                        reason = "idle" if is_idle else f"stuck {retired_age:.0f}s"
                        logger.info(f"💀 Janitor closing retired browser (sig={sig[:8]}, {reason})")
                        JANITOR_LAST_ACTION = f"close_retired sig={sig[:8]} reason={reason}"
                        with suppress(Exception):
                            await crawler.close()
                        RETIRED_POOL.pop(sig, None)
                        # Keep shared per-sig metadata if a live hot/cold browser with the
                        # same signature still exists; otherwise hot idle TTL can no longer
                        # advance and the browser may never be collected.
                        if sig not in HOT_POOL and sig not in COLD_POOL:
                            LAST_USED.pop(sig, None)
                            USAGE_COUNT.pop(sig, None)
                            LAST_IDLE.pop(sig, None)
                        gc.collect()

                if mem_pct > 60 or len(RETIRED_POOL) > 0:
                    logger.info(f"📊 Pool: hot={len(HOT_POOL)}, cold={len(COLD_POOL)}, retired={len(RETIRED_POOL)}, mem={mem_pct:.1f}%")
        except asyncio.CancelledError:
            JANITOR_RUNNING = False
            JANITOR_LAST_ACTION = "cancelled"
            raise
        except Exception as e:
            JANITOR_LAST_TICK = time.time()
            JANITOR_LOOP_COUNT += 1
            JANITOR_LAST_ACTION = "error"
            JANITOR_LAST_ERROR = repr(e)
            logger.error(f"Janitor loop error: {e}", exc_info=True)
            await asyncio.sleep(1)
