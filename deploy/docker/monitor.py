# monitor.py - Real-time monitoring stats with Redis persistence
import time
import json
import asyncio
from typing import Dict, List, Optional
from datetime import datetime, timezone
from collections import deque
from pathlib import Path
from redis import asyncio as aioredis
from utils import get_container_memory_percent
import psutil
import logging

logger = logging.getLogger(__name__)


def _read_cgroup_text(path: str) -> Optional[str]:
    try:
        p = Path(path)
        if p.exists():
            return p.read_text().strip()
    except Exception:
        return None
    return None


def _read_cgroup_memory_snapshot() -> Dict:
    """Return raw cgroup memory usage/limits and a compact memory.stat view."""
    usage_txt = _read_cgroup_text("/sys/fs/cgroup/memory.current")
    limit_txt = _read_cgroup_text("/sys/fs/cgroup/memory.max")

    if usage_txt is None:
        usage_txt = _read_cgroup_text("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        limit_txt = _read_cgroup_text("/sys/fs/cgroup/memory/memory.limit_in_bytes")

    usage_bytes = int(usage_txt) if usage_txt and usage_txt.isdigit() else None
    if limit_txt == "max":
        limit_bytes = None
    elif limit_txt and limit_txt.isdigit():
        limit_bytes = int(limit_txt)
    else:
        limit_bytes = None

    stat_txt = _read_cgroup_text("/sys/fs/cgroup/memory.stat")
    if stat_txt is None:
        stat_txt = _read_cgroup_text("/sys/fs/cgroup/memory/memory.stat")

    memory_stat = {}
    if stat_txt:
        for line in stat_txt.splitlines():
            try:
                key, value = line.split()
                memory_stat[key] = int(value)
            except ValueError:
                continue

    interesting_keys = [
        "anon",
        "file",
        "kernel",
        "kernel_stack",
        "pagetables",
        "slab",
        "sock",
        "shmem",
        "inactive_anon",
        "active_anon",
        "inactive_file",
        "active_file",
    ]

    return {
        "usage_bytes": usage_bytes,
        "usage_mb": round(usage_bytes / (1024 ** 2), 2) if usage_bytes is not None else None,
        "limit_bytes": limit_bytes,
        "limit_mb": round(limit_bytes / (1024 ** 2), 2) if limit_bytes else None,
        "stat_bytes": {k: memory_stat.get(k) for k in interesting_keys if k in memory_stat},
        "stat_mb": {
            k: round(memory_stat[k] / (1024 ** 2), 2)
            for k in interesting_keys
            if k in memory_stat
        },
    }


def _get_top_processes(limit: int = 15) -> List[Dict]:
    """Return top processes in the container by RSS for live memory debugging."""
    rows = []
    now = time.time()
    for proc in psutil.process_iter(
        ["pid", "ppid", "name", "cmdline", "create_time", "memory_info", "status"]
    ):
        try:
            info = proc.info
            memory_info = info.get("memory_info")
            rss = getattr(memory_info, "rss", 0) if memory_info else 0
            cmdline = " ".join(info.get("cmdline") or [])
            rows.append(
                {
                    "pid": info["pid"],
                    "ppid": info["ppid"],
                    "name": info.get("name") or "",
                    "status": info.get("status") or "",
                    "rss_mb": round(rss / (1024 ** 2), 2),
                    "age_seconds": int(max(0, now - (info.get("create_time") or now))),
                    "cmdline": cmdline[:240],
                }
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    rows.sort(key=lambda item: item["rss_mb"], reverse=True)
    return rows[:limit]

class MonitorStats:
    """Tracks real-time server stats with Redis persistence."""

    def __init__(self, redis: aioredis.Redis):
        self.redis = redis
        self.start_time = time.time()

        # In-memory queues (fast reads, Redis backup)
        self.active_requests: Dict[str, Dict] = {}  # id -> request info
        self.completed_requests: deque = deque(maxlen=100)  # Last 100
        self.janitor_events: deque = deque(maxlen=100)
        self.errors: deque = deque(maxlen=100)

        # Endpoint stats (persisted in Redis)
        self.endpoint_stats: Dict[str, Dict] = {}  # endpoint -> {count, total_time, errors, ...}

        # Background persistence queue (max 10 pending persist requests)
        self._persist_queue: asyncio.Queue = asyncio.Queue(maxsize=10)
        self._persist_worker_task: Optional[asyncio.Task] = None

        # Timeline data (5min window, 5s resolution = 60 points)
        self.memory_timeline: deque = deque(maxlen=60)
        self.requests_timeline: deque = deque(maxlen=60)
        self.browser_timeline: deque = deque(maxlen=60)

    async def track_request_start(self, request_id: str, endpoint: str, url: str, config: Dict = None):
        """Track new request start."""
        req_info = {
            "id": request_id,
            "endpoint": endpoint,
            "url": url[:100],  # Truncate long URLs
            "start_time": time.time(),
            "config_sig": config.get("sig", "default") if config else "default",
            "mem_start": psutil.Process().memory_info().rss / (1024 * 1024)
        }
        self.active_requests[request_id] = req_info

        # Increment endpoint counter
        if endpoint not in self.endpoint_stats:
            self.endpoint_stats[endpoint] = {
                "count": 0, "total_time": 0, "errors": 0,
                "pool_hits": 0, "success": 0
            }
        self.endpoint_stats[endpoint]["count"] += 1

        # Queue persistence (handled by background worker)
        try:
            self._persist_queue.put_nowait(True)
        except asyncio.QueueFull:
            logger.warning("Persistence queue full, skipping")

    async def track_request_end(self, request_id: str, success: bool, error: str = None,
                               pool_hit: bool = True, status_code: int = 200):
        """Track request completion."""
        if request_id not in self.active_requests:
            return

        req_info = self.active_requests.pop(request_id)
        end_time = time.time()
        elapsed = end_time - req_info["start_time"]
        mem_end = psutil.Process().memory_info().rss / (1024 * 1024)
        mem_delta = mem_end - req_info["mem_start"]

        # Update stats
        endpoint = req_info["endpoint"]
        if endpoint in self.endpoint_stats:
            self.endpoint_stats[endpoint]["total_time"] += elapsed
            if success:
                self.endpoint_stats[endpoint]["success"] += 1
            else:
                self.endpoint_stats[endpoint]["errors"] += 1
            if pool_hit:
                self.endpoint_stats[endpoint]["pool_hits"] += 1

        # Add to completed queue
        completed = {
            **req_info,
            "end_time": end_time,
            "elapsed": round(elapsed, 2),
            "mem_delta": round(mem_delta, 1),
            "success": success,
            "error": error,
            "status_code": status_code,
            "pool_hit": pool_hit
        }
        self.completed_requests.append(completed)

        # Track errors
        if not success and error:
            self.errors.append({
                "timestamp": end_time,
                "endpoint": endpoint,
                "url": req_info["url"],
                "error": error,
                "request_id": request_id
            })

        await self._persist_endpoint_stats()

    async def track_janitor_event(self, event_type: str, sig: str, details: Dict):
        """Track janitor cleanup events."""
        self.janitor_events.append({
            "timestamp": time.time(),
            "type": event_type,  # "close_cold", "close_hot", "promote"
            "sig": sig[:8],
            "details": details
        })

    def _cleanup_old_entries(self, max_age_seconds: int = 300):
        """Remove entries older than max_age_seconds (default 5min)."""
        now = time.time()
        cutoff = now - max_age_seconds

        # Clean completed requests
        while self.completed_requests and self.completed_requests[0].get("end_time", 0) < cutoff:
            self.completed_requests.popleft()

        # Clean janitor events
        while self.janitor_events and self.janitor_events[0].get("timestamp", 0) < cutoff:
            self.janitor_events.popleft()

        # Clean errors
        while self.errors and self.errors[0].get("timestamp", 0) < cutoff:
            self.errors.popleft()

    async def update_timeline(self):
        """Update timeline data points (called every 5s)."""
        now = time.time()
        mem_pct = get_container_memory_percent()

        # Clean old entries (keep last 5 minutes)
        self._cleanup_old_entries(max_age_seconds=300)

        # Count requests in last 5s
        recent_reqs = sum(1 for req in self.completed_requests
                         if now - req.get("end_time", 0) < 5)

        # Browser counts (acquire lock to prevent race conditions)
        from crawler_pool import PERMANENT, HOT_POOL, COLD_POOL, LOCK
        async with LOCK:
            browser_count = {
                "permanent": 1 if PERMANENT else 0,
                "hot": len(HOT_POOL),
                "cold": len(COLD_POOL)
            }

        self.memory_timeline.append({"time": now, "value": mem_pct})
        self.requests_timeline.append({"time": now, "value": recent_reqs})
        self.browser_timeline.append({"time": now, "browsers": browser_count})

    async def _persist_endpoint_stats(self):
        """Persist endpoint stats to Redis."""
        try:
            await self.redis.set(
                "monitor:endpoint_stats",
                json.dumps(self.endpoint_stats),
                ex=86400  # 24h TTL
            )
        except Exception as e:
            logger.warning(f"Failed to persist endpoint stats: {e}")

    async def _persistence_worker(self):
        """Background worker to persist stats to Redis."""
        while True:
            try:
                await self._persist_queue.get()
                await self._persist_endpoint_stats()
                self._persist_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Persistence worker error: {e}")

    def start_persistence_worker(self):
        """Start the background persistence worker."""
        if not self._persist_worker_task:
            self._persist_worker_task = asyncio.create_task(self._persistence_worker())
            logger.info("Started persistence worker")

    async def stop_persistence_worker(self):
        """Stop the background persistence worker."""
        if self._persist_worker_task:
            self._persist_worker_task.cancel()
            try:
                await self._persist_worker_task
            except asyncio.CancelledError:
                pass
            self._persist_worker_task = None
            logger.info("Stopped persistence worker")

    async def cleanup(self):
        """Cleanup on shutdown - persist final stats and stop workers."""
        logger.info("Monitor cleanup starting...")
        try:
            # Persist final stats before shutdown
            await self._persist_endpoint_stats()
            # Stop background worker
            await self.stop_persistence_worker()
            logger.info("Monitor cleanup completed")
        except Exception as e:
            logger.error(f"Monitor cleanup error: {e}")

    async def load_from_redis(self):
        """Load persisted stats from Redis."""
        try:
            data = await self.redis.get("monitor:endpoint_stats")
            if data:
                self.endpoint_stats = json.loads(data)
                logger.info("Loaded endpoint stats from Redis")
        except Exception as e:
            logger.warning(f"Failed to load from Redis: {e}")

    async def get_health_summary(self) -> Dict:
        """Get current system health snapshot."""
        mem_pct = get_container_memory_percent()
        cpu_pct = psutil.cpu_percent(interval=0.1)

        # Network I/O (delta since last call)
        net = psutil.net_io_counters()

        # Pool status (acquire lock to prevent race conditions)
        from crawler_pool import PERMANENT, HOT_POOL, COLD_POOL, LOCK, get_janitor_state
        janitor_state = get_janitor_state()
        now = time.time()
        async with LOCK:
            # TODO: Track actual browser process memory instead of estimates
            # These are conservative estimates based on typical Chromium usage
            permanent_mem = 270 if PERMANENT else 0  # Estimate: ~270MB for permanent browser
            hot_mem = len(HOT_POOL) * 180  # Estimate: ~180MB per hot pool browser
            cold_mem = len(COLD_POOL) * 180  # Estimate: ~180MB per cold pool browser
            permanent_active = PERMANENT is not None
            hot_count = len(HOT_POOL)
            cold_count = len(COLD_POOL)

        return {
            "container": {
                "memory_percent": round(mem_pct, 1),
                "cpu_percent": round(cpu_pct, 1),
                "network_sent_mb": round(net.bytes_sent / (1024**2), 2),
                "network_recv_mb": round(net.bytes_recv / (1024**2), 2),
                "uptime_seconds": int(time.time() - self.start_time)
            },
            "pool": {
                "permanent": {"active": permanent_active, "memory_mb": permanent_mem},
                "hot": {"count": hot_count, "memory_mb": hot_mem},
                "cold": {"count": cold_count, "memory_mb": cold_mem},
                "total_memory_mb": permanent_mem + hot_mem + cold_mem
            },
            "janitor": {
                "next_cleanup_estimate": "adaptive",
                "memory_pressure": "LOW" if mem_pct < 60 else "MEDIUM" if mem_pct < 80 else "HIGH",
                "running": janitor_state["running"],
                "loop_count": janitor_state["loop_count"],
                "last_tick_epoch": janitor_state["last_tick"],
                "last_tick_seconds_ago": round(now - janitor_state["last_tick"], 1) if janitor_state["last_tick"] else None,
                "last_action": janitor_state["last_action"],
                "last_error": janitor_state["last_error"],
            }
        }

    def get_active_requests(self) -> List[Dict]:
        """Get list of currently active requests."""
        now = time.time()
        return [
            {
                **req,
                "elapsed": round(now - req["start_time"], 1),
                "status": "running"
            }
            for req in self.active_requests.values()
        ]

    def get_completed_requests(self, limit: int = 50, filter_status: str = "all") -> List[Dict]:
        """Get recent completed requests."""
        requests = list(self.completed_requests)[-limit:]
        if filter_status == "success":
            requests = [r for r in requests if r.get("success")]
        elif filter_status == "error":
            requests = [r for r in requests if not r.get("success")]
        return requests

    async def get_browser_list(self) -> List[Dict]:
        """Get detailed browser pool information."""
        from crawler_pool import PERMANENT, HOT_POOL, COLD_POOL, LAST_USED, USAGE_COUNT, DEFAULT_CONFIG_SIG, LOCK

        browsers = []
        now = time.time()

        # Acquire lock to prevent race conditions during iteration
        async with LOCK:
            if PERMANENT:
                browsers.append({
                    "type": "permanent",
                    "sig": DEFAULT_CONFIG_SIG[:8] if DEFAULT_CONFIG_SIG else "unknown",
                    "age_seconds": int(now - self.start_time),
                    "last_used_seconds": int(now - LAST_USED.get(DEFAULT_CONFIG_SIG, now)),
                    "memory_mb": 270,
                    "hits": USAGE_COUNT.get(DEFAULT_CONFIG_SIG, 0),
                    "killable": False
                })

            for sig, crawler in HOT_POOL.items():
                browsers.append({
                    "type": "hot",
                    "sig": sig[:8],
                    "age_seconds": int(now - self.start_time),  # Approximation
                    "last_used_seconds": int(now - LAST_USED.get(sig, now)),
                    "memory_mb": 180,  # Estimate
                    "hits": USAGE_COUNT.get(sig, 0),
                    "killable": True
                })

            for sig, crawler in COLD_POOL.items():
                browsers.append({
                    "type": "cold",
                    "sig": sig[:8],
                    "age_seconds": int(now - self.start_time),
                    "last_used_seconds": int(now - LAST_USED.get(sig, now)),
                    "memory_mb": 180,
                    "hits": USAGE_COUNT.get(sig, 0),
                    "killable": True
                })

        return browsers

    def get_endpoint_stats_summary(self) -> Dict[str, Dict]:
        """Get aggregated endpoint statistics."""
        summary = {}
        for endpoint, stats in self.endpoint_stats.items():
            count = stats["count"]
            avg_time = (stats["total_time"] / count) if count > 0 else 0
            success_rate = (stats["success"] / count * 100) if count > 0 else 0
            pool_hit_rate = (stats["pool_hits"] / count * 100) if count > 0 else 0

            summary[endpoint] = {
                "count": count,
                "avg_latency_ms": round(avg_time * 1000, 1),
                "success_rate_percent": round(success_rate, 1),
                "pool_hit_rate_percent": round(pool_hit_rate, 1),
                "errors": stats["errors"]
            }
        return summary

    def get_timeline_data(self, metric: str, window: str = "5m") -> Dict:
        """Get timeline data for charts."""
        # For now, only 5m window supported
        if metric == "memory":
            data = list(self.memory_timeline)
        elif metric == "requests":
            data = list(self.requests_timeline)
        elif metric == "browsers":
            data = list(self.browser_timeline)
        else:
            return {"timestamps": [], "values": []}

        return {
            "timestamps": [int(d["time"]) for d in data],
            "values": [d.get("value", d.get("browsers")) for d in data]
        }

    def get_janitor_log(self, limit: int = 100) -> List[Dict]:
        """Get recent janitor events."""
        return list(self.janitor_events)[-limit:]

    def get_errors_log(self, limit: int = 100) -> List[Dict]:
        """Get recent errors."""
        return list(self.errors)[-limit:]

    def get_runtime_snapshot(self, top_n: int = 15) -> Dict:
        """Return cgroup/process-level runtime memory details for debugging."""
        top_n = max(1, min(top_n, 50))
        cgroup_memory = _read_cgroup_memory_snapshot()
        return {
            "timestamp": time.time(),
            "container_memory_percent": round(get_container_memory_percent(), 1),
            "process_rss_mb": round(psutil.Process().memory_info().rss / (1024 ** 2), 2),
            "cgroup_memory": cgroup_memory,
            "top_processes": _get_top_processes(limit=top_n),
        }

# Global instance (initialized in server.py)
monitor_stats: Optional[MonitorStats] = None

def get_monitor() -> MonitorStats:
    """Get global monitor instance."""
    if monitor_stats is None:
        raise RuntimeError("Monitor not initialized")
    return monitor_stats
