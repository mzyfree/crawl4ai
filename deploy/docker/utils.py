import dns.resolver
import logging
import yaml
import os
import time
import re
import posixpath
from datetime import datetime
from enum import Enum
from pathlib import Path
from fastapi import Request
from typing import Dict, Optional

class TaskStatus(str, Enum):
    PROCESSING = "processing"
    FAILED = "failed"
    COMPLETED = "completed"

class FilterType(str, Enum):
    RAW = "raw"
    FIT = "fit"
    BM25 = "bm25"
    LLM = "llm"

def load_config() -> Dict:
    """Load and return application configuration with environment variable overrides."""
    config_path = Path(__file__).parent / "config.yml"
    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)
    
    # Override LLM provider from environment if set
    llm_provider = os.environ.get("LLM_PROVIDER")
    if llm_provider:
        config["llm"]["provider"] = llm_provider
        logging.info(f"LLM provider overridden from environment: {llm_provider}")
    
    # Also support direct API key from environment if the provider-specific key isn't set
    llm_api_key = os.environ.get("LLM_API_KEY")
    if llm_api_key and "api_key" not in config["llm"]:
        config["llm"]["api_key"] = llm_api_key
        logging.info("LLM API key loaded from LLM_API_KEY environment variable")
    
    return config

def setup_logging(config: Dict) -> None:
    """Configure application logging."""
    logging.basicConfig(
        level=config["logging"]["level"],
        format=config["logging"]["format"]
    )

def get_base_url(request: Request) -> str:
    """Get base URL including scheme and host."""
    return f"{request.url.scheme}://{request.url.netloc}"

def is_task_id(value: str) -> bool:
    """Check if the value matches task ID pattern."""
    return value.startswith("llm_") and "_" in value

def datetime_handler(obj: any) -> Optional[str]:
    """Handle datetime serialization for JSON."""
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def should_cleanup_task(created_at: str, ttl_seconds: int = 3600) -> bool:
    """Check if task should be cleaned up based on creation time."""
    created = datetime.fromisoformat(created_at)
    return (datetime.now() - created).total_seconds() > ttl_seconds

def decode_redis_hash(hash_data: Dict[bytes, bytes]) -> Dict[str, str]:
    """Decode Redis hash data from bytes to strings."""
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in hash_data.items()}



def get_llm_api_key(config: Dict, provider: Optional[str] = None) -> Optional[str]:
    """Get the appropriate API key based on the LLM provider.
    
    Args:
        config: The application configuration dictionary
        provider: Optional provider override (e.g., "openai/gpt-4")
    
    Returns:
        The API key if directly configured, otherwise None to let litellm handle it
    """
    # Check if direct API key is configured (for backward compatibility)
    if "api_key" in config["llm"]:
        return config["llm"]["api_key"]
    
    # Return None - litellm will automatically find the right environment variable
    return None


def validate_llm_provider(config: Dict, provider: Optional[str] = None) -> tuple[bool, str]:
    """Validate that the LLM provider has an associated API key.
    
    Args:
        config: The application configuration dictionary
        provider: Optional provider override (e.g., "openai/gpt-4")
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    # If a direct API key is configured, validation passes
    if "api_key" in config["llm"]:
        return True, ""
    
    # Otherwise, trust that litellm will find the appropriate environment variable
    # We can't easily validate this without reimplementing litellm's logic
    return True, ""


def get_llm_temperature(config: Dict, provider: Optional[str] = None) -> Optional[float]:
    """Get temperature setting based on the LLM provider.
    
    Priority order:
    1. Provider-specific environment variable (e.g., OPENAI_TEMPERATURE)
    2. Global LLM_TEMPERATURE environment variable
    3. None (to use litellm/provider defaults)
    
    Args:
        config: The application configuration dictionary
        provider: Optional provider override (e.g., "openai/gpt-4")
    
    Returns:
        The temperature setting if configured, otherwise None
    """
    # Check provider-specific temperature first
    if provider:
        provider_name = provider.split('/')[0].upper()
        provider_temp = os.environ.get(f"{provider_name}_TEMPERATURE")
        if provider_temp:
            try:
                return float(provider_temp)
            except ValueError:
                logging.warning(f"Invalid temperature value for {provider_name}: {provider_temp}")
    
    # Check global LLM_TEMPERATURE
    global_temp = os.environ.get("LLM_TEMPERATURE")
    if global_temp:
        try:
            return float(global_temp)
        except ValueError:
            logging.warning(f"Invalid global temperature value: {global_temp}")
    
    # Return None to use litellm/provider defaults
    return None


def get_llm_base_url(config: Dict, provider: Optional[str] = None) -> Optional[str]:
    """Get base URL setting based on the LLM provider.
    
    Priority order:
    1. Provider-specific environment variable (e.g., OPENAI_BASE_URL)
    2. Global LLM_BASE_URL environment variable
    3. None (to use default endpoints)
    
    Args:
        config: The application configuration dictionary
        provider: Optional provider override (e.g., "openai/gpt-4")
    
    Returns:
        The base URL if configured, otherwise None
    """
    # Check provider-specific base URL first
    if provider:
        provider_name = provider.split('/')[0].upper()
        provider_url = os.environ.get(f"{provider_name}_BASE_URL")
        if provider_url:
            return provider_url
    
    # Check global LLM_BASE_URL
    return os.environ.get("LLM_BASE_URL")


def verify_email_domain(email: str) -> bool:
    try:
        domain = email.split('@')[1]
        # Try to resolve MX records for the domain.
        records = dns.resolver.resolve(domain, 'MX')
        return True if records else False
    except Exception as e:
        return False

def get_container_memory_percent() -> float:
    """Get actual memory usage vs limit (cgroup v1/v2 aware)."""
    try:
        # Try cgroup v2 first
        usage_path = Path("/sys/fs/cgroup/memory.current")
        limit_path = Path("/sys/fs/cgroup/memory.max")
        if not usage_path.exists():
            # Fall back to cgroup v1
            usage_path = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
            limit_path = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

        usage = int(usage_path.read_text())
        limit_raw = limit_path.read_text().strip()
        limit = int(limit_raw) if limit_raw != "max" else int(1e19)

        # Handle unlimited (v2: "max", v1: > 1e18)
        if limit > 1e18:
            import psutil
            limit = psutil.virtual_memory().total

        return (usage / limit) * 100
    except:
        # Non-container or unsupported: fallback to host
        import psutil
        return psutil.virtual_memory().percent


def _read_text_if_exists(path: Path) -> Optional[str]:
    try:
        if path.exists():
            return path.read_text().strip()
    except Exception:
        return None
    return None


def _extract_cpu_usage_usec(cpu_stat_txt: str) -> Optional[int]:
    try:
        for line in cpu_stat_txt.splitlines():
            if line.startswith("usage_usec "):
                return int(line.split()[1])
    except Exception:
        return None
    return None


def _get_cgroup_cpu_usage_ns() -> Optional[int]:
    """
    Return container CPU usage in nanoseconds from cgroup v2 or v1.
    - v2: /sys/fs/cgroup/cpu.stat -> usage_usec
    - v1: /sys/fs/cgroup/cpuacct/cpuacct.usage (ns)
    """
    # cgroup v2
    cpu_stat = _read_text_if_exists(Path("/sys/fs/cgroup/cpu.stat"))
    if cpu_stat:
        usec = _extract_cpu_usage_usec(cpu_stat)
        if usec is not None:
            return usec * 1000

    # cgroup v1
    usage_ns = _read_text_if_exists(Path("/sys/fs/cgroup/cpuacct/cpuacct.usage"))
    if usage_ns:
        return int(usage_ns)

    return None


def _get_cgroup_cpu_limit_cores() -> Optional[float]:
    """
    Return CPU quota as "cores" if configured; otherwise None for unlimited/unknown.
    - v2: /sys/fs/cgroup/cpu.max -> "<quota> <period>" or "max <period>"
    - v1: /sys/fs/cgroup/cpu/cpu.cfs_quota_us and cpu.cfs_period_us
    """
    # cgroup v2
    cpu_max = _read_text_if_exists(Path("/sys/fs/cgroup/cpu.max"))
    if cpu_max:
        parts = cpu_max.split()
        if len(parts) >= 2:
            quota_s, period_s = parts[0], parts[1]
            if quota_s != "max":
                quota_us = int(quota_s)
                period_us = int(period_s)
                if quota_us > 0 and period_us > 0:
                    return quota_us / period_us

    # cgroup v1
    quota_txt = _read_text_if_exists(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"))
    period_txt = _read_text_if_exists(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us"))
    if quota_txt and period_txt:
        quota_us = int(quota_txt)
        period_us = int(period_txt)
        if quota_us > 0 and period_us > 0:
            return quota_us / period_us

    return None


def get_container_cpu_percent(sample_window_sec: float = 1.0) -> float:
    """
    Approximate container CPU usage percent relative to its CPU quota.

    - Returns a value in [0, 100] where 100 means the container is fully
      consuming its allocated CPU limit (quota).
    - Uses in-function sampling window (blocking), similar to NWSS.
    """
    quota_cores = _get_cgroup_cpu_limit_cores()
    if quota_cores is None or quota_cores <= 0:
        quota_cores = float(os.cpu_count() or 1)

    if sample_window_sec <= 0:
        sample_window_sec = 0.5

    u1 = _get_cgroup_cpu_usage_ns()
    t1 = time.monotonic()
    if u1 is None:
        import psutil
        return float(psutil.Process().cpu_percent(interval=sample_window_sec))

    time.sleep(sample_window_sec)

    u2 = _get_cgroup_cpu_usage_ns()
    t2 = time.monotonic()
    if u2 is None:
        return 0.0

    dt = t2 - t1
    if dt <= 0:
        return 0.0

    d_usage = u2 - u1
    if d_usage <= 0:
        return 0.0

    pct = (d_usage / (dt * 1e9 * float(quota_cores))) * 100.0
    if pct < 0:
        return 0.0
    if pct > 100.0:
        return 100.0
    return pct