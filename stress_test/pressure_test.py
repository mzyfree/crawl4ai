#!/usr/bin/env python3
import argparse
import json
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Optional


stop_flag = False


def req(
    url: str,
    method: str = "GET",
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> tuple[int, str]:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url=url, method=method, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.status, response.read().decode("utf-8", errors="ignore")


def load_worker(
    target_url: str,
    method: str,
    body: Optional[Dict[str, Any]],
    timeout: int,
    stats: Dict[str, Any],
) -> None:
    global stop_flag
    while not stop_flag:
        start = time.time()
        try:
            code, _ = req(target_url, method=method, body=body, timeout=timeout)
            cost_ms = (time.time() - start) * 1000
            with stats["lock"]:
                stats["total"] += 1
                if 200 <= code < 500:
                    stats["ok"] += 1
                else:
                    stats["err"] += 1
                stats["lat_ms_sum"] += cost_ms
        except Exception:
            with stats["lock"]:
                stats["total"] += 1
                stats["err"] += 1


def ready_monitor(readyz_url: str, interval: float) -> None:
    global stop_flag
    last_status = None
    while not stop_flag:
        try:
            code, body = req(readyz_url, method="GET", timeout=5)
            if code != last_status:
                print(f"[readyz] status={code} body={body[:180]}")
                last_status = code
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                body = ""
            if e.code != last_status:
                print(f"[readyz] status={e.code} body={body[:180]}")
                last_status = e.code
        except Exception as e:
            print(f"[readyz] error={e}")
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stress one crawl4ai pod via hostNetwork node ip and monitor /readyz."
    )
    parser.add_argument("--node-ip", required=True, help="Target node IP, e.g. 10.35.58.191")
    parser.add_argument("--port", type=int, default=11235, help="Target port, default 11235")
    parser.add_argument("--duration", type=int, default=600, help="Stress duration in seconds")
    parser.add_argument("--concurrency", type=int, default=40, help="Concurrent worker count")
    parser.add_argument(
        "--monitor-interval",
        type=float,
        default=2.0,
        help="Seconds between /readyz checks",
    )
    parser.add_argument(
        "--target-path",
        default="/crawl",
        help="Target API path to pressure",
    )
    parser.add_argument("--method", default="POST", choices=["GET", "POST"], help="HTTP method")
    parser.add_argument("--timeout", type=int, default=15, help="Single request timeout seconds")
    parser.add_argument(
        "--body-file",
        default="stress_test/crawl_payload.json",
        help="Path to JSON body file (used when method is POST)",
    )
    parser.add_argument(
        "--body-json",
        default="",
        help='JSON body for POST, e.g. \'{"urls":["https://example.com"]}\'',
    )
    args = parser.parse_args()

    base = f"http://{args.node_ip}:{args.port}"
    target_url = base + args.target_path
    readyz_url = base + "/readyz"
    body = None
    if args.body_json:
        body = json.loads(args.body_json)
    elif args.method == "POST":
        body_path = Path(args.body_file)
        if not body_path.exists():
            raise FileNotFoundError(f"Body file not found: {body_path}")
        body = json.loads(body_path.read_text(encoding="utf-8"))

    stats: Dict[str, Any] = {
        "total": 0,
        "ok": 0,
        "err": 0,
        "lat_ms_sum": 0.0,
        "lock": threading.Lock(),
    }

    print(f"target={target_url}")
    print(f"readyz={readyz_url}")
    print(f"concurrency={args.concurrency}, duration={args.duration}s")

    monitor_thread = threading.Thread(
        target=ready_monitor, args=(readyz_url, args.monitor_interval), daemon=True
    )
    monitor_thread.start()

    start = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for _ in range(args.concurrency):
            pool.submit(load_worker, target_url, args.method, body, args.timeout, stats)

        while time.time() - start < args.duration:
            time.sleep(5)
            with stats["lock"]:
                total = stats["total"]
                ok = stats["ok"]
                err = stats["err"]
                avg_ms = (stats["lat_ms_sum"] / total) if total else 0.0
            print(f"[load] total={total} ok={ok} err={err} avg_ms={avg_ms:.1f}")

    global stop_flag
    stop_flag = True
    time.sleep(1)

    with stats["lock"]:
        total = stats["total"]
        ok = stats["ok"]
        err = stats["err"]
        avg_ms = (stats["lat_ms_sum"] / total) if total else 0.0
    print(f"[final] total={total} ok={ok} err={err} avg_ms={avg_ms:.1f}")


if __name__ == "__main__":
    main()
