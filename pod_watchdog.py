from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
from typing import Dict, List, Optional
import urllib.request
from urllib.error import HTTPError, URLError

CONFIG_PATH = Path(__file__).with_name("runpod_config.json")
STATE_PATH = Path(__file__).with_name("runpod_watchdog_state.json")
GRAPHQL_URL = "https://api.runpod.io/graphql"


def _log(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[{timestamp}] {message}", flush=True)


def _load_config() -> Dict[str, object]:
    if CONFIG_PATH.exists():
        data = json.loads(CONFIG_PATH.read_text())
    else:
        data = {}
    data.setdefault("api_key", os.getenv("RUNPOD_API_KEY", ""))
    data.setdefault("idle_seconds", 600)
    data.setdefault("poll_seconds", 60)
    data.setdefault("gpu_util_threshold", 5)
    data.setdefault("gpu_mem_fraction_threshold", 0.05)
    data.setdefault("terminate_mode", "terminate")
    data.setdefault("terminate_on_empty_queue", False)
    data.setdefault("empty_queue_grace_seconds", 0)
    data.setdefault("enabled", False)
    data.setdefault("idle_enabled", False)
    data.setdefault("queue_empty_enabled", data.get("terminate_on_empty_queue", False))
    data.setdefault("terminate_all", False)
    data.setdefault("db_path", os.getenv("AF_DB_PATH", "/workspace/AlphaForecasting/runs/experiments.sqlite"))
    return data


def _write_state(state: Dict[str, object]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))


def _graphql_request(api_key: str, query: str) -> Dict[str, object]:
    payload = json.dumps({"query": query}).encode("utf-8")
    req = urllib.request.Request(GRAPHQL_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {api_key}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
    data = json.loads(body)
    if "errors" in data:
        raise RuntimeError(data["errors"])
    return data.get("data") or {}


def _list_pods(api_key: str) -> List[Dict[str, str]]:
    data = _graphql_request(api_key, "{ myself { pods { id name desiredStatus } } }")
    myself = data.get("myself") or {}
    return list(myself.get("pods") or [])


def _terminate_pod(api_key: str, pod_id: str, mode: str) -> None:
    if mode == "stop":
        query = f"mutation {{ podStop(input: {{podId: \"{pod_id}\"}}) {{ id desiredStatus }} }}"
    else:
        query = f"mutation {{ podTerminate(input: {{podId: \"{pod_id}\"}}) }}"
    _graphql_request(api_key, query)


def _get_experiment_counts(db_path: str) -> Dict[str, int]:
    counts = {"running": 0, "queued": 0, "paused": 0}
    path = Path(db_path)
    if not path.exists():
        return counts
    conn = sqlite3.connect(path)
    try:
        cursor = conn.execute(
            "SELECT status, COUNT(*) as total FROM experiments GROUP BY status"
        )
        for status, total in cursor.fetchall():
            if status in counts:
                counts[status] = int(total)
    finally:
        conn.close()
    return counts


def _gpu_active(util_threshold: float, mem_fraction_threshold: float) -> bool:
    try:
        output = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False
    for line in output.strip().splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 3:
            continue
        try:
            util = float(parts[0])
            mem_used = float(parts[1])
            mem_total = float(parts[2])
        except ValueError:
            continue
        mem_fraction = mem_used / mem_total if mem_total else 0.0
        if util >= util_threshold or mem_fraction >= mem_fraction_threshold:
            return True
    return False


def main() -> None:
    cfg = _load_config()
    api_key = str(cfg.get("api_key") or "").strip()
    if not api_key:
        _log("Missing Runpod API key. Set RUNPOD_API_KEY or runpod_config.json.")
        return

    idle_since: Optional[float] = None
    empty_queue_since: Optional[float] = None
    last_config_sig: Optional[tuple] = None
    _log("Runpod watchdog started.")

    while True:
        try:
            cfg = _load_config()
            poll_seconds = int(cfg.get("poll_seconds") or 60)
            idle_seconds = int(cfg.get("idle_seconds") or 600)
            util_threshold = float(cfg.get("gpu_util_threshold") or 0)
            mem_threshold = float(cfg.get("gpu_mem_fraction_threshold") or 0)
            terminate_mode = str(cfg.get("terminate_mode") or "terminate").lower()
            empty_queue_grace = int(cfg.get("empty_queue_grace_seconds") or 0)
            terminate_all = bool(cfg.get("terminate_all"))
            db_path = str(cfg.get("db_path"))
            enabled = bool(cfg.get("enabled", False))
            idle_enabled = bool(cfg.get("idle_enabled", False))
            queue_enabled = bool(cfg.get("queue_empty_enabled", cfg.get("terminate_on_empty_queue", False)))
            config_sig = (
                enabled,
                idle_enabled,
                queue_enabled,
                idle_seconds,
                empty_queue_grace,
                poll_seconds,
                terminate_mode,
                util_threshold,
                mem_threshold,
            )
            if config_sig != last_config_sig:
                last_config_sig = config_sig
                _log(
                    "Config: enabled={} idle={} queue={} idle_seconds={} empty_queue_grace={} poll={} mode={} gpu_util={} gpu_mem_frac={}".format(
                        enabled,
                        idle_enabled,
                        queue_enabled,
                        idle_seconds,
                        empty_queue_grace,
                        poll_seconds,
                        terminate_mode,
                        util_threshold,
                        mem_threshold,
                    )
                )

            counts = _get_experiment_counts(db_path)
            active_running = counts.get("running", 0)
            active_queued = counts.get("queued", 0)
            active_paused = counts.get("paused", 0)
            active = active_running + active_queued + active_paused
            gpu_active = _gpu_active(util_threshold, mem_threshold)

            now = time.time()
            if not enabled:
                idle_since = None
                empty_queue_since = None
                _write_state(
                    {
                        "timestamp": now,
                        "enabled": False,
                        "running": active_running,
                        "queued": active_queued,
                        "paused": active_paused,
                        "gpu_active": gpu_active,
                        "idle_since": None,
                        "empty_queue_since": None,
                    }
                )
                _log(
                    "Status: enabled=false running={} queued={} paused={} gpu_active={}".format(
                        active_running, active_queued, active_paused, gpu_active
                    )
                )
                time.sleep(max(5, poll_seconds))
                continue

            if active > 0 or gpu_active:
                idle_since = None
            else:
                if idle_since is None:
                    idle_since = now

            if queue_enabled and active_running == 0 and active_queued == 0 and active_paused == 0:
                if empty_queue_since is None:
                    empty_queue_since = now
                if now - empty_queue_since >= empty_queue_grace:
                    _log(
                        "Status: enabled=true running={} queued={} paused={} gpu_active={} idle_for={}s empty_for={}s".format(
                            active_running,
                            active_queued,
                            active_paused,
                            gpu_active,
                            int(now - idle_since) if idle_since else 0,
                            int(now - empty_queue_since) if empty_queue_since else 0,
                        )
                    )
                    _log("Queue empty; requesting pod termination.")
                    _terminate_now(api_key, terminate_mode, terminate_all)
                    return
            else:
                empty_queue_since = None

            if idle_enabled and idle_since and now - idle_since >= idle_seconds:
                _log(
                    "Status: enabled=true running={} queued={} paused={} gpu_active={} idle_for={}s empty_for={}s".format(
                        active_running,
                        active_queued,
                        active_paused,
                        gpu_active,
                        int(now - idle_since) if idle_since else 0,
                        int(now - empty_queue_since) if empty_queue_since else 0,
                    )
                )
                _log("Idle threshold reached; requesting pod termination.")
                _terminate_now(api_key, terminate_mode, terminate_all)
                return

            _write_state(
                {
                    "timestamp": time.time(),
                    "enabled": enabled,
                    "idle_enabled": idle_enabled,
                    "queue_empty_enabled": queue_enabled,
                    "running": active_running,
                    "queued": active_queued,
                    "paused": active_paused,
                    "gpu_active": gpu_active,
                    "idle_since": idle_since,
                    "empty_queue_since": empty_queue_since,
                }
            )
            _log(
                "Status: enabled=true running={} queued={} paused={} gpu_active={} idle_for={}s empty_for={}s".format(
                    active_running,
                    active_queued,
                    active_paused,
                    gpu_active,
                    int(now - idle_since) if idle_since else 0,
                    int(now - empty_queue_since) if empty_queue_since else 0,
                )
            )
        except (HTTPError, URLError, RuntimeError, sqlite3.Error, OSError) as exc:
            _log(f"Watchdog error: {exc}")

        time.sleep(max(5, poll_seconds))


def _terminate_now(api_key: str, mode: str, terminate_all: bool) -> None:
    pods = _list_pods(api_key)
    if not pods:
        _log("No pods found; skipping termination.")
        return
    env_pod_id = os.getenv("RUNPOD_POD_ID")
    target_ids: List[str] = []
    if env_pod_id:
        target_ids = [env_pod_id]
    elif terminate_all:
        target_ids = [pod["id"] for pod in pods if pod.get("id")]
    else:
        if len(pods) > 1:
            _log(f"Multiple pods found ({len(pods)}); terminating first only.")
        target_ids = [pods[0]["id"]]

    for pod_id in target_ids:
        _log(f"Sending {mode} request for pod {pod_id}.")
        _terminate_pod(api_key, pod_id, mode)


if __name__ == "__main__":
    main()
