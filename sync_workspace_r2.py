from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
import logging
import os
import time
from typing import Iterable, Optional, Tuple

logger = logging.getLogger("runpod_tricks.workspace_sync")
WORKSPACE_DIRS = (".codex", ".vscode-server")
LOG_EVERY = 200
ETA_WINDOWS = (10, 50, 200)


@dataclass(frozen=True)
class R2Config:
    account_id: str
    bucket: str
    endpoint: str
    access_key: str
    secret_key: str
    token: Optional[str] = None
    prefix_workspace: str = "workspace/backups"


def _env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return ""


def _public_config_paths() -> Iterable[Path]:
    env_path = _env_first("AF_R2_PUBLIC_CONFIG", "R2_PUBLIC_CONFIG")
    paths = []
    if env_path:
        paths.append(Path(env_path))
    workspace = _workspace_root()
    paths.append(workspace / "AlphaForecasting" / "config" / "r2_public.json")
    paths.append(workspace / "AlphaMorphing" / "config" / "r2_public.json")
    return paths


def _secret_config_paths() -> Iterable[Path]:
    env_path = _env_first("AF_R2_CONFIG", "R2_CONFIG")
    paths = []
    if env_path:
        paths.append(Path(env_path))
    workspace = _workspace_root()
    paths.append(workspace / "AlphaForecasting" / ".secrets" / "r2.json")
    paths.append(workspace / "AlphaMorphing" / ".secrets" / "r2.json")
    paths.append(workspace / "runpod_tricks" / ".secrets" / "r2.json")
    return paths


def _load_public_config() -> dict:
    for path in _public_config_paths():
        try:
            if path.exists():
                return json.loads(path.read_text())
        except Exception:
            continue
    return {}


def _load_secret_config() -> dict:
    for path in _secret_config_paths():
        try:
            if path.exists():
                return json.loads(path.read_text())
        except Exception:
            continue
    return {}


def load_r2_config() -> Optional[R2Config]:
    cfg = _load_public_config()
    allow_flag = _env_first("AF_R2_ALLOW_FILE_SECRETS", "R2_ALLOW_FILE_SECRETS")
    allow_file_secrets = True
    if allow_flag:
        allow_file_secrets = allow_flag.lower() in {"1", "true", "yes", "on"}
    secret_cfg = _load_secret_config() if allow_file_secrets else {}
    account_id = _env_first("AF_R2_ACCOUNT_ID", "R2_ACCOUNT_ID") or cfg.get("account_id") or ""
    bucket = _env_first("AF_R2_BUCKET", "R2_BUCKET") or cfg.get("bucket") or ""
    endpoint = _env_first("AF_R2_ENDPOINT", "R2_ENDPOINT") or cfg.get("endpoint") or ""
    access_key = _env_first(
        "AF_R2_ACCESS_KEY",
        "AF_R2_ACCESS_KEY_ID",
        "R2_ACCESS_KEY",
        "R2_ACCESS_KEY_ID",
    ) or (secret_cfg.get("access_key") if allow_file_secrets else "")
    secret_key = _env_first(
        "AF_R2_SECRET_KEY",
        "AF_R2_SECRET_ACCESS_KEY",
        "AF_R2_SECRET_KEY_ID",
        "R2_SECRET_KEY",
        "R2_SECRET_ACCESS_KEY",
    ) or (secret_cfg.get("secret_key") if allow_file_secrets else "")
    token = _env_first("AF_R2_TOKEN", "R2_TOKEN") or (secret_cfg.get("token") if allow_file_secrets else None)
    prefix_workspace = (
        _env_first("AF_R2_PREFIX_WORKSPACE", "R2_PREFIX_WORKSPACE")
        or cfg.get("prefix_workspace")
        or "workspace/backups"
    )

    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"

    if not (bucket and endpoint and access_key and secret_key):
        return None

    return R2Config(
        account_id=account_id,
        bucket=bucket,
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        token=token,
        prefix_workspace=prefix_workspace,
    )


class RollingEta:
    def __init__(self, windows: Iterable[int]) -> None:
        self._windows = tuple(windows)
        self._samples = {window: [] for window in self._windows}

    def add(self, duration: float) -> None:
        for window in self._samples:
            samples = self._samples[window]
            samples.append(duration)
            if len(samples) > window:
                samples.pop(0)

    def format(self, remaining: int) -> str:
        if remaining <= 0:
            return "0s"
        parts = []
        for window, samples in self._samples.items():
            if not samples:
                continue
            avg = sum(samples) / len(samples)
            eta = remaining * avg
            parts.append(f"w{window}(n={len(samples)})={_fmt_duration(eta)}")
        return ", ".join(parts) if parts else "n/a"


def _fmt_duration(seconds: float) -> str:
    if seconds is None or seconds < 0:
        return "n/a"
    seconds = int(round(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:d}:{secs:02d}"


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s")


def _workspace_root() -> Path:
    env_path = os.getenv("AF_WORKSPACE_ROOT")
    if env_path:
        return Path(env_path).expanduser()
    return Path(__file__).resolve().parents[1]


def _iter_workspace_files() -> Iterable[Path]:
    root = _workspace_root()
    for folder in WORKSPACE_DIRS:
        base = root / folder
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_file():
                yield path


def _workspace_key(cfg, file_path: Path) -> str:
    rel_path = file_path.relative_to(_workspace_root()).as_posix()
    prefix = cfg.prefix_workspace.rstrip("/")
    return f"{prefix}/{rel_path}"


def _client():
    cfg = load_r2_config()
    if not cfg:
        raise RuntimeError("R2 config not available.")
    try:
        import boto3  # type: ignore
        from botocore.config import Config  # type: ignore
    except Exception as exc:
        raise RuntimeError("boto3 is required for R2 sync; install boto3") from exc
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def _head_object(client, bucket: str, key: str) -> Optional[dict]:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except Exception:
        return None


def _should_upload(local_path: Path, remote_meta: Optional[dict]) -> bool:
    if remote_meta is None:
        return True
    try:
        remote_size = int(remote_meta.get("ContentLength") or 0)
    except (TypeError, ValueError):
        remote_size = 0
    if local_path.stat().st_size != remote_size:
        return True
    remote_time = remote_meta.get("LastModified")
    if remote_time is None:
        return False
    local_mtime = datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
    return local_mtime > remote_time


def _sync_file(client, cfg, local_path: Path) -> Tuple[str, str, Optional[str], float]:
    start = time.monotonic()
    key = _workspace_key(cfg, local_path)
    try:
        meta = _head_object(client, cfg.bucket, key)
        if not _should_upload(local_path, meta):
            return key, "skipped", None, time.monotonic() - start
        client.upload_file(str(local_path), cfg.bucket, key)
        return key, "uploaded", None, time.monotonic() - start
    except Exception as exc:
        return key, "failed", str(exc), time.monotonic() - start


def sync_workspace(cfg, workers: int) -> None:
    files = list(_iter_workspace_files())
    if not files:
        logger.info("No workspace files found to sync.")
        return
    logger.info("Workspace sync: %d files to scan.", len(files))

    tracker = RollingEta(ETA_WINDOWS)
    phase_start = time.monotonic()
    processed = 0
    skipped = 0
    failed = 0
    uploaded = 0
    client = _client()

    if workers <= 1:
        for path in files:
            key, status, err, elapsed = _sync_file(client, cfg, path)
            processed += 1
            if status == "failed":
                failed += 1
                logger.warning("Workspace %s failed: %s", key, err)
            elif status == "skipped":
                skipped += 1
            else:
                uploaded += 1
                tracker.add(elapsed)
            if processed % LOG_EVERY == 0 or processed == len(files):
                remaining = max(len(files) - processed, 0)
                elapsed_total = time.monotonic() - phase_start
                logger.info(
                    "Workspace: %d/%d done (%d uploaded, %d skipped, %d failed). Elapsed %s. ETA %s",
                    processed,
                    len(files),
                    uploaded,
                    skipped,
                    failed,
                    _fmt_duration(elapsed_total),
                    tracker.format(remaining),
                )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_sync_file, client, cfg, path) for path in files]
            for future in as_completed(futures):
                key, status, err, elapsed = future.result()
                processed += 1
                if status == "failed":
                    failed += 1
                    logger.warning("Workspace %s failed: %s", key, err)
                elif status == "skipped":
                    skipped += 1
                else:
                    uploaded += 1
                    tracker.add(elapsed)
                if processed % LOG_EVERY == 0 or processed == len(files):
                    remaining = max(len(files) - processed, 0)
                    elapsed_total = time.monotonic() - phase_start
                    logger.info(
                        "Workspace: %d/%d done (%d uploaded, %d skipped, %d failed). Elapsed %s. ETA %s",
                        processed,
                        len(files),
                        uploaded,
                        skipped,
                        failed,
                        _fmt_duration(elapsed_total),
                        tracker.format(remaining),
                    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync .codex/.vscode-server to Cloudflare R2")
    parser.add_argument("--workers", type=int, default=None, help="Worker threads for sync")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    _setup_logging(args.verbose)
    cfg = load_r2_config()
    if not cfg:
        missing = []
        if not _env_first("AF_R2_ACCESS_KEY", "AF_R2_ACCESS_KEY_ID", "R2_ACCESS_KEY", "R2_ACCESS_KEY_ID"):
            missing.append("AF_R2_ACCESS_KEY")
        if not _env_first(
            "AF_R2_SECRET_KEY",
            "AF_R2_SECRET_ACCESS_KEY",
            "AF_R2_SECRET_KEY_ID",
            "R2_SECRET_KEY",
            "R2_SECRET_ACCESS_KEY",
        ):
            missing.append("AF_R2_SECRET_KEY")
        if missing:
            raise SystemExit(f"R2 credentials missing: {', '.join(missing)}")
        raise SystemExit("R2 config not found; set AF_R2_* or config/r2_public.json")

    workers = args.workers
    if workers is None:
        workers = int(os.getenv("AF_R2_WORKSPACE_WORKERS", "4") or 4)
    workers = max(1, workers)
    logger.info("Using %d worker(s) for workspace sync.", workers)
    logger.info("Workspace root: %s", _workspace_root())
    logger.info("Prefix: %s", cfg.prefix_workspace)
    sync_workspace(cfg, workers)
    logger.info("Workspace sync completed.")


if __name__ == "__main__":
    main()
