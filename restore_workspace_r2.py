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

logger = logging.getLogger("runpod_tricks.workspace_restore")
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


def _public_config_paths() -> Iterable[Path]:
    env_path = os.getenv("AF_R2_PUBLIC_CONFIG") or os.getenv("R2_PUBLIC_CONFIG")
    paths = []
    if env_path:
        paths.append(Path(env_path))
    workspace = _workspace_root()
    paths.append(workspace / "AlphaForecasting" / "config" / "r2_public.json")
    paths.append(workspace / "AlphaMorphing" / "config" / "r2_public.json")
    return paths


def _load_public_config() -> dict:
    for path in _public_config_paths():
        try:
            if path.exists():
                return json.loads(path.read_text())
        except Exception:
            continue
    return {}


def load_r2_config() -> Optional[R2Config]:
    cfg = _load_public_config()
    account_id = os.getenv("AF_R2_ACCOUNT_ID") or os.getenv("R2_ACCOUNT_ID") or cfg.get("account_id") or ""
    bucket = os.getenv("AF_R2_BUCKET") or os.getenv("R2_BUCKET") or cfg.get("bucket") or ""
    endpoint = os.getenv("AF_R2_ENDPOINT") or os.getenv("R2_ENDPOINT") or cfg.get("endpoint") or ""
    access_key = os.getenv("AF_R2_ACCESS_KEY") or os.getenv("R2_ACCESS_KEY") or ""
    secret_key = os.getenv("AF_R2_SECRET_KEY") or os.getenv("R2_SECRET_KEY") or ""
    token = os.getenv("AF_R2_TOKEN") or os.getenv("R2_TOKEN")
    prefix_workspace = os.getenv("AF_R2_PREFIX_WORKSPACE") or os.getenv("R2_PREFIX_WORKSPACE") or cfg.get(
        "prefix_workspace"
    ) or "workspace/backups"

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


def _client():
    cfg = load_r2_config()
    if not cfg:
        raise RuntimeError("R2 config not available.")
    try:
        import boto3  # type: ignore
        from botocore.config import Config  # type: ignore
    except Exception as exc:
        raise RuntimeError("boto3 is required for R2 restore; install boto3") from exc
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def _list_objects(prefix: str) -> Iterable[dict]:
    cfg = load_r2_config()
    if not cfg:
        return []
    client = _client()
    token: Optional[str] = None
    while True:
        kwargs = {"Bucket": cfg.bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []) or []:
            yield item
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")


def _should_download(local_path: Path, size: int, remote_time: Optional[datetime], overwrite: bool) -> bool:
    if overwrite:
        return True
    if not local_path.exists():
        return True
    try:
        if local_path.stat().st_size != size:
            return True
    except FileNotFoundError:
        return True
    if remote_time is None:
        return False
    local_mtime = datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
    return local_mtime < remote_time


def _download_one(client, bucket: str, key: str, local_path: Path) -> Tuple[str, str, Optional[str], float]:
    start = time.monotonic()
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(local_path))
        return key, "downloaded", None, time.monotonic() - start
    except Exception as exc:
        return key, "failed", str(exc), time.monotonic() - start


def restore_workspace(cfg, workers: int, overwrite: bool) -> None:
    prefix = cfg.prefix_workspace.rstrip("/") + "/"
    workspace_root = _workspace_root()
    objects = []
    for item in _list_objects(prefix):
        key = item.get("Key") or ""
        if not key.startswith(prefix):
            continue
        rel_path = key[len(prefix) :]
        if not rel_path or rel_path.endswith("/"):
            continue
        objects.append((key, rel_path, item))

    total = len(objects)
    if total == 0:
        logger.info("No workspace objects found under prefix %s.", prefix)
        return

    logger.info("Workspace restore: %d objects to consider.", total)
    tracker = RollingEta(ETA_WINDOWS)
    processed = 0
    skipped = 0
    failed = 0
    downloaded = 0
    phase_start = time.monotonic()
    client = _client()

    pending = []
    for key, rel_path, item in objects:
        size = int(item.get("Size") or 0)
        remote_time = item.get("LastModified")
        local_path = workspace_root / rel_path
        if _should_download(local_path, size, remote_time, overwrite):
            pending.append((key, local_path))
        else:
            skipped += 1

    if not pending:
        logger.info("Workspace restore: all files already up to date.")
        return

    logger.info("Workspace restore: %d downloads pending (%d skipped).", len(pending), skipped)

    if workers <= 1:
        for key, local_path in pending:
            _, status, err, elapsed = _download_one(client, cfg.bucket, key, local_path)
            processed += 1
            if status == "failed":
                failed += 1
                logger.warning("Workspace %s failed: %s", key, err)
            else:
                downloaded += 1
                tracker.add(elapsed)
            if processed % LOG_EVERY == 0 or processed == len(pending):
                remaining = max(len(pending) - processed, 0)
                elapsed_total = time.monotonic() - phase_start
                logger.info(
                    "Workspace: %d/%d done (%d downloaded, %d skipped, %d failed). Elapsed %s. ETA %s",
                    processed,
                    len(pending),
                    downloaded,
                    skipped,
                    failed,
                    _fmt_duration(elapsed_total),
                    tracker.format(remaining),
                )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(_download_one, client, cfg.bucket, key, local_path)
                for key, local_path in pending
            ]
            for future in as_completed(futures):
                key, status, err, elapsed = future.result()
                processed += 1
                if status == "failed":
                    failed += 1
                    logger.warning("Workspace %s failed: %s", key, err)
                else:
                    downloaded += 1
                    tracker.add(elapsed)
                if processed % LOG_EVERY == 0 or processed == len(pending):
                    remaining = max(len(pending) - processed, 0)
                    elapsed_total = time.monotonic() - phase_start
                    logger.info(
                        "Workspace: %d/%d done (%d downloaded, %d skipped, %d failed). Elapsed %s. ETA %s",
                        processed,
                        len(pending),
                        downloaded,
                        skipped,
                        failed,
                        _fmt_duration(elapsed_total),
                        tracker.format(remaining),
                    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore .codex/.vscode-server from Cloudflare R2")
    parser.add_argument("--workers", type=int, default=None, help="Worker threads for restore")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Force download even when local file looks up to date",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    _setup_logging(args.verbose)
    cfg = load_r2_config()
    if not cfg:
        raise SystemExit("R2 config not found; set AF_R2_* or config/r2_public.json")

    workers = args.workers
    if workers is None:
        workers = int(os.getenv("AF_R2_WORKSPACE_WORKERS", "4") or 4)
    workers = max(1, workers)
    logger.info("Using %d worker(s) for workspace restore.", workers)
    logger.info("Workspace root: %s", _workspace_root())
    logger.info("Prefix: %s", cfg.prefix_workspace)
    restore_workspace(cfg, workers, args.overwrite)
    logger.info("Workspace restore completed.")


if __name__ == "__main__":
    main()
