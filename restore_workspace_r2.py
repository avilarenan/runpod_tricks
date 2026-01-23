from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
import logging
import os
import shutil
import stat
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
    paths.append(workspace / "secrets_bundle.json")
    return paths


def _load_public_config() -> dict:
    for path in _public_config_paths():
        try:
            if path.exists():
                return json.loads(path.read_text())
        except Exception:
            continue
    return {}


def _normalize_secret_config(cfg: dict) -> dict:
    if not isinstance(cfg, dict):
        return {}
    normalized = dict(cfg)
    aliases = {
        "access_key": (
            "access_key",
            "AF_R2_ACCESS_KEY",
            "AF_R2_ACCESS_KEY_ID",
            "R2_ACCESS_KEY",
            "R2_ACCESS_KEY_ID",
        ),
        "secret_key": (
            "secret_key",
            "AF_R2_SECRET_KEY",
            "AF_R2_SECRET_ACCESS_KEY",
            "AF_R2_SECRET_KEY_ID",
            "R2_SECRET_KEY",
            "R2_SECRET_ACCESS_KEY",
        ),
        "token": ("token", "AF_R2_TOKEN", "R2_TOKEN"),
        "account_id": ("account_id", "AF_R2_ACCOUNT_ID", "R2_ACCOUNT_ID"),
        "bucket": ("bucket", "AF_R2_BUCKET", "R2_BUCKET"),
        "endpoint": ("endpoint", "AF_R2_ENDPOINT", "R2_ENDPOINT"),
        "prefix_workspace": ("prefix_workspace", "AF_R2_PREFIX_WORKSPACE", "R2_PREFIX_WORKSPACE"),
    }
    for dest, keys in aliases.items():
        if normalized.get(dest):
            continue
        for key in keys:
            value = cfg.get(key)
            if value:
                normalized[dest] = value
                break
    return normalized


def _load_secret_config() -> dict:
    for path in _secret_config_paths():
        try:
            if path.exists():
                return _normalize_secret_config(json.loads(path.read_text()))
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
    merged_cfg = {**cfg, **secret_cfg}
    account_id = _env_first("AF_R2_ACCOUNT_ID", "R2_ACCOUNT_ID") or merged_cfg.get("account_id") or ""
    bucket = _env_first("AF_R2_BUCKET", "R2_BUCKET") or merged_cfg.get("bucket") or ""
    endpoint = _env_first("AF_R2_ENDPOINT", "R2_ENDPOINT") or merged_cfg.get("endpoint") or ""
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
        or merged_cfg.get("prefix_workspace")
        or "workspace/backups"
    )

    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"

    if not (bucket and endpoint and access_key and secret_key):
        return None
    
    ret = R2Config(
        account_id=account_id,
        bucket=bucket,
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        token=token,
        prefix_workspace=prefix_workspace,
    )

    logger.debug(
        "R2 config loaded for bucket=%s endpoint=%s prefix=%s",
        ret.bucket,
        ret.endpoint,
        ret.prefix_workspace,
    )

    return ret


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


def _setup_logging(verbose: bool, log_file: Optional[Path], no_stdout: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    handlers: list[logging.Handler] = []
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        file_handler.setLevel(level)
        handlers.append(file_handler)
    if not no_stdout:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        stream_handler.setLevel(level)
        handlers.append(stream_handler)
    if not handlers:
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=level, handlers=handlers)


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


def _is_exec_candidate(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            head = handle.read(4)
    except OSError:
        return False
    if head.startswith(b"#!"):
        return True
    return head == b"\x7fELF"


def _fix_vscode_exec_bits() -> int:
    root = _workspace_root() / ".vscode-server"
    if not root.exists():
        return 0
    updated = 0
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            path = Path(dirpath) / name
            try:
                st = os.lstat(path)
            except OSError:
                continue
            if stat.S_ISLNK(st.st_mode):
                continue
            if st.st_mode & stat.S_IXUSR:
                continue
            if not _is_exec_candidate(path):
                continue
            try:
                os.chmod(path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                updated += 1
            except OSError:
                continue
    return updated


def _on_rmtree_error(func, path, exc_info) -> None:
    try:
        os.chmod(path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
        func(path)
    except Exception:
        pass


def _remove_workspace_dirs() -> None:
    root = _workspace_root()
    for name in (".vscode-server", ".codex"):
        target = root / name
        if not target.exists() and not target.is_symlink():
            continue
        if target.is_symlink():
            try:
                target.unlink()
            except OSError:
                continue
        else:
            shutil.rmtree(target, onerror=_on_rmtree_error)


def _ensure_codex_home_link() -> Optional[Path]:
    workspace_codex = _workspace_root() / ".codex"
    workspace_codex.mkdir(parents=True, exist_ok=True)
    home_codex = Path.home() / ".codex"
    if home_codex.is_symlink():
        try:
            if home_codex.resolve() == workspace_codex.resolve():
                return home_codex
        except OSError:
            pass
        try:
            home_codex.unlink()
        except OSError:
            return None
    elif home_codex.exists():
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        backup = home_codex.with_name(f".codex.bak-{timestamp}")
        try:
            home_codex.rename(backup)
        except OSError:
            return None
    try:
        home_codex.symlink_to(workspace_codex)
    except OSError:
        return None
    return home_codex


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
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing .codex/.vscode-server before restoring",
    )
    parser.add_argument(
        "--link-codex-home",
        action="store_true",
        help="Symlink ~/.codex to /workspace/.codex after restore",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Write logs to this file",
    )
    parser.add_argument(
        "--no-stdout",
        action="store_true",
        help="Suppress stdout logging (useful with --log-file)",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    log_file = Path(args.log_file).expanduser() if args.log_file else None
    _setup_logging(args.verbose, log_file, args.no_stdout)
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
        raise SystemExit("R2 config not found; set AF_R2_* or config/r2_public.json or secrets_bundle.json")

    workers = args.workers
    if workers is None:
        workers = int(os.getenv("AF_R2_WORKSPACE_WORKERS", "4") or 4)
    workers = max(1, workers)
    logger.info("Using %d worker(s) for workspace restore.", workers)
    logger.info("Workspace root: %s", _workspace_root())
    logger.info("Prefix: %s", cfg.prefix_workspace)
    if args.clean:
        logger.info("Cleaning existing .codex/.vscode-server before restore.")
        _remove_workspace_dirs()
    restore_workspace(cfg, workers, args.overwrite)
    fixed = _fix_vscode_exec_bits()
    if fixed:
        logger.info("Fixed exec bits for %d file(s) under .vscode-server.", fixed)
    if args.link_codex_home:
        linked = _ensure_codex_home_link()
        if linked:
            logger.info("Linked %s -> %s", linked, _workspace_root() / ".codex")
        else:
            logger.warning("Failed to link ~/.codex to /workspace/.codex.")
    logger.info("Workspace restore completed.")


if __name__ == "__main__":
    main()
