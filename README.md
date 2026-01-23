# runpod_tricks

Utilities for Runpod workflows and workspace management.

## Workspace sync/restore (.codex / .vscode-server)
Sync editor/runtime state to Cloudflare R2:
```bash
python sync_workspace_r2.py
```

Restore the workspace from R2:
```bash
python restore_workspace_r2.py
```
The restore script also repairs missing executable bits for files under `.vscode-server`.

Useful restore flags:
- `--clean`: remove existing `.codex`/`.vscode-server` before download.
- `--link-codex-home`: symlink `~/.codex` -> `/workspace/.codex`.
- `--log-file /path/to/log` and `--no-stdout`: capture logs without UI spam.

Environment variables:
- `AF_WORKSPACE_ROOT` (optional): workspace root (default: `/workspace`)
- `AF_R2_PUBLIC_CONFIG` (optional): non-sensitive R2 config path (default: `/workspace/AlphaForecasting/config/r2_public.json`)
- `AF_R2_PREFIX_WORKSPACE` (optional): workspace prefix (default: `workspace/backups`)
- `AF_R2_WORKSPACE_WORKERS` (optional): worker threads (default: `4`)
- `AF_R2_ACCOUNT_ID`, `AF_R2_BUCKET`, `AF_R2_ENDPOINT` (optional): R2 connection info (non-sensitive)
- `AF_R2_ACCESS_KEY`, `AF_R2_SECRET_KEY`, `AF_R2_TOKEN` (optional): R2 secrets (sensitive)
- `AF_R2_ALLOW_FILE_SECRETS` (optional): allow reading secrets from `.secrets/r2.json` (`1`/`0`, default: `1`)

Secrets bundle support:
- If `/workspace/secrets_bundle.json` exists, `sync_workspace_r2.py` and `restore_workspace_r2.py` will also accept `AF_R2_ACCESS_KEY`, `AF_R2_SECRET_KEY`, and `AF_R2_TOKEN` from that file.

`fresh_install.sh` notes:
- Loads secrets from `secrets_bundle.json` when present.
- If `RESTORE_WORKSPACE=auto` (default) and the secrets bundle exists, it runs the restore with `--clean`, `--link-codex-home`, and `--log-file /workspace/restore_workspace_r2.log`.
