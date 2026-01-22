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

Environment variables:
- `AF_WORKSPACE_ROOT` (optional): workspace root (default: `/workspace`)
- `AF_R2_PUBLIC_CONFIG` (optional): non-sensitive R2 config path (default: `/workspace/AlphaForecasting/config/r2_public.json`)
- `AF_R2_PREFIX_WORKSPACE` (optional): workspace prefix (default: `workspace/backups`)
- `AF_R2_WORKSPACE_WORKERS` (optional): worker threads (default: `4`)
- `AF_R2_ACCOUNT_ID`, `AF_R2_BUCKET`, `AF_R2_ENDPOINT` (optional): R2 connection info (non-sensitive)
- `AF_R2_ACCESS_KEY`, `AF_R2_SECRET_KEY`, `AF_R2_TOKEN` (optional): R2 secrets (sensitive)
- `AF_R2_ALLOW_FILE_SECRETS` (optional): allow reading secrets from `.secrets/r2.json` (`1`/`0`, default: `1`)
