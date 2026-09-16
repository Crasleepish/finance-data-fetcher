---
name: bigquant-stock-status-sync
description: Project-specific workflow that browser-exports BigQuant cn_stock_status via the authenticated Windows Chrome, downloads and verifies the Parquet export locally, then atomically overwrites stock_hist_unadj.is_st / is_suspend. Use when asked to sync BigQuant stock status, stop/risk-warning/suspension (ST / 停牌) flags, export or refresh cn_stock_status, or update stock_hist_unadj status columns.
compatibility: opencode
---

# BigQuant Stock Status Sync

Project-specific to `finance-data-fetcher`. It exports `cn_stock_status` from BigQuant
AIStudio through the authenticated Windows Chrome session, verifies the Parquet export
locally, then atomically overwrites `stock_hist_unadj.is_st` and `is_suspend` in the
configured PostgreSQL database.

Write scope is exactly those two columns. No schema changes, no row inserts, no row deletes.

## Required skill loads

Load in this order before any browser operation:

1. `browser-policy` — CDP endpoint, `win-chrome` session, ownership/detach rules.
2. `playwright-cli` — Playwright CLI command syntax and evaluation workflow.

## Step 0 — confirm inputs

Confirm all of the following with the user before starting:

- `START_DATE` and `END_DATE` (inclusive; export partitions by calendar quarter).
- Local output directory inside this repo; default `bigquant/cn_stock_status/`.
- Overwrite semantics (defaults below) and that `apply` mutates production data.

Default semantics (project-established):

- Matched target rows: `is_st = is_risk_warning` (0 → 0, 1 → 1) and
  `is_suspend = 'Y'` when `suspended = 1`, otherwise `'N'`.
- Every target key without a source match ends `is_st = NULL`, `is_suspend = 'N'`.
- Source keys absent from the target are skipped — this is an UPDATE, never an insert.

## Step 1 — browser export in AIStudio

1. Check CDP availability: `curl -fsS "${BROWSER_CDP_ENDPOINT}/json/version"`.
2. Attach if needed: `playwright-cli attach --cdp="${BROWSER_CDP_ENDPOINT}" -s=win-chrome`.
3. Navigate to `https://bigquant.com/aistudio`.
4. If the page is unauthenticated, pause and ask the user to complete login in the visible
   Windows Chrome window; resume only after the studio page is loaded. Never automate
   CAPTCHA/2FA.
5. Open a terminal: command palette (`Ctrl+Shift+P`) → `Terminal: Create New Terminal`.
   Do not use Notebook cells for the helper.
6. Click the terminal textbox by accessible-name regex `/终端 \d+/` (use `/Terminal \d+/`
   on an English locale) and insert text with `keyboard.insertText`. Prefer snapshots,
   `find`, and terminal text output for progress; do not rely on screenshots for routine
   progress.

Transfer the helper into AIStudio as base64 in one single-line remote command. The
`$(...)` must NOT appear in text typed into the remote terminal — it would be expanded
remotely, where the local file does not exist. Generate the JS runner locally so the
complete remote Python command is embedded JSON-escaped:

```bash
# 1. locally: base64 the helper (one line, wrapped by the local shell)
base64 -w0 .opencode/skills/bigquant-stock-status-sync/scripts/export_cn_stock_status.py \
  > /tmp/opencode/export.b64

# 2. locally: build the single-line remote decode command and generate the JS runner
python3 - <<'PY'
import json
from pathlib import Path

b64 = Path("/tmp/opencode/export.b64").read_text().strip()
remote_cmd = (
    "python -c \"import base64;open('/home/aiuser/work/export_cn_stock_status.py','wb')"
    f".write(base64.b64decode('{b64}'))\"\n"
)
js = f"""async page => {{
  const box = page.getByRole('textbox', {{ name: /终端 \\d+/ }}).last();
  await box.click({{ force: true }});
  await page.keyboard.insertText({json.dumps(remote_cmd)});
  await page.keyboard.press('Enter');
}}"""
Path("/tmp/opencode/insert_helper.js").write_text(js)
PY

# 3. locally: run it through playwright-cli (valid syntax)
playwright-cli -s=win-chrome run-code --filename=/tmp/opencode/insert_helper.js
```

Use `/Terminal \d+/` in the regex on an English locale. After inserting, confirm the file
exists and matches with a terminal command (`ls -l` / `md5sum` / `python -c "import
ast;ast.parse(open(...).read())"`); do not use screenshots for routine progress.

Run it detached and observe the log:

```bash
cd /home/aiuser/work && nohup python export_cn_stock_status.py \
  --start "$START_DATE" --end "$END_DATE" \
  --out-dir /home/aiuser/work/cn_stock_status_export > export.log 2>&1 &
tail -n 20 export.log
pgrep -af export_cn_stock_status
```

Success marker is the exact line `EXPORT_FINISHED` in `export.log` (failure prints
`EXPORT_FAILED` and exits nonzero). Keep polling with `tail`/`pgrep`; no arbitrary sleeps.

## Step 2 — zip and download

Zip the remote export after `EXPORT_FINISHED`:

```bash
cd /home/aiuser/work && python -c "import shutil; shutil.make_archive('cn_stock_status_export','zip','/home/aiuser/work','cn_stock_status_export')"
```

Download through the authenticated `vscode-remote-resource` route. Derive the dynamic
`/stable-<hash>/vscode-remote-resource` base from a successful request in the current
browser session — never hard-code the studio UUID or the stable hash. The base comes from
the page's own resource timing and already includes the required `tkn=...` query
parameter, so preserve it when building the URL:

```javascript
// /tmp/opencode/find_base.js — base from the page's own resource timing
async page => {
  const url = await page.evaluate(() =>
    performance.getEntriesByType('resource')
      .map(e => e.name)
      .find(n => n.includes('vscode-remote-resource')));
  if (!url) return null;
  const u = new URL(url);
  u.searchParams.delete('path'); // keep tkn= and any other auth params
  return u.toString().replace(/\?$/, '');
}
```

```bash
# 1. capture the dynamic base (keeps /stable-<hash>/... tkn=...), then generate the fetch JS locally
BASE=$(playwright-cli -s=win-chrome --raw run-code --filename=/tmp/opencode/find_base.js \
  | python3 -c "import json,sys; value=json.load(sys.stdin); print(value or '')")
test -n "$BASE" || { echo "no vscode-remote-resource base found"; exit 1; }

BASE="$BASE" python3 - <<'PY'
import json, os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qsl

parts = urlsplit(os.environ["BASE"])
query = parse_qsl(parts.query, keep_blank_values=True)
query.append(("path", "/home/aiuser/work/cn_stock_status_export.zip"))
url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))
js = f"""async page => {{
  const resp = await page.request.get({json.dumps(url)});
  if (!resp.ok()) throw new Error('download failed: ' + resp.status());
  return (await resp.body()).toString('base64');
}}"""
Path("/tmp/opencode/fetch_zip.js").write_text(js)
PY
playwright-cli -s=win-chrome --raw run-code --filename=/tmp/opencode/fetch_zip.js > /tmp/opencode/export_zip.json

# 2. --raw prints the returned base64 string as a JSON string; decode via json.load
python3 -c "import base64, json, sys; open('/tmp/opencode/cn_stock_status_export.zip','wb').write(base64.b64decode(json.load(sys.stdin)))" \
  < /tmp/opencode/export_zip.json

unzip -t /tmp/opencode/cn_stock_status_export.zip
unzip -o /tmp/opencode/cn_stock_status_export.zip -d bigquant/
```

Move the extracted files so `manifest.csv` sits beside the `*.parquet` files in the
confirmed local output directory (default `bigquant/cn_stock_status/`).

## Step 3 — local verification (before stopping AIStudio)

Run the source-only check (no database access) and require success:

```bash
uv run python .opencode/skills/bigquant-stock-status-sync/scripts/sync_stock_status.py \
  verify --source-dir bigquant/cn_stock_status
```

This enforces manifest row counts, filename/date bounds, no overlapping or duplicate
period ranges, actual per-file data min/max inside each declared interval, naïve-midnight
timestamps, instrument regex, no nulls, status values 0/1, rejects duplicate keys, and
rejects extra parquet files not listed by the manifest. It reports actual data min/max
(not just declared bounds), e.g. a declared `2005-01-01` start with actual first trade
date `2005-01-04`. Only after this passes, stop the studio.

## Step 4 — stop AIStudio

Use direct semantic targets, not stale refs:

1. Click button `管理` (exact accessible name).
2. Click menuitem `停止开发环境`.
3. Click confirm button `是`.
4. Verify through the BigQuant studio API that the response for this studio has
   `data.status == "stopped"` (poll the status request visible in the page network
   activity; wait for observable state instead of fixed sleeps).
5. `playwright-cli -s=win-chrome detach` — never `close`; the external Windows Chrome
   stays running.

## Step 5 — database sync (only after the browser is stopped)

Run inside the app image on `docker-bridge`, because the configured DB hostname may only
resolve there. Reuse the running app/compose image; never print the DB URL.

```bash
REPO=/home/crasleepish/dev/finance-data-fetcher
APP_CONTAINER=$(docker compose -f "$REPO/docker-compose.yml" ps -q finance-data-fetcher)
test -n "$APP_CONTAINER" || { echo "compose service finance-data-fetcher is not running"; exit 1; }
IMAGE=$(docker inspect --format '{{.Config.Image}}' "$APP_CONTAINER")
test -n "$IMAGE" || { echo "could not derive image from $APP_CONTAINER"; exit 1; }

run_sync() {
  docker run --rm -i --network docker-bridge \
    -v "$REPO":/work:ro \
    -e APP_CONFIG_PATH=/work/config/app.yaml \
    -e PYTHONPATH=/app/src \
    "$IMAGE" /app/.venv/bin/python \
    /work/.opencode/skills/bigquant-stock-status-sync/scripts/sync_stock_status.py "$@"
}

run_sync verify    --source-dir /work/bigquant/cn_stock_status
run_sync preflight --source-dir /work/bigquant/cn_stock_status
# inspect matched / source-absent / target-unmatched counts, then:
run_sync apply --source-dir /work/bigquant/cn_stock_status --confirm FULL_OVERWRITE
```

If the compose file or image interpreter path differs, adjust `$IMAGE` and the
`/app/.venv/bin/python` path, or use
`sh -lc 'uv run --project /app python /work/.../sync_stock_status.py "$@"' _ "$@"`.

`preflight` stages into a session TEMP table and performs no target writes. `apply`
requires the literal `--confirm FULL_OVERWRITE` and aborts on any invariant failure.

## Safety and maintenance

- Staging is a PostgreSQL TEMP table loaded by streaming COPY (per-record-batch StringIO),
  with a unique key index and ANALYZE; it vanishes when the session exits.
- The update takes `SHARE ROW EXCLUSIVE` on `stock_hist_unadj`, keeps the row count fixed,
  updates matched rows differentially, resets unmatched rows to `NULL`/`'N'`, and asserts
  all invariants inside the transaction before COMMIT; failures roll back.
- After commit the script re-verifies over a fresh connection and checks deterministic
  samples. It never runs VACUUM.
- The update rewrites a large fraction of a multi-GB table: expect significant WAL and
  table bloat, schedule a maintenance window, and run `VACUUM (ANALYZE) stock_hist_unadj`
  manually afterwards. Do not run VACUUM automatically from the sync.
