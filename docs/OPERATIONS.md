# Operations

This is the runbook for the production deployment: deploys, the first v2 deploy, rollback,
backups, logs, health checks and maintenance. The commands assume the production layout:

| | |
|---|---|
| Deploy directory | `~/ynab-bot` on the server (`/home/ubuntu/ynab-bot`), compose project `ynab-bot` |
| Container | `revolut-ynab-bot`, image `ghcr.io/cyrill-meier/ynab_helper:latest`, no `command:`, `user:` or `healthcheck:` override, `TZ=Europe/Zurich`. The `docker compose … revolut-ynab-bot` commands below use the service name of the reference compose file; adjust them if the server's compose file names the service differently. |
| Mounts | `bot_data/` → `/app/bot_data` (read-write), `bot.env` → `/app/.env` (read-only, 0600 root) |
| Proxy | the shared `caddy` container: `bot.netcope.ch` → `revolut-ynab-bot:8080` |
| Updates | the shared `watchtower` container pulls `:latest` every 5 minutes |

The server's `docker-compose.yml` and `Caddyfile` also serve other sites. **The repository
never changes them**, and nothing in v2 requires a change there. Never replace them with the
reference files from this repo. Edit them by hand, line by line, if you want the
[optional improvements](#optional-host-side-improvements).

## Contents

- [Deploy flow](#deploy-flow)
- [The first v2 deploy](#the-first-v2-deploy)
- [Rollback](#rollback)
- [Backups and restore](#backups-and-restore)
- [Logs](#logs)
- [Health check](#health-check)
- [Changing bot.env](#changing-botenv)
- [Running commands in the container](#running-commands-in-the-container)
- [Optional host-side improvements](#optional-host-side-improvements)
- [CSS rebuild](#css-rebuild)
- [Dependency updates](#dependency-updates)

---

## Deploy flow

```
branch → PR → CI (lint, tests, CSS drift, pip-audit, image build + selfcheck + container smoke test)
       → merge to main → CI again → publish job pushes :<sha7>, then :latest to GHCR
       → Watchtower (≤ 5 min) pulls :latest and recreates revolut-ynab-bot → startup DM
```

- **Only tested images are published.** The `image` job builds the image once, checks it
  (`selfcheck`, `scripts/ci_smoke.sh`: privilege drop, entry points, SIGTERM within 10 s) and
  hands exactly that image to `publish` as a checksummed archive. `publish` runs only for
  pushes to `main`, and it is the only job with `packages: write`. Pull requests and manual
  runs (`workflow_dispatch`) build and check but never publish.
- **Documentation-only pushes do not deploy.** A push to `main` that only touches `**/*.md`,
  `docs/**`, `docker-compose.yml`, `Caddyfile`, `.env.example` or `.github/dependabot.yml`
  skips CI, so production is not restarted.
- **Order.** Pushes to `main` share one queue (`deploy-main`) and publish in commit order.
  If several commits land while a run is in progress, only the newest is built, so not every
  commit gets a `:<sha7>` tag. Every deployed one does.
- **The restart.** Watchtower stops the old container (SIGTERM). The bot stops polling, gives
  running jobs 4 s, tells users whose job was cut off to resend, and exits within about 7.5 s.
  The new container then migrates the databases if needed, starts the web UI and sends
  the admin `🚀 Bot started v2.0.0 (<sha7>, <build date>) — web UI: …`.
- **Check what runs.** The first line of `/help` shows the version and commit. From the
  host: `docker exec revolut-ynab-bot printenv BUILD_SHA BUILD_DATE`.
- **Deploy now instead of waiting:**
  `cd ~/ynab-bot && docker compose pull revolut-ynab-bot && docker compose up -d revolut-ynab-bot`.

---

## The first v2 deploy

v2 runs on the existing host without any change to the server's files, but its first start
changes the data directory. Read this section before merging v2 into `main`.

### Before merging

1. **Bring 1.4.2 into the v2 branch.** The 1.4.2 hotfix is on `main` (PR #26, merge commit
   `a61a2a8`, 2026-10-17) but not in `rework/v2`, so the v2 pull request conflicts. Merge
   `main` into the branch and keep v2's side of every conflict. v2 fixes the same bugs in its
   own code, so nothing from the v1 side is needed:

   ```bash
   git fetch origin && git switch rework/v2 && git merge origin/main
   # expected conflicts: revolut_to_ynab.py, revolut_ynab_bot.py (keep v2's shims),
   # README.md (keep v2's), tests/test_import_flows.py (deleted in v2, modified in 1.4.2)
   git checkout --ours revolut_to_ynab.py revolut_ynab_bot.py README.md
   git rm tests/test_import_flows.py
   .venv/bin/pytest -q && git commit
   ```

2. **Record the rollback target.** Note the commit of the running v1 image; its
   `:<sha7>` tag is the rollback image. It should read **`a61a2a8`** (v1.4.2; `/help` shows
   the version). Older v1 images still have the data-integrity bugs that 1.4.2 fixed.

   ```bash
   docker exec revolut-ynab-bot printenv BUILD_SHA          # expected: a61a2a8
   docker pull ghcr.io/cyrill-meier/ynab_helper:a61a2a8      # the tag exists and is cached
   ```

3. **Take your own copy** of the data directory (optional: v2 backs up every database by
   itself before migrating). The databases are in WAL mode, so stop the container for the
   few seconds the copy takes. The archive holds YNAB tokens, so keep it private:

   ```bash
   docker stop revolut-ynab-bot
   sudo tar -C ~/ynab-bot -czf ~/bot_data-before-v2.tgz bot_data && sudo chmod 600 ~/bot_data-before-v2.tgz
   docker start revolut-ynab-bot
   ```

4. **Check `bot.env`.** v2 validates the web settings strictly. `WEB_UI_PUBLIC_URL` must be
   an `http://` or `https://` URL with no query or fragment, and `WEB_UI_SECRET_KEY` must have
   at least 32 characters. Otherwise the web UI stays off, the startup DM names the problem,
   and the health check reports unhealthy. No variable has to be added or renamed. Check the
   key's length without printing it (surrounding quotes count here, so subtract 2 if the
   value is quoted):

   ```bash
   sudo awk '/^WEB_UI_SECRET_KEY=/ { print length(substr($0, index($0, "=") + 1)) }' ~/ynab-bot/bot.env
   ```

5. Don't start an import around the time of the merge.

### What happens automatically on the first start

1. **Ownership changes (entrypoint, as root).** The entrypoint reads `/app/.env`, sets
   `umask 077`, and hands every entry under `bot_data/` to uid/gid **10001:10001**:
   directories 0700, files 0600. Symlinks and hard-linked files are skipped. It then drops to
   uid 10001 for good. Log line:
   `data dir /app/bot_data: N entries handed to 10001:10001, 0 already owned, …`.
   **Consequence:** on the host, `bot_data/` and `bot.log` are no longer readable by
   `ubuntu`. Use `sudo` (`sudo tail -f ~/ynab-bot/bot_data/bot.log`) or `/logs` in Telegram.
   Your manual `.bak` copies in `bot_data/` change owner too.
2. **Verified backups.** Before migrating, every database (`bot_users.db` and each
   `transactions_<id>.db`) is copied with SQLite's backup API to
   `bot_data/backups/<name>.<UTC stamp>.pre-v1.db`. Each copy is integrity-checked
   (`PRAGMA quick_check`) and has mode 0600. If a backup fails, that database is not migrated.
3. **v1 → v2 migration**, before the web UI starts:
   - `bot_users.db`: missing columns are added, the legacy state `approved` becomes
     `awaiting_token`, and expired web sessions are deleted.
   - `transactions_<id>.db`: the new tables (`import_ledger`, the YNAB mirror and
     `csv_files`) are created. The v1 importer rows are copied into the ledger (identity
     `legacy`), and v1 "ghost" rows sharing one YNAB transaction are collapsed. The newest
     statement in `csv_cache/<id>/` is registered with status **`legacy`**. The v1
     `transactions` table is **not modified**.
   - Log line: `data dir /app/bot_data ready: users DB migrated, 1 transactions DB(s), 0 failure(s)`.
   - If one user's database fails, it stays at v0 (backup kept), the other users start, and
     the startup DM says `⚠ Migration of transactions_<id>.db failed: …`. If `bot_users.db`
     fails, the bot exits with code 1 and Docker restarts it in a loop with no startup DM.
     Roll back (see [Rollback](#rollback)).
4. **The web UI starts** (`web UI serving on 0.0.0.0:8080 (public https://bot.netcope.ch)`),
   then polling resumes from the saved `telegram_offset`, and the admin gets the startup DM.

### What users notice once

- **Web sessions and unused sign-in buttons from v1 stop working**, because tokens are now
  stored as HMACs keyed with `WEB_UI_SECRET_KEY`. Send `/login` for a new button.
- **`/reconcile` and `/dedupe` ask for a fresh upload first.** The statement cached by v1 has
  status `legacy`, because its import status is unknown, so both answer "The newest statement
  (…) comes from the previous version of the bot … Upload the latest statement again".
  Send the current Revolut statement once. That first v2 import links the v1 rows to YNAB
  and fills in their timestamps. Check its summary: it should create only rows that are
  genuinely new, and report the rest as unchanged.
- The first sync after the deploy fetches the whole budget once (one larger request). Later
  syncs are deltas.
- Old `/settings` buttons in the chat keep working. Old `/dedupe` pickers say "This list has
  expired" and lose their buttons.
- Pending or denied strangers now get at most one reply per 5 minutes.

### Verify

1. **Startup DM**: `🚀 Bot started v2.0.0 (<sha7>, …) — web UI: enabled at https://bot.netcope.ch`,
   then `Ready users: 1`, and no `⚠ Migration … failed` line.
2. **`/status`**: balances, `Last statement: … status legacy`, ledger counts, last sync.
3. **Container logs**:

   ```bash
   docker logs --tail 100 revolut-ynab-bot
   docker logs revolut-ynab-bot 2>&1 | grep -E 'handed to|backed up|ready:|web UI|starting'
   ```

4. **Health**: `healthy` within about two minutes (60 s start period, then one check per minute):

   ```bash
   docker inspect --format '{{.State.Health.Status}}' revolut-ynab-bot
   ```

5. **Backups and ownership**:

   ```bash
   sudo ls -l ~/ynab-bot/bot_data/backups/
   sudo ls -ln ~/ynab-bot/bot_data | head
   ```

6. **Web UI**: `/login`, then the button, then the dashboard shows the Revolut account.
7. **Import**: send the latest statement and read the summary, then run `/reconcile` and
   check the preview.
8. Copy the `pre-v1` backups out of `bot_data/backups/` (see
   [Backups and restore](#backups-and-restore)), because later backups eventually rotate them
   out.

---

## Rollback

There are two ways back: run an older image, or restore a backup. They can be combined.

### Pin an older image

Watchtower does not read compose files. It recreates a container from that container's own
image reference, so a container running `:<sha7>` stays on `:<sha7>`. The pin still belongs
in the server's compose file, because any later `docker compose up -d` (including the
"deploy now" command above) recreates the container from the image named there, which is
`:latest`:

```bash
cd ~/ynab-bot
sudo cp docker-compose.yml docker-compose.yml.before-pin
# edit the revolut-ynab-bot service:  image: ghcr.io/cyrill-meier/ynab_helper:<sha7>
docker compose pull revolut-ynab-bot
docker compose up -d revolut-ynab-bot
docker exec revolut-ynab-bot printenv BUILD_SHA     # confirms the pinned commit
```

A `:<sha7>` tag never moves, so Watchtower finds nothing to update and leaves the pinned
container alone. To resume automatic deploys, set the image back to `:latest` once `main`
holds a fixed version, and run `docker compose up -d revolut-ynab-bot`. If you prefer, stop
Watchtower meanwhile (`docker stop watchtower`), but it also updates the other sites on this
host.

`<sha7>` is the commit you recorded [before the first v2 deploy](#before-merging) (v1.4.2:
`a61a2a8`), or any
earlier v2 commit (`git log --oneline main`; the publish step lists the tags it pushed). For
a v2 → v2 rollback you can instead re-run an older CI run of `main` with "Re-run all jobs".
It rebuilds that commit and publishes it as `:latest`.

**Rolling back to the v1 image after v2 has run** works, because v2 left v1's tables in
place. Some things to know:

- v1 runs as root and reads the 10001-owned files without problems. Files it creates are
  root-owned until the next v2 start takes them over again.
- Web sessions created by v2 are not valid in v1 (different token hashing), so send
  `/login` again.
- v1 does not see v2-only data. Imports done by v2 are in `import_ledger`, not in v1's
  `transactions` table, so the first v1 import posts those rows again. YNAB rejects them as
  duplicate import ids, and v1.4.2 reconciles from YNAB's current state. Rows v2 created
  under a renumbered import id (`…:2` instead of `…:1`) can show up as duplicates: check the
  first v1 import summary and `/dedupe`.
- Rolling forward to v2 again needs no migration (the files are already at schema
  version 1). For transactions v1 created in the meantime, v2 adopts them instead of creating
  them again, as long as their import id, amount and date still match the statement row.
  Check the summary of the first import after rolling forward.

### Restore a backup

Use this when a database is damaged, or to undo a migration that did run (a `pre-v1` backup
puts the file back to v1's schema). A migration that *failed* needs no restore: it rolls
back, the file stays at v0 unchanged, and its backup is an identical copy. YNAB itself is
never affected by a restore; you only lose local state written since the backup (ledger
entries, statement records, web sessions, and `/setup` changes for `bot_users.db`). The next
sync and import rebuild the links from YNAB.

```bash
docker stop revolut-ynab-bot
cd ~/ynab-bot
sudo ls -l bot_data/backups/                                   # pick the file
sudo mkdir -p ~/ynab-bot/replaced-$(date +%F)
sudo mv bot_data/transactions_<id>.db bot_data/transactions_<id>.db-wal bot_data/transactions_<id>.db-shm \
        ~/ynab-bot/replaced-$(date +%F)/ 2>/dev/null
sudo cp bot_data/backups/transactions_<id>.db.<stamp>.pre-v1.db bot_data/transactions_<id>.db
docker start revolut-ynab-bot
```

Always move the `-wal` and `-shm` files along with the database, and never copy a database
while the container is running. The same steps work for `bot_users.db`. A restored `pre-v1`
file is migrated again on the next v2 start, with a new backup, and the newest statement in
`csv_cache/<id>/` is registered as `legacy` again: upload the current statement once before
`/reconcile` or `/dedupe`. The entrypoint takes ownership of the restored, root-owned file.

---

## Backups and restore

- **Automatic**: before every schema migration, each database is backed up to
  `bot_data/backups/<db name>.<UTC stamp>.pre-v<N>.db`. The copy is made with SQLite's backup
  API (consistent while the bot runs) and integrity-checked. The **newest 10 per database are
  kept, counting every tag**, so manual backups made the same way eventually rotate out the
  `pre-v1` ones.
- **Keep the `pre-v1` backups** somewhere else after the first deploy. `bot_data/` is 0700
  and owned by 10001, so your own shell cannot expand a `*` in it; let root's shell do it:

  ```bash
  sudo sh -c 'install -d -m 700 /home/ubuntu/ynab-bot-backups && cp -a /home/ubuntu/ynab-bot/bot_data/backups/*.pre-v1.db /home/ubuntu/ynab-bot-backups/'
  ```

- **Manual, consistent backup while the bot runs**, as the app user so the files stay
  10001-owned:

  ```bash
  docker exec -u 10001:10001 revolut-ynab-bot python3 -c "
  from pathlib import Path
  from ynab_helper.storage import backup_db
  d = Path('/app/bot_data')
  for db in [d / 'bot_users.db', *sorted(d.glob('transactions_*.db'))]:
      print(backup_db(db, d / 'backups', tag='manual'))"
  ```

- **Whole directory, off the host**: the live databases use WAL, so a copy taken while the
  bot runs can be inconsistent. Stop the container for the copy (or use only the consistent
  copies in `backups/` from the archive). The archive contains YNAB tokens, so make it
  private:

  ```bash
  docker stop revolut-ynab-bot
  sudo tar -C ~/ynab-bot -czf ~/bot_data-$(date +%F).tgz bot_data && sudo chmod 600 ~/bot_data-$(date +%F).tgz
  docker start revolut-ynab-bot
  ```
- **Restore**: see [Restore a backup](#restore-a-backup).

---

## Logs

| Where | What |
|---|---|
| `docker logs revolut-ynab-bot` | Everything the process writes to stderr, including the entrypoint's lines before the log file is opened. The production compose sets no log limits (see [optional improvements](#optional-host-side-improvements)). |
| `bot_data/bot.log` | `LOG_FILE=/app/bot_data/bot.log`. Rotated at 5 MB, with `bot.log.1` … `bot.log.5` kept (about 30 MB at most). On the host, read it with `sudo` (the file belongs to uid 10001, mode 0600): `sudo tail -f ~/ynab-bot/bot_data/bot.log`. |
| Telegram `/logs [N]` | The admin gets the last N lines (default 500, at most 100,000, across rotated files) as a document. |

- Audit trail: one INFO line per YNAB write under the logger `ynab_helper.audit` (created,
  updated, deleted, with date, amount and payee). These lines are written even with
  `LOG_LEVEL=WARNING`. Find them with
  `sudo sh -c 'grep ynab_helper.audit /home/ubuntu/ynab-bot/bot_data/bot.log*'` (root's shell
  has to expand the `*`, because `bot_data/` is closed to `ubuntu`).
- The secrets from `bot.env` (`TELEGRAM_BOT_TOKEN`, `YNAB_TOKEN`, `CRYPTO_BTC_XPUB`,
  `ANKR_API_KEY`, `WEB_UI_SECRET_KEY`) are masked in every line. The per-user YNAB tokens
  and xpubs stored in `bot_users.db` are not registered with the log redactor; the YNAB and
  network clients only keep tokens out of their own error messages. Treat `bot.log` as
  sensitive.
- For more detail, set `LOG_LEVEL=DEBUG` in `bot.env` and run
  `docker restart revolut-ynab-bot` (see [Changing bot.env](#changing-botenv)).
- A CLI run through `docker exec` reads the same `bot.env`, so it logs to the same `bot.log`.
  Two processes writing that file can lose lines when it rotates.

---

## Health check

The image defines
`HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 CMD python3 -m ynab_helper.healthcheck`,
so it applies without any compose change. Each check prints one line, `OK: …` or
`UNHEALTHY: …`.

- **Heartbeat**: `bot_data/.heartbeat` must be younger than 180 s (`HEALTHCHECK_MAX_AGE`).
  The poll loop touches it on every iteration, and a supervisor thread every 15 s while the
  loop makes progress. A long poll counts as progress; a handler stuck for more than 120 s
  does not. So a hung bot turns unhealthy.
- **Web**: when `WEB_UI_ENABLED` is on, `GET http://127.0.0.1:8080/health` must answer 200
  within 3 s. **A web UI that is enabled but misconfigured (so it never started) reports
  unhealthy on purpose**: the problem shows up in `docker ps` instead of hiding behind a
  running bot. Fix `bot.env`, or set `WEB_UI_ENABLED=0`, then run
  `docker restart revolut-ynab-bot`.
- Three failures in a row (about 3 minutes) mark the container `unhealthy`. Docker does
  **not** restart it because of that. Look at the logs, then run
  `docker restart revolut-ynab-bot`.

```bash
docker inspect --format '{{.State.Health.Status}}' revolut-ynab-bot
docker inspect --format '{{range .State.Health.Log}}{{.Output}}{{end}}' revolut-ynab-bot
docker exec revolut-ynab-bot python3 -m ynab_helper.healthcheck      # run one check now
```

---

## Changing bot.env

The bot reads `bot.env` once, when it starts. After an edit, run
`docker restart revolut-ynab-bot`. `docker compose up -d` is not enough: `bot.env` is a
mounted file (`./bot.env:/app/.env:ro`), not an `env_file:`, so Compose sees no change and
leaves the container running with the old settings.

`bot.env` and the `Caddyfile` are single-file bind mounts. An editor or `sed -i` that saves
by replacing the file gives it a new inode, and a *running* container keeps seeing the old
copy. A restart mounts the new file, but a `caddy reload` or a `docker exec` command in the
running container does not. Edit in place (for example with `sudo nano`), and for Caddy
check what the container sees before reloading:
`docker exec caddy cat /etc/caddy/Caddyfile`. If it is stale, `docker restart caddy`
(briefly interrupts the other sites).

---

## Running commands in the container

`docker exec` runs as root and bypasses the entrypoint. The documented commands handle that
themselves: the `revolut_to_ynab.py` shim and `python3 -m ynab_helper.container …` load
`/app/.env`, take ownership of `bot_data/` and drop to uid 10001 before doing anything. So
they never leave root-owned files behind.

```bash
docker exec revolut-ynab-bot python3 revolut_to_ynab.py --db-stats
docker exec revolut-ynab-bot python3 revolut_to_ynab.py --list-budgets
docker exec revolut-ynab-bot python3 -m ynab_helper.container cli --sync
docker exec revolut-ynab-bot python3 -m ynab_helper.container selfcheck   # imports, RIPEMD-160, writable data dir
docker exec -u 10001:10001 revolut-ynab-bot python3 -c "…"                # ad-hoc Python as the app user
```

- Do **not** use `docker exec … python3 -m ynab_helper …`. That form skips the privilege drop,
  so it runs as root and warns about it.
- Do **not** run the `revolut_ynab_bot.py` shim in the live container. It drops privileges
  too, but it starts a second bot: a second Telegram poller (Telegram answers 409 Conflict)
  and a web server that cannot bind port 8080.
- The CLI inside the container uses its own database, `/app/bot_data/.revolut_to_ynab.db`,
  and `YNAB_*` from `bot.env`. It does not see the bot users' ledgers or statements, so
  `--reconcile` there fails with "No Revolut statement has been imported…". Point it at a
  user's database with `--db-path /app/bot_data/transactions_<id>.db` (this only fits when
  `YNAB_*` in `bot.env` name that user's budget and account). Read-only modes such as
  `--db-stats` are harmless there. Mutating modes do not share the bot's per-user lock
  (separate process), so don't run them while the bot might be importing for that user:

  ```bash
  docker exec -it revolut-ynab-bot python3 revolut_to_ynab.py --reconcile \
      --db-path /app/bot_data/transactions_<id>.db       # -it for the confirmation prompt
  ```
- The image has no `sqlite3` binary. To inspect a database, use Python as the app user,
  read-only:

  ```bash
  docker exec -u 10001:10001 revolut-ynab-bot python3 -c "
  import sqlite3
  c = sqlite3.connect('file:/app/bot_data/bot_users.db?mode=ro', uri=True)
  for r in c.execute('SELECT telegram_id, state, budget_name, account_name FROM users'): print(r)"
  ```

---

## Optional host-side improvements

None of these are needed for v2, and **the repository does not apply them**. The server's
files are shared with other sites. Change them by hand, one at a time, validate, and keep a
copy of the original. The reference [`Caddyfile`](../Caddyfile) and
[`docker-compose.yml`](../docker-compose.yml) show each change in context.

| Where | Change | Why |
|---|---|---|
| Caddyfile, `bot.netcope.ch` block | Replace the `Content-Security-Policy "…"` directive with its own line `header ?Content-Security-Policy "…"` (the `?` sets it only when the app sent none), or remove it. If it sits inside a `header { … }` block, move it out of the block rather than adding `?` in place: inside one block, `?` fields are only filled when *all* of them are missing. | The app sends a stricter CSP (`'self'` only, no `'unsafe-inline'`, no CDN). Depending on how the block is written, Caddy's v1-era policy replaces it or is sent alongside it. v2 works with the current policy, but only the app's own policy gives the full protection. `cdn.tailwindcss.com` and `'unsafe-inline'` are no longer needed. |
| Caddyfile, same block | `request_body { max_size 26MiB }` | Stops oversized bodies at the edge. The app enforces the same limit itself (26 MiB for `/api/upload`). The reference uses `MiB` because Caddy's `MB` means 10⁶ bytes and could reject an upload the app accepts. |
| Caddyfile, same block | `-Via` in the `header { … }` block and a `Permissions-Policy` header, if not there yet | Pentest items L-3 and I-2. |
| compose, `revolut-ynab-bot` | `logging: {driver: local, options: {max-size: "10m", max-file: "3"}}` | `docker logs` output is otherwise never rotated. |
| compose, `revolut-ynab-bot` | `stop_grace_period: 20s` | The bot exits within about 7.5 s, inside Docker's default 10 s. This only adds headroom. |
| compose, `revolut-ynab-bot` | `security_opt: [no-new-privileges:true]` | The entrypoint needs no setuid binaries (tested on first and later starts). |

Apply and check. Edit the `Caddyfile` in place, and make sure the container sees the new
version before reloading (see [Changing bot.env](#changing-botenv) for why):

```bash
docker exec caddy cat /etc/caddy/Caddyfile                  # shows your edit?
docker exec caddy caddy validate --config /etc/caddy/Caddyfile
docker exec caddy caddy reload --config /etc/caddy/Caddyfile
cd ~/ynab-bot && docker compose up -d revolut-ynab-bot      # after a compose change
curl -sS -o /dev/null -D - https://bot.netcope.ch/ | grep -iE 'content-security|via|permissions|strict-transport'
```

---

## CSS rebuild

The web UI's stylesheet `ynab_helper/web/static/app.css` is built with Tailwind and
committed. After you change classes in templates or JavaScript:

```bash
bash scripts/build_css.sh            # downloads the pinned Tailwind CLI v3.4.17 once (sha256-checked) into .cache/
bash scripts/build_css.sh --check    # what CI runs; fails if app.css is stale
```

Commit the rebuilt `app.css` with the change. `--offline` (or `TAILWIND_OFFLINE=1`) never
downloads anything.

---

## Dependency updates

Runtime dependencies are listed in `requirements.in` (fastapi, uvicorn, jinja2,
pycryptodome); development ones in `requirements-dev.in`. Both are compiled into
hash-locked files:

```bash
uv pip compile --generate-hashes --python-version 3.12 --universal requirements.in -o requirements.txt
uv pip compile --generate-hashes --python-version 3.12 --universal requirements-dev.in -o requirements-dev.txt
# a single package:  add --upgrade-package <name>;  everything:  add --upgrade
uv pip install --python .venv/bin/python --require-hashes -r requirements-dev.txt
.venv/bin/pytest -q
.venv/bin/pip-audit --requirement requirements.txt --require-hashes --disable-pip
```

The image installs `requirements.txt` with `--require-hashes --only-binary=:all:`, so every
package must have a wheel and match a hash. The base image is pinned by digest in the
`Dockerfile`.

**Dependabot** (`.github/dependabot.yml`) opens grouped PRs every Monday at 06:00
(Europe/Zurich). Version updates wait for a cooldown (3–7 days); security updates do not.

| Ecosystem | Updates |
|---|---|
| `uv` | `requirements*.txt`: re-runs `uv pip compile` from the file header, so transitive pins and hashes stay consistent. **Unconfirmed**: the repository has no `uv.lock` and no dependencies in `pyproject.toml`, and it is not verified that Dependabot's `uv` support picks up compiled requirements files alone. Check that the first Monday run opens a PR (or reports an error under *Insights → Dependency graph → Dependabot*); otherwise update with the commands above. |
| `docker` | The digest of `python:3.12-slim-bookworm` in the `Dockerfile` (minor and major Python bumps are ignored). |
| `docker-compose` | Caddy and Watchtower tags in the reference `docker-compose.yml` (not the server's). |
| `github-actions` | Action commit SHAs in `.github/workflows/`. |

Dependabot PRs run the full CI but never get a token that can publish. **Merging one
deploys it**, so read the changelog of anything that is not a patch release.
