# Changelog

Finding ids (F01, C02, …) refer to the October 2026 code review of v1. Each confirmed finding
has a regression test named `test_<id>_…`. Pentest ids (M-1, L-2, …) refer to
[PENTEST_REPORT.md](PENTEST_REPORT.md).

## 2.0.0 — unreleased

A rework of the whole application into the `ynab_helper` package, on one service layer
shared by the bot, the web UI and the CLI. The deployment, `bot.env` variables, data
directory, Telegram commands, web URLs and YNAB import id formats all stay compatible. The
first start migrates the data in place, with backups; read
[The first v2 deploy](docs/OPERATIONS.md#the-first-v2-deploy) before deploying.

### Changes you will see

- **`/reconcile` asks before posting.** It shows the statement balance, YNAB's cleared
  balance on the statement date, the difference and any warnings, with *Post adjustment* /
  *Cancel* buttons. The web UI has a confirmation dialog, and the CLI asks. It only uses the
  newest statement, and only after that statement was imported successfully.
- **`/dedupe` asks before deleting.** *Delete* opens a confirmation view listing exactly the
  selected transactions. `/dedupe_delete 1,3-5` shows the same view instead of deleting.
  Candidates are limited to the dates of the statement's rows.
- **`/login` sends a sign-in button** instead of a link in the text. The `/auth` page signs
  you in with one click, and asks first if this browser is already signed in to another
  account. The button message is deleted after sign-in.
- **New commands**: `/cancel` (leave any setup step), `/logout_all` (sign out of the web UI
  everywhere) and, for the admin, `/revoke <id>` (sign a user out of the web UI).
- **Import replies are built from structured results**: counts, changes (field: old → new),
  removals, skipped rows with reasons, warnings and failures. If something failed: "⚠
  Partially applied — send the file again to retry".
- **`/status`** shows the working, cleared and uncleared balance, the last statement with its
  import status, ledger counts and the last sync time.
- **`/setup`** uses buttons and keeps your current setup saved until the new one is complete
  (`/cancel` restores it; other commands and the web UI wait until then). The message containing your YNAB token is deleted.
  **`/crypto_setup`** offers *Keep current*, *Skip* and *Cancel*, saves only at the end,
  and never swallows other commands.
- **The startup DM** includes the web UI status (`enabled at …`, or `disabled (reason)` /
  `failed (reason)`) and any migration problem. The version line comes from the build:
  `v2.0.0 (<sha7>, <date>)`.
- **Web UI**:
  - Self-hosted CSS, no CDN.
  - The transactions list shows what YNAB has, for all accounts of the budget.
  - The dashboard shows the working, cleared and uncleared balance with the currency.
  - Spending excludes transfers and adjustments.
  - Subscriptions look back 13 months by default and find yearly charges.
  - "Sign out everywhere" really signs out every session.
  - Upload works from the keyboard.
  - Errors are shown inline with a *Retry* button.
  - Timestamps are shown in your local time.
- **CLI**: every v1 flag still works. Flags the chosen mode ignores are reported, exit codes
  are `0`/`1`/`2`, and `--watch` waits for complete files and retries failures.
- **Admin `/logs`** reads across rotated log files.

### Fixed: data integrity

- Reconciled and cleared transactions are never downgraded, and user memos are never wiped:
  cleared only moves uncleared → cleared, and the memo is YNAB's current memo with only the
  `(pending)` marker removed. `/cleanup_pending` never sends a cleared state and skips
  uncleared rows. (F01, C02)
- Your edits in YNAB are kept: payee, category and approval are never sent on updates, and
  amount or date are written only while YNAB still holds what the importer wrote. (F33)
- The importer's ledger and the YNAB mirror are now separate tables, so a web sync can no
  longer corrupt matching. This fixes ghost rows that led to deleting a live transaction
  (F02), renamed payees that broke matching (F11), and transactions moved between accounts
  that were counted twice (F39).
- The ledger is scoped per YNAB account, so switching accounts with `/setup` no longer skips
  re-imports or deletes the other account's pendings. (F12)
- Rows are identified by their started timestamp and amount, corrected for the export's
  timezone offset. This fixes wrong matches and duplicates after a timezone shift (F34) or a
  renumbering (C01), swallowed rows at the window boundary (F10), and settlements that became
  delete + create instead of an amount update (F35).
- Failures no longer wedge imports or get recorded as applied. Every action's result is
  collected, the ledger records only what YNAB confirmed, a transaction deleted in YNAB is
  marked as such, and re-sending the statement converges. (F06, F07)
- Transactions deleted in YNAB, and rows that were reverted or superseded, are never
  resurrected. (F08)
- `--skip-pending` only stops creating pending rows; it no longer deletes pendings already in
  YNAB. (F09)
- An older statement sent again no longer reverts cleared rows to pending, deletes newer
  pendings or re-creates removed rows. (F36)
- The statement currency must match the budget currency (F14). States other than
  COMPLETED, PENDING and REVERTED (DECLINED, FAILED, …) are skipped and reported instead of
  imported as spending (F37). Non-finite, short and absurd rows are skipped with a reason
  (F85).
- Reconcile uses the newest successfully imported statement and the balance of its latest
  *completed* row, shows a preview and needs a confirmation (F04, F27).
- Crypto:
  - A missing price refuses the sync instead of valuing the holding at 0 (F05).
  - ypub, zpub and legacy xpubs are scanned with their own script types (F28).
  - xpubs are fully validated: checksum, version and curve (F48).
  - Adjustments compare against the working balance, so uncleared transfers are not counted
    twice (F49).
  - Zero used addresses with a non-zero YNAB balance refuse the sync.
- Dedupe:
  - Re-keyed and date-shifted transactions are no longer flagged as orphans (F13).
  - An old *Yes, delete* button can no longer delete a newer selection (F20).
  - Ranges are validated before they are expanded (F44).
  - Candidates come from the parsed rows' dates and need the `YNAB:` format (F46).
  - More than 200 orphans no longer fail "select all" (F104).
- `--dry-run` writes nothing to YNAB or the local database (F74). Dates are no longer left
  stale after re-dating in YNAB (F76).
- An upload is fully validated before it is stored, the last good statement is never pruned,
  and the statement cache is shared safely between the bot and the web UI (F84, F83).

### Fixed: security

- Every button press goes through the authorization gate, as every message already did. The
  bot ignores group chats, and `/deny` and `/setup` revoke web sessions and sign-in links.
  The web UI also checks the user's state on every request. (F03, F56, F18)
- The upload name is never used as a path; files get server-generated names (F19, pentest
  L-2).
- The web UI enforces body limits (26 MiB upload, 64 KiB API, 16 KiB otherwise) before
  routing and authentication, and checks auth and CSRF before reading any body (F21).
  `python-multipart` is gone, because uploads are the raw CSV body (F22).
- Tailwind is precompiled and self-hosted, and the app sends a strict CSP without
  `'unsafe-inline'` (F23, pentest I-1).
- Output is HTML with every value escaped, and the `/login` link can no longer be corrupted
  (F91, F96, F25).
- Web sessions:
  - Tokens and CSRF are HMACs keyed with `WEB_UI_SECRET_KEY` (F89).
  - Login tokens are consumed atomically by a POST, never by the GET (F86, F88).
  - A real sign-out everywhere, and CSRF on logout (F61).
  - The `/auth` rate limiter evicts old entries and is keyed per client and IPv6 /64 (F87).
  - Security headers and `Cache-Control: no-store` on every page (F90).
  - The IP allowlist fails closed (F59).
- The container drops to uid 10001 after fixing `bot_data` ownership (F70). Files are 0600,
  messages containing a YNAB token are deleted (F57), the secrets from `bot.env` are masked
  in every log line, and the YNAB and network clients keep tokens out of their error
  messages (F92). Per-user tokens from `bot_users.db` are not registered with the log
  redactor.
- IBKR TLS verification is off only for localhost (F80).
- CI: tests gate every publish (F31). Manual runs never publish (F71). Every code change is
  deployed, not only changes under the old path filter; only documentation-only pushes are
  skipped (F68). Pushes to `main` publish in commit order, so an older build can no longer
  overwrite a newer `:latest` (F109). Actions are pinned by commit SHA, and only the publish
  job can write packages (F115). Dependencies are hash-locked and pip-audited (F107, F73).
- Pentest: the transactions search filter works (M-1). `/api/dedupe/delete` takes at most
  200 ids, and only ids from your own scan (L-1). Bad uploads get 4xx (L-4), and errors are
  generic `{error, message}` (I-4).

### Fixed: reliability

- Library code no longer prints, so output is never mixed between users through `sys.stdout`
  (F15). Import summaries are rendered from results, not parsed from text (F47, F29).
- Per-user locks shared by the bot and the web UI (F16). Slow bot work runs on worker threads
  (F43), and web uploads run off the event loop (F41).
- YNAB:
  - 429 and 5xx are handled, with retries only where they are safe.
  - Updates use bulk PATCH.
  - Syncs are deltas, throttled, and back off after a 429 (F17, F81, F82).
- Telegram polling backs off on errors (F26). The offset is saved after each update, and
  SIGTERM shuts down within about 7.5 s (F40).
- Docker health check: heartbeat plus web `/health` (F69). A web UI that fails to start is
  reported to the admin (F94). A malformed variable can no longer crash the bot (F60).
- `bot.log` rotates at 5 MB × 5; the reference compose file also limits container logs (F110).
- Every network call has a timeout, a deadline and failover where possible. This covers
  CoinGecko, Blockstream timeouts and malformed ETH RPC answers (F42, F78, F79).
- `--watch` imports only complete files and retries later; it honours `--dry-run`,
  `--db-path` and `--skip-pending` (F51).
- Migrations are versioned and idempotent, with backups (F75). Dedupe results are no longer
  lost when an edit fails (F95), and the web sync works for new users (F97). The version line
  no longer goes stale (F112).

### Fixed: user experience

- Onboarding:
  - YNAB errors are handled, so the user is never stuck (F52).
  - Only open accounts are offered, and duplicate names ask for the number (F53).
  - `/setup` can be cancelled and keeps the old setup until the new one is complete (F54).
  - Crypto setup no longer traps the user (F55).
  - `/approve` leaves users who are already onboarded alone (F93).
  - Switching budgets clears the crypto account (C05).
- Web:
  - An expired session leads to the login page (F24).
  - Real error messages, and stale responses are dropped (F62, F63).
  - Dashboard numbers and currency (F64, C06, F100).
  - Spending, categories and subscriptions are correct (F30, C03, F65, F98, F105).
  - Keyboard-accessible upload with an in-flight guard (F66, F103).
  - The account picker stays open (F99).
  - Filters survive reload and Back (F101).
  - Accessible names, live regions and dialogs (F102).
  - Timestamps in local time (F106).
- CLI: `--db-stats` and `--sync` show the database actually used (C07).
- Docs: README, operations runbook with rollback (F108), amd64-only image (F111), no
  cloudflared leftovers (F113), and a pentest status table (F114).

### Behaviour changes to know

- **Data directory ownership.** After the first start, `bot_data/` belongs to uid/gid 10001
  with modes 0700/0600, so reading `bot.log` on the host needs `sudo`.
- **Sign in once more.** Existing web sessions and unused sign-in links stop working, because
  tokens are now HMAC-keyed. Send `/login`.
- **Upload a fresh statement.** The statement cached by v1 is registered as `legacy`, so
  `/reconcile` and `/dedupe` ask for a fresh upload first.
- **Statements are refused** when their currency differs from the budget's, when the account
  is closed or deleted in YNAB, or when YNAB is rate limiting at the moment of the import
  (nothing is written; try again later).
- Reverted or vanished pendings are deleted from YNAB only while they are still uncleared
  there. Otherwise they are left alone with a warning.
- Crypto and IBKR syncs need a CHF budget. Crypto refuses to post when the BTC wallet shows no
  history while YNAB has a balance; the CLI's `--allow-zero` overrides that.
- Pending and denied users get at most one reply per 5 minutes, and group chats are ignored.
- The reconciliation adjustment no longer claims to lock other transactions.
- `--since-date` is accepted but ignored (syncs are incremental). The CLI database in the
  container is `/app/bot_data/.revolut_to_ynab.db`.
- New optional variables: `WEB_UI_TRUSTED_PROXIES`, `HEALTHCHECK_MAX_AGE`, `APP_UID` and
  `APP_GID`, plus `TELEGRAM_API_BASE` for tests and smoke runs only. Invalid web
  settings (for example a `WEB_UI_SECRET_KEY` under 32 characters) keep the web UI off instead
  of crashing, and the health check then reports unhealthy.
- Subscriptions default to a 13-month window (window options 6, 13, 24 and 36 months).

### Migration notes

- No change to the server's compose file, Caddyfile or `bot.env` is required.
  [Optional improvements](docs/OPERATIONS.md#optional-host-side-improvements) are listed
  separately.
- The first start backs up every database to `bot_data/backups/*.pre-v1.db`, then migrates
  `bot_users.db` and each `transactions_<id>.db` to schema version 1. v1's tables are left
  untouched, so the v1 image can still start for a rollback.
- v1's YNAB copy is not migrated; the first sync fetches the whole budget once.
- Before merging, merge `main` (1.4.2) into the v2 branch, keeping v2's side of the
  conflicts (see [Before merging](docs/OPERATIONS.md#before-merging)).
- Rollback: pin `ghcr.io/cyrill-meier/ynab_helper:a61a2a8` (v1.4.2) in the server's compose
  file (see
  [Rollback](docs/OPERATIONS.md#rollback)).

## 1.4.2 — hotfix (v1), 2026-10-17

Released on `main` while v2 was in progress (PR #26, merge commit `a61a2a8`; image
`ghcr.io/cyrill-meier/ynab_helper:a61a2a8`, the rollback target for the first v2 deploy). It
fixes the two critical data-integrity bugs of the review in the v1 code.

- **Never downgrade reconciled or cleared transactions.** The web UI's sync stored
  `cleared='reconciled'` locally, so the next overlapping import PATCHed reconciled rows
  back to `cleared` with the CSV memo, un-reconciling them and wiping the user's memo. The
  diff now only moves cleared forward (uncleared → cleared), and every PATCH is computed from
  YNAB's current state: memos only lose the `(pending)` marker, and amounts the user edited
  are not reverted. (F01)
- **Never delete a live transaction after a re-date.** After a timezone date-shift re-key,
  the web sync re-inserted the old key as a second local row, and the superseded-pending
  sweep then deleted the live YNAB transaction. The sync now updates the row already linked
  to the YNAB id. The sweep never removes a transaction that another live row links to, or
  one that is cleared in YNAB. (F02)
- **Transactions deleted in YNAB are skipped**: a 404 no longer aborts the whole import.
- **`/cleanup_pending` never un-reconciles**: it no longer sends a cleared state, and it no
  longer strips the marker from rows that are still pending. (C02)
