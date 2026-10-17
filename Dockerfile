# YNAB Helper: Revolut CSV -> YNAB importer, Telegram bot and web UI in one image.
#
# Build:   docker build -t ynab-helper .
# Run:     docker run --rm ynab-helper selfcheck
#
# The production compose file on the server has no `command:`, `user:` or
# `healthcheck:` overrides, so everything the container needs to do the right
# thing (privilege drop, health, stop signal) is defined here.

# Base image pinned by the digest of the multi-arch index (python 3.12.15 on
# Debian bookworm, published 2026-10-06) so rebuilding a commit reproduces the
# same OS layer. Dependabot (docker ecosystem) proposes digest bumps.
FROM python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258

# PYTHONDONTWRITEBYTECODE: the code tree is root-owned and read-only for the
# runtime user, and the HEALTHCHECK runs as root; neither should try to write
# __pycache__ into /app at runtime (pip already precompiled site-packages).
# PYTHONUNBUFFERED: log lines reach `docker logs` immediately.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Unprivileged runtime identity. ynab_helper.container switches to it after the
# root-only setup (see the note at ENTRYPOINT). The ids are fixed because the
# data volume's ownership on the host must survive image rebuilds. The home
# directory is the data dir, matching the HOME the entrypoint sets, so
# ~-relative defaults (e.g. the CLI's ~/.revolut_to_ynab.db) persist.
# /app/bot_data is created and owned by the app user *before* the VOLUME
# declaration, so a fresh named volume inherits that ownership and mode.
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --home-dir /app/bot_data \
        --shell /usr/sbin/nologin app \
    && install -d -o 10001 -g 10001 -m 0700 /app/bot_data

WORKDIR /app

# Dependencies first so this layer is reused until requirements.txt changes.
# --require-hashes: every package (including transitive ones) must match the
# hashes in the lock file. --only-binary: install wheels only, so no package
# build code runs during the image build.
COPY requirements.txt ./
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: \
        --disable-pip-version-check --root-user-action=ignore \
        -r requirements.txt

# Application code. Copied as root and therefore read-only for the app user.
# The two top-level scripts are kept because operators run them through
# `docker exec` (`python3 revolut_to_ynab.py ...`).
COPY ynab_helper/ ./ynab_helper/
COPY revolut_to_ynab.py revolut_ynab_bot.py ./

# Persistent state: SQLite databases, uploaded statements, bot.log, heartbeat.
VOLUME ["/app/bot_data"]

# Web UI (uvicorn) inside the container; reached through the reverse proxy.
EXPOSE 8080

# The bot installs a SIGTERM handler and shuts down within 8 s, inside the
# 10 s that both a plain `docker stop` and Watchtower allow before SIGKILL
# (scripts/ci_smoke.sh enforces this). Stated explicitly so a base-image
# change cannot silently alter it.
STOPSIGNAL SIGTERM

# Liveness: ynab_helper.healthcheck checks that the Telegram poll loop is still
# making progress (heartbeat file in /app/bot_data), which a hung network call
# would otherwise hide (F69). Docker marks the container unhealthy after 3
# failed checks; the 60 s start period covers startup migrations.
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD ["python3", "-m", "ynab_helper.healthcheck"]

# Build metadata, declared last so a new commit only invalidates these tiny
# layers. CI passes the short SHA and a human-readable UTC date (shown in the
# version line of /help and the admin startup message) and overrides the
# `revision`/`created` labels with the full SHA and an RFC 3339 timestamp.
ARG BUILD_SHA=dev
ARG BUILD_DATE=local
ENV BUILD_SHA=${BUILD_SHA} \
    BUILD_DATE=${BUILD_DATE}
LABEL org.opencontainers.image.source="https://github.com/Cyrill-Meier/YNAB_Helper" \
      org.opencontainers.image.revision="${BUILD_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.title="ynab-helper" \
      org.opencontainers.image.description="Revolut CSV to YNAB importer with a Telegram bot and web dashboard"

# There is deliberately no USER instruction. Production bind-mounts a
# bot_data directory whose files are all root-owned and a root-only (0600)
# bot.env, and its compose file cannot be changed by this repository, so a
# process that starts as uid 10001 could neither read its configuration nor
# write its databases (F70: the container would crash-loop). Instead the
# entrypoint starts as root, chowns /app/bot_data to 10001:10001, reads
# /app/.env, sets umask 077 and drops to uid/gid 10001 before running the
# command. When the container is started as a non-root user it just runs.
# `docker exec` starts processes as root and bypasses the entrypoint, so run
# manual commands through it to get the same .env loading and privilege drop,
# e.g. `docker exec revolut-ynab-bot python3 -m ynab_helper.container cli …`;
# otherwise they may leave root-owned files in /app/bot_data.
ENTRYPOINT ["python3", "-m", "ynab_helper.container"]
CMD ["bot"]
