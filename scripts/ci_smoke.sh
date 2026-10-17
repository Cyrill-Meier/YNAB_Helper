#!/usr/bin/env bash
# Container smoke test for a built ynab-helper image. CI runs it before any
# push; it also works locally (Colima, Docker Desktop):
#
#   scripts/ci_smoke.sh IMAGE
#
# What it checks, and why:
#   1. Image config: no USER (the entrypoint must start as root to repair the
#      production bind mount, F70), the documented ENTRYPOINT/CMD, HEALTHCHECK,
#      STOPSIGNAL and data volume, and the fixed app user 10001:10001.
#   2. The code tree is read-only and readable for the app user.
#   3. Entrypoint on a production-like data dir (a volume seeded with
#      root-owned files): `selfcheck` passes, everything is chowned to 10001,
#      and a command run through the entrypoint has uid/gid 10001, no
#      supplementary groups and no capabilities. On Linux with passwordless
#      sudo (CI) a root-only 0600 /app/.env is mounted too and must reach the
#      command's environment; bind-mount ownership is not portable to macOS VMs.
#   4. The documented entry points (DESIGN 2.3) start: `cli --help` through
#      the entrypoint, `python3 revolut_to_ynab.py --help` both through the
#      entrypoint and the way `docker exec` runs it (root, no entrypoint), and
#      both shims import as the app user without starting anything. The root
#      run must not leave root-owned files in the data dir.
#   5. The bot started with no arguments (production has no command override)
#      keeps running with an unreachable Telegram API (--network none, so
#      nothing leaves the machine), runs as 10001, creates its files 0600 and
#      exits on SIGTERM within production's 10 s stop timeout instead of being
#      SIGKILLed (F40).
#
# Every check runs even after a failure; the exit status is 1 if any failed.
set -euo pipefail

if [[ $# -ne 1 || -z "$1" ]]; then
    echo "usage: $0 IMAGE" >&2
    exit 2
fi
readonly IMAGE="$1"
readonly APP_ID=10001
readonly DROPPED_IDS="${APP_ID} ${APP_ID} ${APP_ID} ${APP_ID}" # real/effective/saved/fs
readonly NO_CAPS="0000000000000000"
readonly ENV_MARKER="smoke-dotenv-$$"
# Production stops the bot with a plain `docker stop` or Watchtower, both of
# which send SIGKILL 10 s after SIGTERM (the server's compose file has no
# stop_grace_period). The bot promises to stop within 8 s (DESIGN 8.1); the
# budget leaves 1 s of slack for docker itself and still fails a bot that
# would only just make it, or not make it, in production.
readonly STOP_TIMEOUT=10
readonly STOP_BUDGET_MS=9000
readonly START_TIMEOUT=30
# Upper bound for commands that must return on their own (an import that
# accidentally starts the bot would otherwise hang the job).
readonly COMMAND_TIMEOUT=60

run_id="ynab-smoke-$$-${RANDOM}"
volume="${run_id}-data"
container="${run_id}-bot"
env_dir=""

cleanup() {
    docker rm --force "${container}" >/dev/null 2>&1 || true
    docker volume rm --force "${volume}" >/dev/null 2>&1 || true
    if [[ -n "${env_dir}" ]]; then
        sudo -n rm -rf -- "${env_dir}" >/dev/null 2>&1 || rm -rf -- "${env_dir}" || true
    fi
}
trap cleanup EXIT

failures=0
pass() { printf 'ok    %s\n' "$1"; }
fail() {
    printf 'FAIL  %s\n' "$1" >&2
    failures=$((failures + 1))
}
expect_eq() { # name expected actual
    if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1: expected '$2', got '$3'"; fi
}
expect_success() { # name command...
    local name="$1" output
    shift
    if output="$("$@" 2>&1)"; then
        pass "${name}"
    else
        fail "${name} (exit $?)"
        printf '%s\n' "${output}" | tail -n 8 | sed 's/^/      /' >&2
    fi
}

# Milliseconds since the epoch. bash 5 has EPOCHREALTIME; the bash 3.2 that
# macOS ships does not, so fall back to perl (part of macOS) and then to
# whole seconds.
now_ms() {
    if [[ -n "${EPOCHREALTIME:-}" ]]; then
        local micros="${EPOCHREALTIME/[.,]/}"
        echo "$((10#${micros} / 1000))"
    elif command -v perl >/dev/null 2>&1; then
        perl -MTime::HiRes=time -e 'printf "%d\n", time() * 1000'
    else
        echo "$(($(date +%s) * 1000))"
    fi
}

# Prints the stdout of "$@"; if the command fails, prints a marker instead so
# that a broken command can never satisfy an expectation (several checks
# expect empty output, which a failed `docker run` would also produce).
out() {
    local output
    if output="$("$@")"; then
        printf '%s' "${output}"
    else
        printf '<command failed: %s>' "$*"
    fi
}

# Whitespace-normalised value of one line ("Uid", "CapEff", ...) of a
# /proc/<pid>/status text given on stdin.
status_field() { awk -v k="$1:" '$1 == k { $1 = ""; print }' | xargs; }

cfg() { docker image inspect --format "$1" "${IMAGE}"; }
# Root shell inside the image that bypasses the entrypoint (inspection only).
inspect_sh() { docker run --rm --network none --entrypoint sh "$@"; }
# A command run through the real entrypoint (root setup, then privilege drop).
via_entrypoint() {
    docker run --rm --network none -v "${volume}:/app/bot_data" \
        ${env_mount[@]+"${env_mount[@]}"} "${IMAGE}" "$@"
}
in_bot() { docker exec "${container}" "$@"; }

echo "== 1. image configuration"
expect_eq "no USER instruction (entrypoint starts as root)" "" "$(out cfg '{{.Config.User}}')"
expect_eq "entrypoint" '["python3","-m","ynab_helper.container"]' \
    "$(out cfg '{{json .Config.Entrypoint}}')"
expect_eq "default command" '["bot"]' "$(out cfg '{{json .Config.Cmd}}')"
expect_eq "stop signal" "SIGTERM" "$(out cfg '{{.Config.StopSignal}}')"
expect_eq "healthcheck command" '["CMD","python3","-m","ynab_helper.healthcheck"]' \
    "$(out cfg '{{json .Config.Healthcheck.Test}}')"
expect_eq "data volume" '{"/app/bot_data":{}}' "$(out cfg '{{json .Config.Volumes}}')"
expect_eq "web port" '{"8080/tcp":{}}' "$(out cfg '{{json .Config.ExposedPorts}}')"
env_json="$(out cfg '{{json .Config.Env}}')"
for var in PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1; do
    if [[ "${env_json}" == *"\"${var}\""* ]]; then pass "env ${var}"; else fail "env ${var} missing"; fi
done
expect_eq "app user" "app:x:${APP_ID}:${APP_ID}::/app/bot_data:/usr/sbin/nologin" \
    "$(out inspect_sh "${IMAGE}" -c 'getent passwd app')"

echo "== 2. code tree permissions"
expect_eq "code not writable by the app user" "read-only" "$(
    out inspect_sh --user "${APP_ID}:${APP_ID}" "${IMAGE}" -c \
        'if [ -w /app ] || [ -w /app/ynab_helper ]; then echo writable; else echo read-only; fi'
)"
expect_eq "every code file readable by the app user" "" "$(
    out inspect_sh --user "${APP_ID}:${APP_ID}" "${IMAGE}" -c \
        'find /app/ynab_helper /app/revolut_to_ynab.py /app/revolut_ynab_bot.py ! -readable'
)"

echo "== 3. entrypoint on a root-owned data dir (as in production)"
docker volume create "${volume}" >/dev/null
inspect_sh -v "${volume}:/app/bot_data" "${IMAGE}" -c '
    set -e
    mkdir -p /app/bot_data/csv_cache/1
    echo legacy > /app/bot_data/legacy.bak
    echo "Type,Product" > /app/bot_data/csv_cache/1/old.csv
    chown -R 0:0 /app/bot_data
    chmod 0755 /app/bot_data /app/bot_data/csv_cache /app/bot_data/csv_cache/1
    chmod 0644 /app/bot_data/legacy.bak /app/bot_data/csv_cache/1/old.csv
'
env_mount=()
kernel="$(uname -s)"
if [[ "${kernel}" == "Linux" ]] && sudo -n true 2>/dev/null; then
    env_dir="$(mktemp -d)"
    printf 'SMOKE_ENV_MARKER=%s\n' "${ENV_MARKER}" >"${env_dir}/bot.env"
    sudo -n chown 0:0 "${env_dir}/bot.env"
    sudo -n chmod 0600 "${env_dir}/bot.env"
    env_mount=(-v "${env_dir}/bot.env:/app/.env:ro")
    echo "(with a root-only 0600 /app/.env)"
else
    echo "(no root-only /app/.env mount: needs Linux and passwordless sudo)"
fi

if via_entrypoint selfcheck; then
    pass "selfcheck"
else
    fail "selfcheck"
fi
expect_eq "everything in bot_data owned by ${APP_ID}:${APP_ID}" "" "$(
    out inspect_sh -v "${volume}:/app/bot_data" "${IMAGE}" -c \
        "find /app/bot_data \\( ! -user ${APP_ID} -o ! -group ${APP_ID} \\) -print"
)"
# Any other command is exec'd after the drop, so it becomes PID 1 itself.
dropped_status="$(out via_entrypoint cat /proc/self/status)"
expect_eq "command uids after the drop" "${DROPPED_IDS}" \
    "$(status_field Uid <<<"${dropped_status}")"
expect_eq "command gids after the drop" "${DROPPED_IDS}" \
    "$(status_field Gid <<<"${dropped_status}")"
expect_eq "command supplementary groups" "" "$(status_field Groups <<<"${dropped_status}")"
expect_eq "command effective capabilities" "${NO_CAPS}" \
    "$(status_field CapEff <<<"${dropped_status}")"
expect_eq "command permitted capabilities" "${NO_CAPS}" \
    "$(status_field CapPrm <<<"${dropped_status}")"
if [[ ${#env_mount[@]} -gt 0 ]]; then
    expect_eq "root-only .env loaded before the drop" "${ENV_MARKER}" \
        "$(out via_entrypoint printenv SMOKE_ENV_MARKER)"
fi

echo "== 4. documented entry points (DESIGN 2.3)"
# --help needs no configuration and must exit 0 before any network or YNAB I/O.
# `docker exec revolut-ynab-bot python3 revolut_to_ynab.py ...` runs as root in
# /app and bypasses the entrypoint (no chown, no privilege drop); this is the
# closest `docker run` equivalent. It runs first and is followed directly by
# the ownership check, because any later run through the entrypoint would
# chown a root-owned file it left behind and hide it.
expect_success "docker exec style (root, no entrypoint): python3 revolut_to_ynab.py --help" \
    docker run --rm --network none -v "${volume}:/app/bot_data" \
    ${env_mount[@]+"${env_mount[@]}"} \
    --entrypoint timeout "${IMAGE}" "${COMMAND_TIMEOUT}" python3 revolut_to_ynab.py --help
expect_eq "docker exec style left no root-owned files in bot_data" "" "$(
    out inspect_sh -v "${volume}:/app/bot_data" "${IMAGE}" -c \
        "find /app/bot_data \\( ! -user ${APP_ID} -o ! -group ${APP_ID} \\) -print"
)"
expect_success "entrypoint: cli --help" via_entrypoint cli --help
expect_success "entrypoint: python3 revolut_to_ynab.py --help" \
    via_entrypoint timeout "${COMMAND_TIMEOUT}" python3 revolut_to_ynab.py --help
# Importing a shim must not run it (an unguarded main() would start the bot).
expect_success "both shims import as the app user without running" \
    inspect_sh --user "${APP_ID}:${APP_ID}" "${IMAGE}" -c \
    "cd /app && exec timeout ${COMMAND_TIMEOUT} python3 -c 'import revolut_to_ynab, revolut_ynab_bot'"

echo "== 5. bot runtime: privilege drop, file modes, SIGTERM"
# Placeholder credentials (deliberately not token-shaped); with --network none
# every Telegram call fails fast, which the bot must survive.
docker run --detach --name "${container}" --network none \
    -v "${volume}:/app/bot_data" ${env_mount[@]+"${env_mount[@]}"} \
    -e TELEGRAM_BOT_TOKEN=0:smoke-test-placeholder \
    -e TELEGRAM_ADMIN_ID=1 \
    -e WEB_UI_ENABLED=0 \
    "${IMAGE}" >/dev/null

# The users DB appears once startup (root setup, config, migrations) is done.
started=no
for _ in $(seq 1 "${START_TIMEOUT}"); do
    running="$(out docker inspect --format '{{.State.Running}}' "${container}")"
    [[ "${running}" == "true" ]] || break
    if in_bot test -f /app/bot_data/bot_users.db; then
        started=yes
        break
    fi
    sleep 1
done
if [[ "${started}" == "yes" ]]; then
    pass "bot started and created bot_users.db"
    # Give startup a moment to fail on the unreachable network, if it would.
    sleep 3
    running="$(out docker inspect --format '{{.State.Running}}' "${container}")"
    expect_eq "bot still running without network" "true" "${running}"
else
    state="$(out docker inspect --format '{{.State.Status}} exit={{.State.ExitCode}}' "${container}")"
    fail "bot did not start within ${START_TIMEOUT}s (${state})"
fi

if [[ "${started}" == "yes" && "${running}" == "true" ]]; then
    # PID 1 is the entrypoint process itself, which must have dropped root.
    bot_status="$(out in_bot cat /proc/1/status)"
    expect_eq "bot uids" "${DROPPED_IDS}" "$(status_field Uid <<<"${bot_status}")"
    expect_eq "bot gids" "${DROPPED_IDS}" "$(status_field Gid <<<"${bot_status}")"
    expect_eq "bot supplementary groups" "" "$(status_field Groups <<<"${bot_status}")"
    expect_eq "bot effective capabilities" "${NO_CAPS}" "$(status_field CapEff <<<"${bot_status}")"
    expect_eq "bot_users.db owner and mode (umask 077)" "${APP_ID}:${APP_ID} 600" \
        "$(out in_bot stat -c '%u:%g %a' /app/bot_data/bot_users.db)"
    expect_eq "no root-owned files created at runtime" "" \
        "$(out in_bot find /app/bot_data ! -user "${APP_ID}" -print)"

    start_ms="$(now_ms)"
    docker stop --time "${STOP_TIMEOUT}" "${container}" >/dev/null
    elapsed_ms=$(($(now_ms) - start_ms))
    exit_code="$(out docker inspect --format '{{.State.ExitCode}}' "${container}")"
    if [[ "${exit_code}" != "137" && "${elapsed_ms}" -le "${STOP_BUDGET_MS}" ]]; then
        pass "graceful stop on SIGTERM (${elapsed_ms} ms, exit ${exit_code})"
    else
        fail "stop on SIGTERM took ${elapsed_ms} ms (budget ${STOP_BUDGET_MS} ms), exit ${exit_code} (137 = killed after the ${STOP_TIMEOUT} s grace period)"
    fi
fi
if [[ "${failures}" -gt 0 ]]; then
    echo "--- bot container log (tail)" >&2
    docker logs --tail 40 "${container}" >&2 || true
fi

echo
if [[ "${failures}" -gt 0 ]]; then
    echo "${failures} check(s) failed" >&2
    exit 1
fi
echo "all container smoke checks passed"
