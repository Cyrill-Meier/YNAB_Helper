#!/usr/bin/env bash
# Build the web UI's stylesheet with the pinned Tailwind CSS standalone CLI (DESIGN §9.4).
#
#   scripts/build_css.sh            rebuild ynab_helper/web/static/app.css (minified)
#   scripts/build_css.sh --check    rebuild into a temp file and fail if it differs from the
#                                   committed app.css (CI's CSS drift check)
#   scripts/build_css.sh --offline  never download: use the cached CLI or fail
#                                   (also: TAILWIND_OFFLINE=1)
#
# The CLI (v3.4.17, no Node.js needed) is downloaded once per OS/architecture into
# .cache/tailwindcss-v3.4.17/ from the official GitHub release and verified against the
# release's published sha256 checksums (hard-coded below). A cached binary is re-verified
# on every run; a mismatch is an error, never silently replaced.
#
# Inputs: tailwind.config.js (repo root; content = templates + JS modules, darkMode
# 'class', the v1 palette), ynab_helper/web/styles/input.css.
set -euo pipefail

TAILWIND_VERSION="3.4.17"
RELEASE_URL="https://github.com/tailwindlabs/tailwindcss/releases/download/v${TAILWIND_VERSION}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${ROOT}/tailwind.config.js"
INPUT="${ROOT}/ynab_helper/web/styles/input.css"
OUTPUT="${ROOT}/ynab_helper/web/static/app.css"
CACHE_DIR="${ROOT}/.cache/tailwindcss-v${TAILWIND_VERSION}"

mode="build"
offline="${TAILWIND_OFFLINE:-0}"
for arg in "$@"; do
  case "${arg}" in
    --check) mode="check" ;;
    --offline) offline="1" ;;
    -h | --help)
      sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "build_css.sh: unknown argument: ${arg}" >&2
      exit 2
      ;;
  esac
done

die() {
  echo "build_css.sh: $*" >&2
  exit 1
}

# ── pick the release asset for this machine (checksums: the release's sha256sums.txt) ──
os="$(uname -s)"
arch="$(uname -m)"
case "${os}/${arch}" in
  Darwin/arm64)
    asset="tailwindcss-macos-arm64"
    sha256="a1d0c7985759accca0bf12e51ac1dcbf0f6cf2fffb62e6e0f62d091c477a10a3"
    ;;
  Darwin/x86_64)
    asset="tailwindcss-macos-x64"
    sha256="6cbdad74be776c087ffa5e9a057512c54898f9fe8828d3362212dfe32fc933a3"
    ;;
  Linux/x86_64 | Linux/amd64)
    asset="tailwindcss-linux-x64"
    sha256="7d24f7fa191d2193b78cd5f5a42a6093e14409521908529f42d80b11fde1f1d4"
    ;;
  Linux/aarch64 | Linux/arm64)
    asset="tailwindcss-linux-arm64"
    sha256="69b1378b8133192d7d2feb12a116fa12d035594f58db3eff215879e4ad8cf39b"
    ;;
  Linux/armv7l | Linux/armv7)
    asset="tailwindcss-linux-armv7"
    sha256="704e7d91afba6e1f630889afd0d7db36b4634e628512cc141d504a5beae28860"
    ;;
  MINGW*/x86_64 | MSYS*/x86_64 | CYGWIN*/x86_64)
    asset="tailwindcss-windows-x64.exe"
    sha256="67f1c5e3f5a03406a7bf5badf5ada09b79f3ae78ec43450c15f7e983068da346"
    ;;
  MINGW*/aarch64 | MSYS*/aarch64 | MINGW*/arm64 | MSYS*/arm64)
    asset="tailwindcss-windows-arm64.exe"
    sha256="76f516476784c00f1562160b5758e3d8f0e6c48957efb26b5b50fbdfd76aa382"
    ;;
  *)
    die "no pinned Tailwind CLI for ${os}/${arch}"
    ;;
esac

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    die "neither sha256sum nor shasum is available"
  fi
}

binary="${CACHE_DIR}/${asset}"

fetch_cli() {
  [[ "${offline}" == "1" ]] && die "${binary} is not cached and --offline was given"
  command -v curl >/dev/null 2>&1 || die "curl is required to download the Tailwind CLI"
  mkdir -p "${CACHE_DIR}"
  local partial
  partial="$(mktemp "${CACHE_DIR}/.download.XXXXXX")"
  # shellcheck disable=SC2064
  trap "rm -f '${partial}'" EXIT
  echo "build_css.sh: downloading ${asset} v${TAILWIND_VERSION}" >&2
  curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
    --retry 3 --retry-delay 2 --output "${partial}" "${RELEASE_URL}/${asset}"
  local actual
  actual="$(sha256_of "${partial}")"
  if [[ "${actual}" != "${sha256}" ]]; then
    die "checksum mismatch for ${asset}: expected ${sha256}, got ${actual}"
  fi
  chmod 0755 "${partial}"
  mv -f "${partial}" "${binary}"
  trap - EXIT
}

if [[ ! -f "${binary}" ]]; then
  fetch_cli
fi
actual="$(sha256_of "${binary}")"
if [[ "${actual}" != "${sha256}" ]]; then
  die "cached ${binary} has sha256 ${actual}, expected ${sha256}; delete it and rerun"
fi
[[ -x "${binary}" ]] || chmod 0755 "${binary}"

[[ -f "${CONFIG}" ]] || die "missing ${CONFIG}"
[[ -f "${INPUT}" ]] || die "missing ${INPUT}"

build_into() {
  # The CLI resolves the config's content globs relative to the config file
  # (content.relative), so the working directory does not matter; run from the root anyway.
  # BROWSERSLIST_IGNORE_OLD_DATA: the CLI bundles its own caniuse data; its "outdated"
  # notice is noise (the output does not depend on the date).
  (cd "${ROOT}" && BROWSERSLIST_IGNORE_OLD_DATA=1 "${binary}" --config "${CONFIG}" \
    --input "${INPUT}" --output "$1" --minify) \
    2> >(grep -v -e '^$' -e 'Rebuilding' -e 'Done in' >&2 || true)
}

if [[ "${mode}" == "check" ]]; then
  tmp="$(mktemp "${TMPDIR:-/tmp}/app.css.XXXXXX")"
  trap 'rm -f "${tmp}"' EXIT
  build_into "${tmp}"
  if [[ ! -f "${OUTPUT}" ]] || ! cmp -s "${tmp}" "${OUTPUT}"; then
    echo "build_css.sh: ${OUTPUT#"${ROOT}"/} is out of date: run scripts/build_css.sh and commit it" >&2
    exit 1
  fi
  echo "build_css.sh: app.css is up to date" >&2
else
  tmp="$(mktemp "$(dirname "${OUTPUT}")/.app.css.XXXXXX")"
  trap 'rm -f "${tmp}"' EXIT
  build_into "${tmp}"
  chmod 0644 "${tmp}"
  mv -f "${tmp}" "${OUTPUT}"
  trap - EXIT
  echo "build_css.sh: wrote ${OUTPUT#"${ROOT}"/} ($(wc -c <"${OUTPUT}" | tr -d ' ') bytes)" >&2
fi
