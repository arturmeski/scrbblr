#!/usr/bin/env bash
set -euo pipefail

# Generate and publish the HTML report only when new scrobbles exist.
#
# Defaults can be overridden by config file and then by flags:
#
# Config file precedence:
#   1) $XDG_CONFIG_HOME/scrbblr/publish.conf
#   2) ~/.config/scrbblr/publish.conf
#   3) ~/.scrbblr-publish.conf (legacy fallback)
#
# Supported config variables:
#   OUTPUT_DIR
#   REMOTE_TARGET
#   DB_PATH
#
# Flags:
#   --output <dir>
#   --remote <rsync target>
#   --db-path <path>
#   --watch              keep running, checking every --interval seconds
#   --interval <secs>    seconds between checks in --watch mode (default: 300)

OUTPUT_DIR="${HOME}/music-report"
REMOTE_TARGET="user@host:/var/www/music-report"
DB_PATH=""
WATCH=0
INTERVAL=300
FORCE=0

XDG_CONFIG_BASE="${XDG_CONFIG_HOME:-${HOME}/.config}"
PRIMARY_CONFIG="${XDG_CONFIG_BASE}/scrbblr/publish.conf"
LEGACY_CONFIG="${HOME}/.scrbblr-publish.conf"

if [[ -f "${PRIMARY_CONFIG}" ]]; then
  # shellcheck disable=SC1090
  source "${PRIMARY_CONFIG}"
elif [[ -f "${LEGACY_CONFIG}" ]]; then
  # shellcheck disable=SC1090
  source "${LEGACY_CONFIG}"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --remote)
      REMOTE_TARGET="$2"
      shift 2
      ;;
    --db-path)
      DB_PATH="$2"
      shift 2
      ;;
    --watch)
      WATCH=1
      shift
      ;;
    --interval)
      INTERVAL="$2"
      shift 2
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --help|-h)
      cat <<'EOF'
Usage: scrbblr-publish [OPTIONS]

Generate and publish the HTML report only when new scrobbles exist.
Cover art is extracted automatically by `scrbblr report` before each run.

Options:
  --output <dir>       Directory for generated report files (default: ~/music-report)
  --remote <target>    rsync destination, e.g. user@host:/var/www/music-report
  --db-path <path>     Path to scrobbles database (default: auto-detected)
  --watch              Keep running, checking every --interval seconds
  --interval <secs>    Seconds between checks in --watch mode (default: 300)
  --force              Regenerate and publish even if no new scrobbles
  --help, -h           Show this help and exit

Config file (loaded before flags):
  $XDG_CONFIG_HOME/scrbblr/publish.conf  (primary)
  ~/.config/scrbblr/publish.conf         (fallback)
  ~/.scrbblr-publish.conf                (legacy)

Supported config variables: OUTPUT_DIR, REMOTE_TARGET, DB_PATH
EOF
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

if [[ ! "${INTERVAL}" =~ ^[1-9][0-9]*$ ]]; then
  printf 'Invalid --interval value: %s (must be a positive integer)\n' "${INTERVAL}" >&2
  exit 2
fi

if ! command -v scrbblr >/dev/null 2>&1; then
  printf 'scrbblr not found in PATH\n' >&2
  exit 1
fi

if ! command -v rsync >/dev/null 2>&1; then
  printf 'rsync not found in PATH\n' >&2
  exit 1
fi

STATE_HOME="${XDG_STATE_HOME:-${HOME}/.local/state}"
STATE_DIR="${STATE_HOME}/scrbblr"
MARKER_FILE="${STATE_DIR}/last-published-scrobble.txt"
mkdir -p "${STATE_DIR}"

do_publish() {
  local last_args=()
  if [[ -n "${DB_PATH}" ]]; then
    last_args+=(--db-path "${DB_PATH}")
  fi

  local latest_scrobble
  latest_scrobble="$(scrbblr last-scrobble "${last_args[@]}")"
  if [[ -z "${latest_scrobble}" ]]; then
    printf 'No scrobbles yet. Nothing to publish.\n'
    return 0
  fi

  local last_published=""
  if [[ -f "${MARKER_FILE}" ]]; then
    last_published="$(<"${MARKER_FILE}")"
  fi

  if [[ "${last_published}" == "${latest_scrobble}" ]]; then
    if [[ "${FORCE}" -eq 1 ]]; then
      printf 'No new scrobbles since %s, but --force set. Regenerating anyway...\n' "${latest_scrobble}"
    else
      printf 'No new scrobbles since %s. Skipping publish.\n' "${latest_scrobble}"
      return 0
    fi
  else
    printf 'New scrobbles detected (latest: %s). Regenerating report...\n' "${latest_scrobble}"
  fi

  local report_args=(report --html --output "${OUTPUT_DIR}")
  if [[ -n "${DB_PATH}" ]]; then
    report_args+=(--db-path "${DB_PATH}")
  fi
  scrbblr "${report_args[@]}"

  printf 'Publishing via rsync to %s...\n' "${REMOTE_TARGET}"
  rsync -Pavz "${OUTPUT_DIR}" "${REMOTE_TARGET}"

  printf '%s\n' "${latest_scrobble}" > "${MARKER_FILE}"
  printf 'Publish complete. Marker updated at %s\n' "${MARKER_FILE}"
}

if [[ "${WATCH}" -eq 1 ]]; then
  printf 'Watch mode: checking every %d seconds. Press Ctrl+C to stop.\n' "${INTERVAL}"
  while true; do
    do_publish
    sleep "${INTERVAL}"
  done
else
  do_publish
fi
