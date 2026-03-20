#!/usr/bin/env bash
#
# Interactive uninstaller for mpris-scrobbler.
#
# Steps:
#   1. Stop and disable the systemd user service
#   2. Remove the systemd service unit file
#   3. Remove the installed binary
#   4. Optionally remove all data (database, covers, config)
#
# Each step asks for confirmation before proceeding.

set -euo pipefail

BIN_NAME="mpris-scrobbler"
PUBLISH_BIN_NAME="mpris-scrobbler-publish"
INSTALL_DIR="$HOME/.local/bin"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_NAME="mpris-scrobbler.service"

DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/mpris-scrobbler"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

confirm() {
    local prompt="$1"
    echo ""
    read -rp "$(echo -e "${YELLOW}$prompt [y/N]${NC} ")" answer
    case "$answer" in
        [yY]|[yY][eE][sS]) return 0 ;;
        *) return 1 ;;
    esac
}

# -----------------------------------------------------------------------
# Step 1: Stop and disable the service
# -----------------------------------------------------------------------

if systemctl --user is-enabled "$SERVICE_NAME" &>/dev/null || \
   systemctl --user is-active "$SERVICE_NAME" &>/dev/null; then
    if confirm "Step 1/4: Stop and disable $SERVICE_NAME?"; then
        systemctl --user stop "$SERVICE_NAME" 2>/dev/null || true
        systemctl --user disable "$SERVICE_NAME" 2>/dev/null || true
        info "Service stopped and disabled."
    else
        warn "Skipping service stop/disable."
    fi
else
    info "Step 1/4: Service is not installed or not running. Nothing to stop."
fi

# -----------------------------------------------------------------------
# Step 2: Remove service unit file
# -----------------------------------------------------------------------

if [[ -f "$SERVICE_DIR/$SERVICE_NAME" ]]; then
    if confirm "Step 2/4: Remove $SERVICE_DIR/$SERVICE_NAME?"; then
        rm "$SERVICE_DIR/$SERVICE_NAME"
        systemctl --user daemon-reload
        info "Service unit removed and daemon reloaded."
    else
        warn "Skipping service unit removal."
    fi
else
    info "Step 2/4: No service unit file found. Nothing to remove."
fi

# -----------------------------------------------------------------------
# Step 3: Remove binary and publish helper
# -----------------------------------------------------------------------

if [[ -f "$INSTALL_DIR/$BIN_NAME" || -f "$INSTALL_DIR/$PUBLISH_BIN_NAME" ]]; then
    if confirm "Step 3/4: Remove installed binaries from $INSTALL_DIR?"; then
        if [[ -f "$INSTALL_DIR/$BIN_NAME" ]]; then
            rm "$INSTALL_DIR/$BIN_NAME"
            info "Removed: $INSTALL_DIR/$BIN_NAME"
        fi
        if [[ -f "$INSTALL_DIR/$PUBLISH_BIN_NAME" ]]; then
            rm "$INSTALL_DIR/$PUBLISH_BIN_NAME"
            info "Removed: $INSTALL_DIR/$PUBLISH_BIN_NAME"
        fi
    else
        warn "Skipping binary removal."
    fi
else
    info "Step 3/4: No installed binaries found in $INSTALL_DIR. Nothing to remove."
fi

# -----------------------------------------------------------------------
# Step 4: Remove data (database, covers)
# -----------------------------------------------------------------------

if [[ -d "$DATA_DIR" ]]; then
    echo ""
    warn "Data directory: $DATA_DIR"
    if [[ -f "$DATA_DIR/scrobbles.db" ]]; then
        local_size=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
        scrobble_count=$(sqlite3 "$DATA_DIR/scrobbles.db" "SELECT COUNT(*) FROM scrobbles;" 2>/dev/null || echo "unknown")
        info "  Database: $DATA_DIR/scrobbles.db ($scrobble_count scrobbles)"
    fi
    if [[ -d "$DATA_DIR/covers" ]]; then
        cover_count=$(find "$DATA_DIR/covers" -type f 2>/dev/null | wc -l)
        info "  Covers: $DATA_DIR/covers/ ($cover_count files)"
    fi
    info "  Total size: $(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)"

    if confirm "Step 4/4: DELETE all scrobble data, covers, and database? (THIS CANNOT BE UNDONE)"; then
        rm -rf "$DATA_DIR"
        info "Data directory removed."
    else
        warn "Keeping data at $DATA_DIR."
    fi
else
    info "Step 4/4: No data directory found at $DATA_DIR. Nothing to remove."
fi

echo ""
info "Uninstall complete."
