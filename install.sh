#!/usr/bin/env bash
#
# Interactive installer for scrbblr.
#
# Steps:
#   1. Build release binary
#   2. Install to ~/.local/bin/
#   3. Install systemd user service
#   4. Enable and start the service
#   5. Check status and print logs
#
# Each step asks for confirmation before proceeding.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_NAME="scrbblr"
PUBLISH_SCRIPT_NAME="scrbblr-publish.sh"
PUBLISH_INSTALLED_NAME="scrbblr-publish"
PUBLISH_CONFIG_EXAMPLE_SRC="$SCRIPT_DIR/contrib/examples/publish.conf.example"
PUBLISH_CONFIG_DIR="$HOME/.config/scrbblr"
PUBLISH_CONFIG_FILE="$PUBLISH_CONFIG_DIR/publish.conf"
INSTALL_DIR="$HOME/.local/bin"
SERVICE_SRC="$SCRIPT_DIR/contrib/systemd/user/scrbblr.service"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_NAME="scrbblr.service"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# Ask the user for confirmation. Returns 0 if yes, 1 if no.
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
# Step 1: Build
# -----------------------------------------------------------------------

info "Source directory: $SCRIPT_DIR"
echo ""

if confirm "Step 1/5: Build release binary? (cargo build --release)"; then
    info "Building..."
    (cd "$SCRIPT_DIR" && cargo build --release)
    info "Build complete: $SCRIPT_DIR/target/release/$BIN_NAME"
else
    warn "Skipping build. Make sure $SCRIPT_DIR/target/release/$BIN_NAME exists."
fi

# Verify the binary exists before continuing.
if [[ ! -f "$SCRIPT_DIR/target/release/$BIN_NAME" ]]; then
    error "Binary not found at $SCRIPT_DIR/target/release/$BIN_NAME"
    error "Cannot continue without a built binary. Run 'cargo build --release' first."
    exit 1
fi

# -----------------------------------------------------------------------
# Step 2: Install binary and publish helper
# -----------------------------------------------------------------------

if confirm "Step 2/5: Install binary and publish helper to $INSTALL_DIR/?"; then
    mkdir -p "$INSTALL_DIR"
    install -Dm755 "$SCRIPT_DIR/target/release/$BIN_NAME" "$INSTALL_DIR/$BIN_NAME"
    info "Installed: $INSTALL_DIR/$BIN_NAME"

    if [[ -f "$SCRIPT_DIR/$PUBLISH_SCRIPT_NAME" ]]; then
        install -Dm755 \
            "$SCRIPT_DIR/$PUBLISH_SCRIPT_NAME" \
            "$INSTALL_DIR/$PUBLISH_INSTALLED_NAME"
        info "Installed: $INSTALL_DIR/$PUBLISH_INSTALLED_NAME"

        if [[ ! -f "$PUBLISH_CONFIG_FILE" ]]; then
            if [[ -f "$PUBLISH_CONFIG_EXAMPLE_SRC" ]]; then
                mkdir -p "$PUBLISH_CONFIG_DIR"
                install -Dm644 "$PUBLISH_CONFIG_EXAMPLE_SRC" "$PUBLISH_CONFIG_FILE"
                info "Installed example config: $PUBLISH_CONFIG_FILE"
            else
                warn "Publish config example not found: $PUBLISH_CONFIG_EXAMPLE_SRC"
            fi
        else
            info "Keeping existing publish config: $PUBLISH_CONFIG_FILE"
        fi
    else
        warn "Publish helper not found: $SCRIPT_DIR/$PUBLISH_SCRIPT_NAME"
    fi

    # Check PATH
    if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
        warn "$INSTALL_DIR is not in your PATH."
        warn "Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
        echo ""
        echo "    export PATH=\"\$HOME/.local/bin:\$PATH\""
        echo ""
    else
        info "$INSTALL_DIR is already in PATH."
    fi
else
    warn "Skipping install."
fi

# -----------------------------------------------------------------------
# Step 3: Install systemd user service
# -----------------------------------------------------------------------

if confirm "Step 3/5: Install systemd user service to $SERVICE_DIR/$SERVICE_NAME?"; then
    if [[ ! -f "$SERVICE_SRC" ]]; then
        error "Service file not found: $SERVICE_SRC"
        exit 1
    fi

    mkdir -p "$SERVICE_DIR"
    cp "$SERVICE_SRC" "$SERVICE_DIR/$SERVICE_NAME"
    info "Installed: $SERVICE_DIR/$SERVICE_NAME"

    echo ""
    info "Current service configuration:"
    grep "ExecStart" "$SERVICE_DIR/$SERVICE_NAME"
    echo ""
    warn "If you need a different --player name, edit:"
    warn "  $SERVICE_DIR/$SERVICE_NAME"
    warn "then run: systemctl --user daemon-reload"

    # Reload systemd to pick up the new/updated unit file.
    systemctl --user daemon-reload
    info "systemd user daemon reloaded."
else
    warn "Skipping service installation."
fi

# -----------------------------------------------------------------------
# Step 4: Enable and start
# -----------------------------------------------------------------------

if confirm "Step 4/5: Enable and start $SERVICE_NAME?"; then
    systemctl --user enable "$SERVICE_NAME"
    info "Service enabled (will start on login)."

    systemctl --user restart "$SERVICE_NAME"
    info "Service (re)started."
else
    warn "Skipping enable/start."
fi

# -----------------------------------------------------------------------
# Step 5: Check status and logs
# -----------------------------------------------------------------------

if confirm "Step 5/5: Check service status and print recent logs?"; then
    echo ""
    info "Service status:"
    echo "---"
    systemctl --user status "$SERVICE_NAME" --no-pager || true
    echo "---"
    echo ""
    info "Recent logs:"
    echo "---"
    journalctl --user -u "$SERVICE_NAME" -n 30 --no-pager || true
    echo "---"
else
    warn "Skipping status check."
fi

echo ""
info "Done."
info ""
info "Useful commands:"
info "  Check status:   systemctl --user status $SERVICE_NAME"
info "  View logs:      journalctl --user -u $SERVICE_NAME -f"
info "  Stop service:   systemctl --user stop $SERVICE_NAME"
info "  Restart:        systemctl --user restart $SERVICE_NAME"
info "  Generate report: $BIN_NAME report --html --output ~/music-report"
info "  Publish report:  $PUBLISH_INSTALLED_NAME"
info "  Configure defaults in: ~/.config/scrbblr/publish.conf"
