#!/bin/bash
#
# Plex Re-Encoder - install.sh
# Copyright (C) 2026 Tarquin Douglass
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Author:  Tarquin Douglass
# License: GPL-3.0-or-later
# URL:     https://github.com/tarquin-code/plex-recencoder
# Usage:   sudo bash install.sh
#
set -e

APP_NAME="Plex Re-Encoder"
APP_DIR="/opt/Recode"
SERVICE_NAME="recode"
PORT=9877
MIN_PYTHON="3.9"
# Read version from recode_server.py if available
SCRIPT_DIR_EARLY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR_EARLY}/recode_server.py" ]]; then
    APP_VERSION=$(grep -oP 'VERSION = "\K[^"]+' "${SCRIPT_DIR_EARLY}/recode_server.py" 2>/dev/null || echo "2.1.0")
else
    APP_VERSION="2.1.0"
fi

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

log()  { echo -e "${GREEN}[+]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[x]${NC} $1"; exit 1; }
info() { echo -e "${CYAN}[i]${NC} $1"; }

# ─────────────────────────────────────────────────────────────────
# Pre-flight checks
# ─────────────────────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && err "This installer must be run as root (use sudo)"

echo ""
echo -e "${BOLD}╔══════════════════════════════════════╗${NC}"
echo -e "${BOLD}║     ${CYAN}Plex Re-Encoder Installer${NC}${BOLD}        ║${NC}"
echo -e "${BOLD}║            ${CYAN}v${APP_VERSION}${NC}${BOLD}                    ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════╝${NC}"
echo ""

# Detect package manager
if command -v dnf &>/dev/null; then
    PKG_MGR="dnf"
    PKG_INSTALL="dnf install -y"
    PKG_GROUP="groupinstall"
elif command -v yum &>/dev/null; then
    PKG_MGR="yum"
    PKG_INSTALL="yum install -y"
    PKG_GROUP="groupinstall"
elif command -v apt-get &>/dev/null; then
    PKG_MGR="apt"
    PKG_INSTALL="apt-get install -y"
    PKG_GROUP=""
elif command -v zypper &>/dev/null; then
    PKG_MGR="zypper"
    PKG_INSTALL="zypper install -y"
    PKG_GROUP=""
elif command -v pacman &>/dev/null; then
    PKG_MGR="pacman"
    PKG_INSTALL="pacman -S --noconfirm"
    PKG_GROUP=""
else
    err "Unsupported package manager. Requires dnf, yum, apt-get, zypper, or pacman."
fi
log "Detected package manager: ${PKG_MGR}"

# ─────────────────────────────────────────────────────────────────
# Detect run-as user
# ─────────────────────────────────────────────────────────────────
detect_plex_user() {
    # Try running Plex process
    local plex_pid=$(pgrep -f "Plex Media Server" | head -1)
    if [[ -n "$plex_pid" ]]; then
        ps -o user= -p "$plex_pid" | tr -d ' '
        return
    fi
    # Try systemd service
    if systemctl is-active plexmediaserver &>/dev/null; then
        systemctl show plexmediaserver -p User --value 2>/dev/null | tr -d ' '
        return
    fi
    # Try common user names
    for u in plex plexmediaserver; do
        id "$u" &>/dev/null && echo "$u" && return
    done
    echo ""
}

PLEX_USER=$(detect_plex_user)
if [[ -n "$PLEX_USER" ]]; then
    log "Detected Plex user: ${BOLD}${PLEX_USER}${NC}"
    RUN_USER="$PLEX_USER"
    RUN_GROUP=$(id -gn "$PLEX_USER" 2>/dev/null || echo "$PLEX_USER")
else
    warn "Could not detect Plex user"
    # Try to detect from media files
    if [[ -d "/mnt" ]]; then
        MEDIA_OWNER=$(find /mnt -maxdepth 3 -name "*.mkv" -o -name "*.mp4" 2>/dev/null | head -1 | xargs stat -c '%U' 2>/dev/null || echo "")
        if [[ -n "$MEDIA_OWNER" && "$MEDIA_OWNER" != "root" ]]; then
            info "Media files owned by: ${BOLD}${MEDIA_OWNER}${NC}"
            RUN_USER="$MEDIA_OWNER"
            RUN_GROUP=$(id -gn "$MEDIA_OWNER" 2>/dev/null || echo "$MEDIA_OWNER")
        fi
    fi
    if [[ -z "$RUN_USER" ]]; then
        read -p "Enter the user to run Recode as [plex]: " RUN_USER
        RUN_USER=${RUN_USER:-plex}
        RUN_GROUP=${RUN_USER}
        if ! id "$RUN_USER" &>/dev/null; then
            log "Creating user ${RUN_USER}..."
            useradd -r -s /sbin/nologin "$RUN_USER" 2>/dev/null || true
        fi
    fi
fi
log "Will run as: ${BOLD}${RUN_USER}:${RUN_GROUP}${NC}"

# ─────────────────────────────────────────────────────────────────
# Install system dependencies
# ─────────────────────────────────────────────────────────────────
log "Installing system dependencies..."

case "$PKG_MGR" in
    dnf|yum)
        # Detect if Fedora or RHEL-based
        IS_FEDORA=false
        if [ -f /etc/os-release ]; then
            . /etc/os-release
            [[ "$ID" == "fedora" ]] && IS_FEDORA=true
        fi
        if $IS_FEDORA; then
            # Fedora — doesn't need EPEL, use RPM Fusion for multimedia
            if ! rpm -q rpmfusion-free-release &>/dev/null; then
                log "Enabling RPM Fusion repositories..."
                FEDORA_VER=$(rpm -E %fedora)
                dnf install -y \
                    "https://mirrors.rpmfusion.org/free/fedora/rpmfusion-free-release-${FEDORA_VER}.noarch.rpm" \
                    "https://mirrors.rpmfusion.org/nonfree/fedora/rpmfusion-nonfree-release-${FEDORA_VER}.noarch.rpm" \
                    2>/dev/null || true
            fi
        else
            # RHEL/Alma/Rocky — enable EPEL + RPM Fusion
            if ! rpm -q epel-release &>/dev/null; then
                log "Enabling EPEL repository..."
                $PKG_INSTALL epel-release 2>/dev/null || dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(rpm -E %rhel).noarch.rpm 2>/dev/null || true
            fi
            if ! rpm -q rpmfusion-free-release &>/dev/null; then
                log "Enabling RPM Fusion Free repository..."
                dnf install -y "https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-$(rpm -E %rhel).noarch.rpm" 2>/dev/null || true
            fi
        fi
        # Remove stale mkvtoolnix repo if present (we bundle it now)
        rm -f /etc/yum.repos.d/mkvtoolnix.repo 2>/dev/null
        $PKG_INSTALL python3 python3-pip python3-devel curl wget pciutils
        # mediainfo from EPEL — install separately so failure doesn't block
        $PKG_INSTALL mediainfo 2>/dev/null || warn "mediainfo not available — install via web UI"
        # mkvtoolnix is bundled in the package — no need to install from repos
        ;;
    apt)
        apt-get update -qq
        $PKG_INSTALL python3 python3-pip python3-venv mkvtoolnix mediainfo curl wget
        ;;
    zypper)
        zypper refresh
        $PKG_INSTALL python3 python3-pip python3-devel curl wget pciutils mediainfo
        ;;
    pacman)
        $PKG_INSTALL python python-pip mkvtoolnix-cli mediainfo curl wget
        ;;
esac

# ─────────────────────────────────────────────────────────────────
# Check Python version
# ─────────────────────────────────────────────────────────────────
PYTHON_BIN=$(command -v python3 || command -v python)
[[ -z "$PYTHON_BIN" ]] && err "Python 3 not found"

PYTHON_VER=$($PYTHON_BIN -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MAJOR=$($PYTHON_BIN -c "import sys; print(sys.version_info.major)")
PYTHON_MINOR=$($PYTHON_BIN -c "import sys; print(sys.version_info.minor)")

if [[ "$PYTHON_MAJOR" -lt 3 ]] || [[ "$PYTHON_MAJOR" -eq 3 && "$PYTHON_MINOR" -lt 9 ]]; then
    err "Python ${MIN_PYTHON}+ required (found ${PYTHON_VER})"
fi
log "Python ${PYTHON_VER} found at ${PYTHON_BIN}"

# ─────────────────────────────────────────────────────────────────
# Install Python packages (using venv for Python 3.12+ compatibility)
# ─────────────────────────────────────────────────────────────────
log "Installing Python packages..."
VENV_DIR="${APP_DIR}/venv"
mkdir -p "$APP_DIR"

# Always use a virtual environment — most reliable across all distros
if [[ -d "${VENV_DIR}" && -x "${VENV_DIR}/bin/python3" ]]; then
    log "Existing virtual environment found"
else
    log "Creating Python virtual environment at ${VENV_DIR}..."
    $PYTHON_BIN -m venv "$VENV_DIR" || err "Failed to create virtual environment. Install python3-venv."
fi
PYTHON_BIN="${VENV_DIR}/bin/python3"
"${VENV_DIR}/bin/pip" install --quiet --upgrade pip 2>/dev/null || true
"${VENV_DIR}/bin/pip" install --quiet fastapi uvicorn psutil requests pydantic python-multipart websockets \
    || err "Failed to install Python packages in virtual environment"
chown -R "${RUN_USER}:${RUN_GROUP}" "$VENV_DIR"

# Verify imports
$PYTHON_BIN -c "import fastapi, uvicorn, psutil, requests, pydantic" \
    || err "Failed to verify Python packages"
log "Python packages installed (venv: ${VENV_DIR})"

# ─────────────────────────────────────────────────────────────────
# Check for NVIDIA GPU (optional)
# ─────────────────────────────────────────────────────────────────
HAS_GPU=false
if command -v nvidia-smi &>/dev/null; then
    if nvidia-smi &>/dev/null; then
        GPU_COUNT=$(nvidia-smi --query-gpu=count --format=csv,noheader,nounits | head -1)
        GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader | head -1)
        log "NVIDIA GPU detected: ${BOLD}${GPU_NAME}${NC} (${GPU_COUNT} GPU(s))"
        HAS_GPU=true
    else
        warn "nvidia-smi found but GPU not accessible"
    fi
else
    warn "No NVIDIA GPU detected — CPU encoding only"
fi

# ─────────────────────────────────────────────────────────────────
# Create app directory and backup existing
# ─────────────────────────────────────────────────────────────────
if [[ -d "$APP_DIR" ]] && [[ -f "${APP_DIR}/recode_server.py" ]]; then
    info "Existing installation found at ${APP_DIR}"
    BACKUP_DIR="${APP_DIR}/backups/pre-install-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    cp "${APP_DIR}/recode_server.py" "$BACKUP_DIR/" 2>/dev/null || true
    cp "${APP_DIR}/static/index.html" "$BACKUP_DIR/" 2>/dev/null || true
    cp "${APP_DIR}/settings.json" "$BACKUP_DIR/" 2>/dev/null || true
    log "Existing files backed up to ${BACKUP_DIR}"
fi
mkdir -p "${APP_DIR}/static" "${APP_DIR}/bin"

# ─────────────────────────────────────────────────────────────────
# Symlink system tools to app bin dir
# ─────────────────────────────────────────────────────────────────
for tool in ffmpeg ffprobe mkvmerge mkvextract mediainfo nvidia-smi; do
    SYS_BIN=$(command -v "$tool" 2>/dev/null || echo "")
    # Also check /usr/local/bin
    if [[ -z "$SYS_BIN" ]] && [[ -x "/usr/local/bin/$tool" ]]; then
        SYS_BIN="/usr/local/bin/$tool"
    fi
    if [[ -n "$SYS_BIN" ]]; then
        ln -sf "$SYS_BIN" "${APP_DIR}/bin/$tool" 2>/dev/null || true
        log "${tool}: ${GREEN}found${NC} → ${APP_DIR}/bin/"
    else
        warn "${tool}: not found (install via web UI after setup)"
    fi
done

# ─────────────────────────────────────────────────────────────────
# Copy application files
# ─────────────────────────────────────────────────────────────────
# If this script is run from the repo, copy files
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/recode_server.py" ]]; then
    cp "${SCRIPT_DIR}/recode_server.py" "${APP_DIR}/"
    cp "${SCRIPT_DIR}/requirements.txt" "${APP_DIR}/" 2>/dev/null || true
    cp "${SCRIPT_DIR}/build-ffmpeg.sh" "${APP_DIR}/" 2>/dev/null || true
    cp "${SCRIPT_DIR}/LICENSE" "${APP_DIR}/" 2>/dev/null || true
    cp "${SCRIPT_DIR}/README.md" "${APP_DIR}/" 2>/dev/null || true
    mkdir -p "${APP_DIR}/static"
    cp "${SCRIPT_DIR}/static/"*.html "${APP_DIR}/static/"
    # Copy bundled binaries to app bin dir
    if [[ -d "${SCRIPT_DIR}/bin" ]]; then
        for bin_file in "${SCRIPT_DIR}/bin/"*; do
            [[ -f "$bin_file" ]] || continue
            cp "$bin_file" "${APP_DIR}/bin/"
            chmod +x "${APP_DIR}/bin/$(basename "$bin_file")"
            log "Installed $(basename "$bin_file") to ${APP_DIR}/bin/"
        done
    fi
    # Copy bundled libraries (mkvtoolnix etc.)
    if [[ -d "${SCRIPT_DIR}/lib" ]]; then
        cp -a "${SCRIPT_DIR}/lib" "${APP_DIR}/"
        log "Bundled libraries copied to ${APP_DIR}/lib/"
    fi
    # Copy static ffmpeg directory
    if [[ -d "${SCRIPT_DIR}/bin/static" ]]; then
        mkdir -p "${APP_DIR}/bin/static"
        cp "${SCRIPT_DIR}/bin/static/"* "${APP_DIR}/bin/static/" 2>/dev/null || true
        chmod +x "${APP_DIR}/bin/static/"* 2>/dev/null || true
        log "Static ffmpeg bundled to ${APP_DIR}/bin/static/"
    fi
    # Install bundled static ffmpeg as default if no system ffmpeg found
    if [[ ! -x "${APP_DIR}/bin/ffmpeg" || -L "${APP_DIR}/bin/ffmpeg" && ! -e "${APP_DIR}/bin/ffmpeg" ]]; then
        if [[ -x "${APP_DIR}/bin/static/ffmpeg" ]]; then
            ln -sf "${APP_DIR}/bin/static/ffmpeg" "${APP_DIR}/bin/ffmpeg"
            ln -sf "${APP_DIR}/bin/static/ffprobe" "${APP_DIR}/bin/ffprobe"
            log "ffmpeg: ${GREEN}using bundled static build${NC} (CPU encoding ready)"
            log "  GPU encoding requires building ffmpeg — use the Setup Wizard"
        fi
    fi
    log "Application files copied"
else
    info "Run this installer from the Recode source directory, or copy files to ${APP_DIR} manually"
fi

# ─────────────────────────────────────────────────────────────────
# Create tmp directory
# ─────────────────────────────────────────────────────────────────
TMP_DIR="/tmp/recode"
mkdir -p "$TMP_DIR"
chown "${RUN_USER}:${RUN_GROUP}" "$TMP_DIR"
log "Temp directory: ${TMP_DIR}"

# ─────────────────────────────────────────────────────────────────
# Set permissions
# ─────────────────────────────────────────────────────────────────
chown -R "${RUN_USER}:${RUN_GROUP}" "${APP_DIR}"
chmod 755 "${APP_DIR}"
log "Permissions set to ${RUN_USER}:${RUN_GROUP}"

# Allow plex user to run package managers and build scripts as root (for web UI tool installs)
SUDOERS_FILE="/etc/sudoers.d/recode"
cat > "$SUDOERS_FILE" << SUDOEOF
# Plex Re-Encoder — allow tool installation and service restart from web UI
${RUN_USER} ALL=(ALL) NOPASSWD: ALL
SUDOEOF
chmod 440 "$SUDOERS_FILE"
log "Sudoers configured for tool installs"

# ─────────────────────────────────────────────────────────────────
# Create systemd service
# ─────────────────────────────────────────────────────────────────
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

cat > "$SERVICE_FILE" << EOF
[Unit]
Description=Plex Re-Encoder Web UI
After=network.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_GROUP}
WorkingDirectory=${APP_DIR}
ExecStart=${PYTHON_BIN} ${APP_DIR}/recode_server.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
ExecStartPre=-/bin/rm -f ${APP_DIR}/.restart-flag

[Install]
WantedBy=multi-user.target
EOF

# Path watcher — restarts recode service when .restart-flag file appears
cat > "/etc/systemd/system/${SERVICE_NAME}-restart.path" << EOF
[Unit]
Description=Watch for Recode restart flag

[Path]
PathExists=${APP_DIR}/.restart-flag
Unit=${SERVICE_NAME}-restart.service

[Install]
WantedBy=multi-user.target
EOF

cat > "/etc/systemd/system/${SERVICE_NAME}-restart.service" << EOF
[Unit]
Description=Restart Recode service (triggered by flag file)

[Service]
Type=oneshot
ExecStart=/bin/systemctl restart ${SERVICE_NAME}
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}-restart.path" 2>/dev/null
systemctl start "${SERVICE_NAME}-restart.path" 2>/dev/null
log "Systemd service created: ${SERVICE_NAME} (with restart watcher)"

# ─────────────────────────────────────────────────────────────────
# Detect Plex
# ─────────────────────────────────────────────────────────────────
PLEX_TOKEN=""
PLEX_PREFS=""
for prefs_path in \
    "/var/lib/plexmediaserver/Library/Application Support/Plex Media Server/Preferences.xml" \
    "/var/lib/plex/Library/Application Support/Plex Media Server/Preferences.xml" \
    "/home/${RUN_USER}/Library/Application Support/Plex Media Server/Preferences.xml" \
    "/usr/lib/plexmediaserver/Library/Application Support/Plex Media Server/Preferences.xml"; do
    if [[ -f "$prefs_path" ]]; then
        PLEX_PREFS="$prefs_path"
        PLEX_TOKEN=$(grep -oP 'PlexOnlineToken="\K[^"]+' "$prefs_path" 2>/dev/null || echo "")
        break
    fi
done

if [[ -n "$PLEX_TOKEN" ]]; then
    log "Plex token detected from ${PLEX_PREFS}"
else
    warn "Plex token not found — configure manually in the web UI"
fi

# ─────────────────────────────────────────────────────────────────
# Enable and start/restart service
# ─────────────────────────────────────────────────────────────────
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}" 2>/dev/null

if systemctl is-active "${SERVICE_NAME}" &>/dev/null; then
    log "Recode service is already running — restarting with new version..."
    systemctl restart "${SERVICE_NAME}"
    sleep 2
    if systemctl is-active "${SERVICE_NAME}" &>/dev/null; then
        log "Service restarted successfully"
    else
        warn "Service failed to restart — check: journalctl -u ${SERVICE_NAME} -e"
    fi
else
    read -p "Start Recode service now? [Y/n]: " START_NOW
    START_NOW=${START_NOW:-Y}
    if [[ "$START_NOW" =~ ^[Yy] ]]; then
        systemctl start "${SERVICE_NAME}"
        sleep 2
        if systemctl is-active "${SERVICE_NAME}" &>/dev/null; then
            log "Service started successfully"
        else
            warn "Service failed to start — check: journalctl -u ${SERVICE_NAME} -e"
        fi
    else
        info "Service enabled but not started. Run: systemctl start ${SERVICE_NAME}"
    fi
fi

# ─────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
[[ -z "$LOCAL_IP" ]] && LOCAL_IP="your-server-ip"

echo ""
echo -e "${BOLD}╔══════════════════════════════════════╗${NC}"
echo -e "${BOLD}║     ${GREEN}Installation Complete!${NC}${BOLD}            ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BOLD}Web UI:${NC}      http://${LOCAL_IP}:${PORT}"
echo -e "  ${BOLD}Service:${NC}     systemctl {start|stop|restart} ${SERVICE_NAME}"
echo -e "  ${BOLD}Logs:${NC}        journalctl -u ${SERVICE_NAME} -f"
echo -e "  ${BOLD}Run as:${NC}      ${RUN_USER}:${RUN_GROUP}"
echo -e "  ${BOLD}Install dir:${NC} ${APP_DIR}"
if [[ "$HAS_GPU" == "true" ]]; then
    echo -e "  ${BOLD}GPU:${NC}         Detected (${GPU_COUNT} GPU(s))"
else
    echo -e "  ${BOLD}GPU:${NC}         Not detected (CPU only)"
fi
[[ -n "$PLEX_TOKEN" ]] && echo -e "  ${BOLD}Plex:${NC}        Token found"
echo ""
if [[ ! -f "${APP_DIR}/settings.json" ]]; then
    echo -e "  ${CYAN}Open the web UI to complete first-time setup.${NC}"
    echo ""
fi

# Plex webhook hint
echo -e "  ${BOLD}Plex Webhook:${NC} http://${LOCAL_IP}:${PORT}/api/plex-webhook"
echo ""
