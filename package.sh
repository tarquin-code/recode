#!/bin/bash
# Package Plex Re-Encoder for distribution
set -e

# Read version from recode_server.py and increment patch
VERSION=$(grep -oP 'VERSION = "\K[^"]+' /opt/Recode/recode_server.py)
[[ -z "$VERSION" ]] && VERSION="0.0.0"
IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION"
PATCH=$((PATCH + 1))
VERSION="${MAJOR}.${MINOR}.${PATCH}"
# Write new version back to recode_server.py
sed -i "s/^VERSION = \".*\"/VERSION = \"${VERSION}\"/" /opt/Recode/recode_server.py

DIST_DIR="/tmp/recode-dist"
TARBALL="/tmp/plex-recode-v${VERSION}.tar.gz"
PROD_DIR="/opt/Recode-Prod"

echo "Packaging Plex Re-Encoder v${VERSION}..."

rm -rf "$DIST_DIR"
mkdir -p "${DIST_DIR}/plex-recode/static"

# Core app files
cp /opt/Recode/recode_server.py "${DIST_DIR}/plex-recode/"
cp /opt/Recode/static/index.html "${DIST_DIR}/plex-recode/static/"
cp /opt/Recode/static/setup.html "${DIST_DIR}/plex-recode/static/"
cp /opt/Recode/install.sh "${DIST_DIR}/plex-recode/"
cp /opt/Recode/build-ffmpeg.sh "${DIST_DIR}/plex-recode/"
cp /opt/Recode/requirements.txt "${DIST_DIR}/plex-recode/"
cp /opt/Recode/LICENSE "${DIST_DIR}/plex-recode/"
cp /opt/Recode/README.md "${DIST_DIR}/plex-recode/"

# Update dovi_tool if not present in bin/
if [[ ! -f "/opt/Recode/bin/dovi_tool" || -L "/opt/Recode/bin/dovi_tool" ]]; then
    echo "Downloading dovi_tool..."
    DOVI_URL=$(curl -sL https://api.github.com/repos/quietvoid/dovi_tool/releases/latest \
        | grep "browser_download_url.*x86_64-unknown-linux-musl" | grep -v ".sha256" | head -1 | cut -d'"' -f4)
    if [[ -n "$DOVI_URL" ]]; then
        mkdir -p /tmp/dovi_dl
        curl -sL "$DOVI_URL" -o /tmp/dovi_dl/dovi_tool.tar.gz
        tar -xzf /tmp/dovi_dl/dovi_tool.tar.gz -C /tmp/dovi_dl 2>/dev/null
        DOVI_BIN=$(find /tmp/dovi_dl -name "dovi_tool" -type f | head -1)
        if [[ -n "$DOVI_BIN" ]]; then
            rm -f "/opt/Recode/bin/dovi_tool"
            cp "$DOVI_BIN" "/opt/Recode/bin/dovi_tool"
            chmod +x "/opt/Recode/bin/dovi_tool"
            echo "  dovi_tool downloaded"
        fi
        rm -rf /tmp/dovi_dl
    fi
else
    echo "dovi_tool: already in bin/"
fi

# Download mkvtoolnix from AppImage only if not already in bin/
if [[ ! -f "/opt/Recode/bin/mkvmerge" || -L "/opt/Recode/bin/mkvmerge" ]]; then
    echo "Downloading mkvtoolnix..."
    MKVTOOLNIX_URL="https://mkvtoolnix.download/appimage/MKVToolNix_GUI-97.0-x86_64.AppImage"
    MKV_TMP="/tmp/mkvtoolnix_dl"
    mkdir -p "$MKV_TMP"
    if curl -sL "$MKVTOOLNIX_URL" -o "${MKV_TMP}/mkvtoolnix.AppImage"; then
        chmod +x "${MKV_TMP}/mkvtoolnix.AppImage"
        cd "$MKV_TMP"
        ./mkvtoolnix.AppImage --appimage-extract >/dev/null 2>&1
        if [[ -f "squashfs-root/usr/bin/mkvmerge" ]]; then
            # Create wrapper scripts that set LD_LIBRARY_PATH
            MKVLIB_DIR="${DIST_DIR}/plex-recode/lib/mkvtoolnix"
            mkdir -p "$MKVLIB_DIR"
            cp squashfs-root/usr/bin/mkvmerge squashfs-root/usr/bin/mkvextract squashfs-root/usr/bin/mkvpropedit "$MKVLIB_DIR/"
            cp -a squashfs-root/usr/lib/*.so* "$MKVLIB_DIR/" 2>/dev/null || true
            for tool in mkvmerge mkvextract mkvpropedit; do
                cat > "${DIST_DIR}/plex-recode/bin/${tool}" << WRAPPER
#!/bin/bash
SCRIPT_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="\${SCRIPT_DIR}/../lib/mkvtoolnix"
LD_LIBRARY_PATH="\${LIB_DIR}:\${LD_LIBRARY_PATH}" exec "\${LIB_DIR}/${tool}" "\$@"
WRAPPER
                chmod +x "${DIST_DIR}/plex-recode/bin/${tool}"
            done
            echo "  mkvtoolnix downloaded from AppImage"
        fi
    fi
    cd /tmp
    rm -rf "$MKV_TMP"
else
    echo "mkvtoolnix: already in bin/"
fi

# Bundle all binaries from /opt/Recode/bin/ (except nvidia-smi which is driver-specific)
echo "Bundling binaries..."
mkdir -p "${DIST_DIR}/plex-recode/bin"
for binary in ffmpeg ffprobe ffmpeg-over-ip-client ffmpeg-over-ip-server dovi_tool mediainfo mkvmerge mkvextract mkvpropedit; do
    if [[ -f "/opt/Recode/bin/${binary}" && ! -L "/opt/Recode/bin/${binary}" ]]; then
        cp "/opt/Recode/bin/${binary}" "${DIST_DIR}/plex-recode/bin/${binary}"
        chmod +x "${DIST_DIR}/plex-recode/bin/${binary}"
        echo "  ${binary}: bundled"
    elif [[ -f "/opt/Recode/bin/${binary}" ]]; then
        echo "  ${binary}: skipped (symlink)"
    fi
done
if [[ -f "${DIST_DIR}/plex-recode/bin/ffmpeg" ]]; then
    FFMPEG_VER=$("${DIST_DIR}/plex-recode/bin/ffmpeg" -version 2>/dev/null | head -1 || echo "unknown")
    echo "  ffmpeg version: ${FFMPEG_VER}"
fi

# Create tarball
cd "$DIST_DIR"
tar -czf "$TARBALL" plex-recode/
rm -rf "$DIST_DIR"

# Copy to Recode-Prod (versioned + fixed "latest" name)
mkdir -p "$PROD_DIR"
cp -f "$TARBALL" "$PROD_DIR/"
cp -f "$TARBALL" "$PROD_DIR/plex-recode.tar.gz"

echo ""
echo "Package created: ${PROD_DIR}/plex-recode-v${VERSION}.tar.gz"
echo "Latest copy:     ${PROD_DIR}/plex-recode.tar.gz"
echo "Size: $(du -h "$TARBALL" | awk '{print $1}')"
echo ""
echo "Installation:"
echo "  tar -xzf plex-recode.tar.gz"
echo "  cd plex-recode"
echo "  sudo bash install.sh"
