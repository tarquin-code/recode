#!/bin/bash
# Package Recode for distribution
set -e

# Read version from recode_server.py — use as-is (set version before running package.sh)
VERSION=$(grep -oP 'VERSION = "\K[^"]+' /opt/Recode/recode_server.py)
[[ -z "$VERSION" ]] && VERSION="0.0.0"

# Sync version across all files
sed -i "s|version-[0-9.]*-blue|version-${VERSION}-blue|" /opt/Recode/README.md

DIST_DIR="/tmp/recode-dist"
TARBALL="/tmp/plex-recode-v${VERSION}.tar.gz"
PROD_DIR="/opt/Recode-Prod"

echo "Packaging Recode v${VERSION}..."

rm -rf "$DIST_DIR"
mkdir -p "${DIST_DIR}/plex-recode/static"

# Build Python binary via PyInstaller
echo "Building recode binary with PyInstaller..."
cd /opt/Recode
pyinstaller --onefile \
  --name recode \
  --distpath "${DIST_DIR}/plex-recode/bin" \
  --workpath /tmp/recode-pybuild \
  --specpath /tmp/recode-pybuild \
  --hidden-import uvicorn.logging \
  --hidden-import uvicorn.loops \
  --hidden-import uvicorn.loops.auto \
  --hidden-import uvicorn.protocols \
  --hidden-import uvicorn.protocols.http \
  --hidden-import uvicorn.protocols.http.auto \
  --hidden-import uvicorn.protocols.websockets \
  --hidden-import uvicorn.protocols.websockets.auto \
  --hidden-import uvicorn.lifespan \
  --hidden-import uvicorn.lifespan.on \
  --hidden-import uvicorn.lifespan.off \
  --hidden-import uvicorn.protocols.http.h11_impl \
  --hidden-import uvicorn.protocols.http.httptools_impl \
  --hidden-import uvicorn.protocols.websockets.wsproto_impl \
  --hidden-import uvicorn.protocols.websockets.websockets_impl \
  --hidden-import multipart \
  --hidden-import python_multipart \
  --hidden-import websockets \
  --hidden-import email.mime.multipart \
  --hidden-import h11 \
  --strip \
  --noconfirm \
  --clean \
  recode_server.py >/dev/null 2>&1 || { echo "PyInstaller build failed!"; exit 1; }
rm -rf /tmp/recode-pybuild
echo "  recode binary: $(du -h "${DIST_DIR}/plex-recode/bin/recode" | awk '{print $1}')"

# Write version file
echo "$VERSION" > "${DIST_DIR}/plex-recode/VERSION"

# Core app files
cp /opt/Recode/static/index.html "${DIST_DIR}/plex-recode/static/"
cp /opt/Recode/static/setup.html "${DIST_DIR}/plex-recode/static/"
cp /opt/Recode/install.sh "${DIST_DIR}/plex-recode/"
cp /opt/Recode/build-ffmpeg.sh "${DIST_DIR}/plex-recode/"
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
for binary in ffmpeg ffprobe recode-remote dovi_tool mediainfo mkvmerge mkvextract mkvpropedit; do
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

# Bundle macOS ARM64 binaries if available
if [[ -d "/opt/Recode/bin/MacOS-arm64" ]]; then
    echo "Bundling macOS ARM64 binaries..."
    mkdir -p "${DIST_DIR}/plex-recode/macos"
    cp -a /opt/Recode/bin/MacOS-arm64/* "${DIST_DIR}/plex-recode/macos/"
    for f in "${DIST_DIR}/plex-recode/macos/"*; do
        [[ -f "$f" ]] && chmod +x "$f" && echo "  $(basename "$f"): bundled ($(du -h "$f" | awk '{print $1}'))"
    done
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
