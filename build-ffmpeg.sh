#!/bin/bash
# Plex Re-Encoder — ffmpeg build script
# Builds ffmpeg with NVENC, libplacebo, Vulkan, libx265, libopus support
# Run as root. Takes 15-30 minutes depending on your system.
set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[+]${NC} $1"; }
info() { echo -e "${CYAN}[i]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[x]${NC} $1"; exit 1; }
CYAN='\033[0;36m'

# Note: when called from web UI, this runs via sudo

BUILD_DIR="/tmp/ffmpeg-build"
INSTALL_PREFIX="/usr/local"
APP_BIN="/opt/Recode/bin"

log "Installing build dependencies..."
if command -v dnf &>/dev/null; then
    # Enable EPEL and CRB/PowerTools for dev packages (RHEL only, not Fedora)
    IS_FEDORA=false
    [ -f /etc/os-release ] && . /etc/os-release && [[ "$ID" == "fedora" ]] && IS_FEDORA=true
    if ! $IS_FEDORA; then
        dnf install -y epel-release 2>/dev/null || true
        dnf config-manager --set-enabled crb 2>/dev/null || dnf config-manager --set-enabled powertools 2>/dev/null || true
    fi
    # Install available packages (some may not exist on minimal installs)
    dnf install -y gcc gcc-c++ make cmake git pkg-config \
        libdrm-devel vulkan-loader-devel \
        numactl-devel freetype-devel fribidi-devel libass-devel \
        x265-devel x264-devel \
        vulkan-headers mesa-vulkan-drivers 2>/dev/null || true
    # These may be in EPEL or CRB — install separately
    for pkg in meson ninja-build nasm yasm opus-devel lame-devel libshaderc-devel glslang-devel; do
        dnf install -y "$pkg" 2>/dev/null || warn "$pkg not available — skipping"
    done
    # If meson not found via package, install via pip
    if ! command -v meson &>/dev/null; then
        log "Installing meson via pip..."
        pip3 install --break-system-packages meson ninja 2>/dev/null || pip3 install meson ninja 2>/dev/null || true
    fi
elif command -v apt-get &>/dev/null; then
    apt-get update -qq
    apt-get install -y build-essential cmake meson ninja-build git nasm yasm pkg-config \
        libdrm-dev libvulkan-dev libshaderc-dev glslang-tools \
        libnuma-dev libfreetype-dev libfribidi-dev libass-dev \
        libopus-dev libx265-dev libx264-dev libmp3lame-dev \
        vulkan-tools mesa-vulkan-drivers 2>/dev/null
    # ffmpeg n7.1 requires Vulkan >= 1.3.277 — install newer headers if needed
    VK_VER=$(pkg-config --modversion vulkan 2>/dev/null || echo "0")
    VK_MINOR=$(echo "$VK_VER" | cut -d. -f3)
    if [[ -n "$VK_MINOR" ]] && [[ "$VK_MINOR" -lt 277 ]] 2>/dev/null; then
        log "Vulkan $VK_VER too old (need >= 1.3.277) — installing newer Vulkan headers..."
        # Install Vulkan SDK headers from LunarG
        if [[ ! -f /etc/apt/sources.list.d/lunarg-vulkan.list ]]; then
            wget -qO - https://packages.lunarg.com/lunarg-signing-key-pub.asc 2>/dev/null | apt-key add - 2>/dev/null || true
            CODENAME=$(lsb_release -cs 2>/dev/null || echo "noble")
            echo "deb https://packages.lunarg.com/vulkan/ ${CODENAME} main" > /etc/apt/sources.list.d/lunarg-vulkan.list 2>/dev/null || true
            apt-get update -qq 2>/dev/null
        fi
        apt-get install -y vulkan-headers libvulkan-dev 2>/dev/null || true
        NEW_VK=$(pkg-config --modversion vulkan 2>/dev/null || echo "unknown")
        log "Vulkan updated to: $NEW_VK"
    fi
elif command -v zypper &>/dev/null; then
    zypper install -y gcc gcc-c++ make cmake meson ninja git nasm yasm pkg-config \
        libdrm-devel libvulkan1 vulkan-devel vulkan-headers \
        libnuma-devel freetype2-devel fribidi-devel libass-devel \
        libopus-devel libx265-devel libx264-devel libmp3lame-devel \
        shaderc Mesa-vulkan-drivers 2>/dev/null || true
    # If meson not found via package, install via pip
    if ! command -v meson &>/dev/null; then
        log "Installing meson via pip..."
        pip3 install --break-system-packages meson ninja 2>/dev/null || pip3 install meson ninja 2>/dev/null || true
    fi
elif command -v pacman &>/dev/null; then
    pacman -S --noconfirm base-devel cmake meson ninja git nasm yasm pkg-config \
        libdrm vulkan-icd-loader shaderc glslang \
        numactl freetype2 fribidi libass \
        opus x265 x264 lame \
        vulkan-headers mesa 2>/dev/null
fi

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Build libplacebo
if ! pkg-config --exists libplacebo 2>/dev/null; then
    log "Building libplacebo..."
    git clone --depth 1 --branch v7.349.0 https://code.videolan.org/videolan/libplacebo.git 2>/dev/null || true
    cd libplacebo
    git submodule update --init --recursive 2>/dev/null || true
    meson setup build --prefix="$INSTALL_PREFIX" -Dvulkan=enabled -Dshaderc=enabled -Ddemos=false
    ninja -C build
    ninja -C build install
    cd "$BUILD_DIR"
    # Ensure /usr/local/lib is in the linker path
    if [[ ! -f /etc/ld.so.conf.d/usr-local.conf ]] || ! grep -q "/usr/local/lib" /etc/ld.so.conf.d/usr-local.conf 2>/dev/null; then
        echo "/usr/local/lib" > /etc/ld.so.conf.d/usr-local.conf
        echo "/usr/local/lib64" >> /etc/ld.so.conf.d/usr-local.conf
    fi
    ldconfig
    log "libplacebo installed"
else
    log "libplacebo already installed"
fi

# Check system x265 version for ffmpeg compatibility
X265_VER=$(pkg-config --modversion x265 2>/dev/null || echo "0")
log "System x265 version: ${X265_VER}"

# Detect NVIDIA GPU and headers
NVENC_OPTS=""
HAS_NVIDIA=false
if command -v nvidia-smi &>/dev/null && nvidia-smi &>/dev/null; then
    HAS_NVIDIA=true
    log "NVIDIA GPU detected"
elif lspci 2>/dev/null | grep -qi "nvidia.*\(vga\|3d\|display\)"; then
    HAS_NVIDIA=true
    log "NVIDIA GPU hardware detected (drivers may not be installed yet)"
fi

if [[ "$HAS_NVIDIA" == "true" ]]; then
    # Install nv-codec-headers if not present
    if ! pkg-config --exists ffnvcodec 2>/dev/null && [[ ! -d "/usr/include/ffnvcodec" ]]; then
        log "Installing nv-codec-headers..."
        git clone --depth 1 https://git.videolan.org/git/ffmpeg/nv-codec-headers.git 2>/dev/null || true
        if [[ -d "nv-codec-headers" ]]; then
            cd nv-codec-headers
            make install PREFIX="$INSTALL_PREFIX"
            cd "$BUILD_DIR"
            log "nv-codec-headers installed"
        fi
    fi
    # Only enable cuda-llvm if CUDA toolkit is installed
    if [[ -d "/usr/local/cuda/include" ]] || command -v nvcc &>/dev/null; then
        NVENC_OPTS="--enable-nvenc --enable-cuda-llvm"
        log "NVENC + CUDA enabled"
    else
        NVENC_OPTS="--enable-nvenc"
        log "NVENC enabled (no CUDA toolkit — cuda-llvm disabled)"
    fi
else
    warn "No NVIDIA GPU detected — building without NVENC (CPU encoding only)"
fi

# Detect optional features — Vulkan/libplacebo only useful with a GPU
VULKAN_OPTS=""
if [[ "$HAS_NVIDIA" == "true" ]] && pkg-config --exists vulkan 2>/dev/null; then
    # Test that Vulkan actually works (not just headers)
    VULKAN_TEST=$(cat <<'VTEST'
#include <vulkan/vulkan.h>
int main() { VkInstanceCreateInfo i = {0}; return 0; }
VTEST
)
    if echo "$VULKAN_TEST" | gcc -x c - -lvulkan -o /dev/null 2>/dev/null; then
        VULKAN_OPTS="--enable-vulkan"
        log "Vulkan support: enabled"
        if pkg-config --exists shaderc 2>/dev/null; then
            VULKAN_OPTS="$VULKAN_OPTS --enable-libshaderc"
            log "libshaderc support: enabled"
        fi
        if pkg-config --exists libplacebo 2>/dev/null; then
            VULKAN_OPTS="$VULKAN_OPTS --enable-libplacebo"
            log "libplacebo support: enabled"
        fi
    else
        warn "Vulkan headers present but runtime not working — skipping"
    fi
else
    if [[ "$HAS_NVIDIA" != "true" ]]; then
        info "No GPU — skipping Vulkan/libplacebo (only needed for DV P5 conversion)"
    else
        warn "Vulkan not found — building without libplacebo (DV P5 conversion unavailable)"
    fi
fi

# Ensure pkg-config can find all libraries
export PKG_CONFIG_PATH="${INSTALL_PREFIX}/lib/pkgconfig:${INSTALL_PREFIX}/lib64/pkgconfig:/usr/lib/x86_64-linux-gnu/pkgconfig:/usr/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"

# Debug: show what pkg-config finds for vulkan
log "Vulkan pkg-config check:"
pkg-config --modversion vulkan 2>&1 && log "  vulkan version: $(pkg-config --modversion vulkan)" || warn "  vulkan not found via pkg-config"
pkg-config --cflags --libs vulkan 2>&1 || true

# Build ffmpeg — pick version based on x265 API compatibility
# ffmpeg n7.1 requires x265 with multi-layer encoder_encode(... x265_picture**)
# Some distros ship x265 4.x without multi-layer support, so we compile-test
FFMPEG_BRANCH="n7.1"
X265_NEW_VER=$(pkg-config --modversion x265 2>/dev/null || echo "0")
log "System x265 version: ${X265_NEW_VER}"

X265_MULTILAYER=false
if pkg-config --exists x265 2>/dev/null; then
    X265_TEST_SRC=$(mktemp /tmp/x265_test_XXXXXX.c)
    cat > "$X265_TEST_SRC" << 'X265TEST'
#include <x265.h>
int main() {
    const x265_api *api = x265_api_get(0);
    x265_encoder *enc = NULL;
    x265_picture pic_in, *pic_out;
    x265_nal *nals;
    uint32_t num_nals;
    /* ffmpeg n7.1 passes x265_picture** as 5th arg */
    x265_picture *pic_out_ptr;
    api->encoder_encode(enc, &nals, &num_nals, &pic_in, &pic_out_ptr);
    return 0;
}
X265TEST
    X265_CF=$(pkg-config --cflags x265 2>/dev/null)
    X265_LF=$(pkg-config --libs x265 2>/dev/null)
    if gcc -fsyntax-only $X265_CF "$X265_TEST_SRC" 2>/dev/null; then
        X265_MULTILAYER=true
        log "x265 multi-layer API: supported — using ffmpeg n7.1"
    else
        log "x265 multi-layer API: not supported — using ffmpeg n7.0.2"
        FFMPEG_BRANCH="n7.0.2"
    fi
    rm -f "$X265_TEST_SRC"
else
    log "x265 not found — using ffmpeg n7.0.2"
    FFMPEG_BRANCH="n7.0.2"
fi

log "Building ffmpeg ${FFMPEG_BRANCH} (this takes 10-20 minutes)..."
rm -rf ffmpeg
git clone --depth 1 --branch "$FFMPEG_BRANCH" https://git.ffmpeg.org/ffmpeg.git 2>/dev/null || true
cd ffmpeg

run_configure() {
    local extra_opts="$1"
    ./configure \
        --prefix="$INSTALL_PREFIX" \
        --enable-gpl \
        --enable-nonfree \
        --enable-libx265 \
        --enable-libx264 \
        --enable-libopus \
        --enable-libmp3lame \
        --enable-libass \
        --enable-libfreetype \
        --enable-libfribidi \
        $extra_opts \
        $NVENC_OPTS \
        --enable-pthreads \
        --extra-cflags="-I${INSTALL_PREFIX}/include $(pkg-config --cflags vulkan 2>/dev/null)" \
        --extra-ldflags="-L${INSTALL_PREFIX}/lib $(pkg-config --libs-only-L vulkan 2>/dev/null)" \
        --extra-libs="-lpthread -lm"
}

# Try with Vulkan first, fall back without if it fails
if [[ -n "$VULKAN_OPTS" ]]; then
    log "Configuring ffmpeg (with Vulkan)..."
    if run_configure "$VULKAN_OPTS"; then
        log "Configure succeeded with Vulkan support"
    else
        warn "Configure failed with Vulkan — retrying without..."
        warn "Check the log above for details"
        make distclean 2>/dev/null || true
        VULKAN_OPTS=""
        run_configure ""
    fi
else
    log "Configuring ffmpeg..."
    run_configure ""
fi

make -j$(nproc)
make install

# Ensure /usr/local/lib is in the linker path
if [[ ! -f /etc/ld.so.conf.d/usr-local.conf ]] || ! grep -q "/usr/local/lib" /etc/ld.so.conf.d/usr-local.conf 2>/dev/null; then
    echo "/usr/local/lib" > /etc/ld.so.conf.d/usr-local.conf
    echo "/usr/local/lib64" >> /etc/ld.so.conf.d/usr-local.conf
    log "Added /usr/local/lib to linker path"
fi
ldconfig

log "ffmpeg installed to ${INSTALL_PREFIX}/bin/ffmpeg"
${INSTALL_PREFIX}/bin/ffmpeg -version | head -1

# Symlink to app bin dir
mkdir -p "$APP_BIN"
for binary in ffmpeg ffprobe; do
    ln -sf "${INSTALL_PREFIX}/bin/${binary}" "${APP_BIN}/${binary}" 2>/dev/null || true
    log "Linked ${binary} → ${APP_BIN}/"
done

echo ""
log "Verifying features..."
${INSTALL_PREFIX}/bin/ffmpeg -hide_banner -encoders 2>/dev/null | grep -q hevc_nvenc && log "  NVENC: YES" || warn "  NVENC: no"
${INSTALL_PREFIX}/bin/ffmpeg -hide_banner -filters 2>/dev/null | grep -q libplacebo && log "  libplacebo: YES" || warn "  libplacebo: no"
echo ""
log "Done! Restart Recode: systemctl restart recode"

# Cleanup
rm -rf "$BUILD_DIR"
