# Plex Re-Encoder

**GPU-accelerated HEVC (H.265) re-encoding for Plex media libraries with Dolby Vision support.**

![Version](https://img.shields.io/badge/version-2.17.0-blue)
![License](https://img.shields.io/badge/license-GPL--3.0-green)
![Python](https://img.shields.io/badge/python-3.9+-yellow)

## Features

### Encoding
- **GPU Encoding** — NVIDIA NVENC hardware acceleration (10-50x faster than realtime)
- **CPU Encoding** — libx265 software encoding when no GPU is available
- **H.265 (HEVC) & H.264** — Encode to HEVC or H.264 with configurable quality
- **Presets** — Auto, Film, Animation, Grain, Custom with per-preset CQ/bitrate/speed
- **Constant Quality (CQ)** — Configurable quality level with max bitrate cap
- **Resolution Scaling** — Downscale 4K to 1080p, 1440p, 720p, or keep original
- **Test Mode** — Encode only the first 5 minutes for quick quality checks

### HDR & Dolby Vision
- **HDR10** — Passthrough and metadata preservation
- **HLG** — Hybrid Log-Gamma support
- **Dolby Vision** — DV Profile 5, 7, 8 support with P8.4 conversion
- **DV P5 Conversion** — Convert DV P5 to P8.4 using libplacebo + Vulkan
- **HDR to DV Upgrade** — Convert HDR10 content to Dolby Vision P8.4
- **dovi_tool Integration** — Automatic RPU extraction and injection

### Audio
- **Codec Options** — Passthrough, Opus, AAC, AC3, EAC3
- **Per-Stream Control** — Configure codec and bitrate for each audio track individually
- **Audio Filters** — All tracks, first only, English only, or specific language
- **Bitrate Control** — Configurable per-codec bitrate (64k-640k)

### Multi-GPU
- **Auto Detection** — Automatically detects all NVIDIA GPUs
- **Auto Load-Balance** — Distributes across all local GPUs and remote servers equally
- **VRAM-Based Concurrency** — 1 encode per 2 GB VRAM, configurable
- **VRAM-Aware Assignment** — GPUs with ≤2GB VRAM excluded from 4K jobs (prevents CUDA OOM)
- **Idle GPU Utilization** — Jobs can use idle GPUs even when max concurrent limit is reached
- **GPU Selection** — Auto, All Local, All Remote, specific GPU, or specific server
- **OOM Recovery** — Automatic re-queue on GPU out-of-memory (up to 3 retries)

### Remote GPU (RRP)
- **Remote Encoding** — Offload encodes to remote GPU servers over TCP
- **FUSE Mount** — Stream input files on-demand (encoding starts immediately, only transfers bytes ffmpeg reads)
- **Multi-Platform** — Linux (NVENC, QSV, VAAPI) and macOS (VideoToolbox) GPU servers
- **Auto-Detect Encoder** — Server capabilities detected via ping (NVENC, VideoToolbox, QSV, AMF, CPU)
- **HMAC-SHA256 Auth** — Secure authentication with shared secret and timestamp
- **SHA256 Verification** — Output file integrity check
- **CUDA Hwaccel** — Hardware decode on NVIDIA remote GPUs
- **VideoToolbox** — Apple Silicon hardware encoding on macOS (HEVC/H.264)
- **Live Progress** — Real-time encoding progress streamed back
- **Cancel Propagation** — Cancelling kills ffmpeg on the remote server, disconnect auto-kills
- **Auto Cleanup** — Temp files removed on cancel, failure, or disconnect
- **Load Balancing** — Distribute jobs across multiple remote servers
- **GPU Server Mode** — Share your GPU with other Recode instances
- **Health Check** — Authenticated ping with live status dots and encoder type in GPU Target dropdown
- **Per-Server Controls** — Power button (green/orange/red), enable/disable per server
- **Remote GPU Jobs** — GPU servers show incoming jobs with client IP and filename
- **Single Binary** — `recode-remote` handles server, client, and ping
- **Single Port** — Everything over one TCP connection (default 9878)

### Plex Integration
- **Library Scanner** — Scan Plex libraries and detect candidates for re-encoding
- **Webhook Auto-Encode** — New media added to Plex is automatically queued
- **Post-Encode Rescan** — Plex library refreshed after each encode completes
- **Token Auto-Detection** — Finds Plex token from Preferences.xml
- **Library Profiles** — Different encode settings per Plex library

### Queue & History
- **Drag & Drop** — Reorder queue items by dragging
- **Concurrent Encodes** — Run multiple encodes simultaneously
- **Pause / Resume** — Pause individual encodes (SIGSTOP/SIGCONT)
- **Search & Filter** — Search by name, filter by codec, HDR type, status
- **Pagination** — Configurable page size for queue, history, and scan results
- **Unlimited History** — Full encode history with detailed logs
- **Retry Failed** — One-click retry for failed or cancelled jobs
- **Discard Larger** — Automatically discard encodes that are larger than the original

### Automation
- **Folder Watch** — Monitor directories for new files and auto-queue
- **Scheduled Encoding** — Set active hours (e.g., encode overnight only)
- **Auto-Start Queue** — Queue starts automatically when jobs are added
- **Library Profiles** — Per-library settings for codec, quality, resolution, audio

### Web Interface
- **Real-Time Progress** — Live encoding stats via WebSocket with interpolated progress between updates
- **Smooth Progress** — Preparing phase, decimal percentages, frame-based fallback, 1-second interpolation
- **System Monitoring** — CPU, RAM, GPU utilization, temperature, VRAM in header
- **GPU Graphs** — Per-GPU utilization, VRAM, and temperature charts with VRAM capability labels
- **Dark / Light Theme** — Toggle with header icon or in Settings
- **Mobile Responsive** — Adapts to desktop, tablet, and mobile screens
- **Settings Modal** — All settings in one place, save without closing, unsaved indicator
- **System Transcodes** — View all ffmpeg/Plex processes, remote GPU jobs on servers
- **Find Duplicates** — Detect original + encoded file pairs
- **Space Savings** — Total potential savings for scanned libraries
- **Scan Cache** — Instant re-scans with local metadata cache
- **Stats Tab** — Aggregate stats: files encoded, space saved, compression ratio

### Updates & Deployment
- **Compiled Binary** — No Python, venv, or pip required on target
- **Auto-Updates** — Daily GitHub check, one-click update with backup and live log
- **Setup Wizard** — System detection, GPU config, Plex integration, GPU Server Mode
- **Bundled Tools** — ffmpeg, ffprobe, recode-remote, dovi_tool, mediainfo, mkvmerge
- **Multi-Distro** — Ubuntu, Debian, Fedora, RHEL, Alma, Rocky, openSUSE, SLES, Arch, Manjaro
- **Per-Distro NVIDIA** — Separate driver install handlers per distribution
- **Systemd Service** — Auto-start on boot with restart watcher
- **FUSE Auto-Setup** — Installer installs fuse3 and configures fuse.conf

## Quick Install

```bash
wget -qO- https://github.com/tarquin-code/plex-recencoder/releases/latest/download/plex-recode.tar.gz | tar xz && cd plex-recode && sudo bash install.sh
```

Then open `http://your-server:9877` in a browser.

## Requirements

### Linux (Full Application)
- **NVIDIA GPU** with drivers (for GPU encoding) — CPU-only mode available
- **Linux** (RHEL/Debian/Arch-based distros)
- No Python required — ships as a compiled binary

### macOS (GPU Server Only)
- **macOS 11+** on Apple Silicon (M1/M2/M3/M4)
- **macFUSE** — `brew install macfuse` (required for FUSE mount mode)
- Uses **VideoToolbox** hardware encoder for HEVC/H.264

### Bundled Tools (no manual install needed)

**Linux:**
- `recode` — Main application binary (compiled, no Python needed)
- `ffmpeg` / `ffprobe` — Jellyfin static build with NVENC, QSV, VAAPI, libx265
- `recode-remote` — RRP remote GPU encoding (server + client + ping)
- `dovi_tool` — Dolby Vision metadata conversion
- `mkvmerge` / `mkvextract` — MKV muxing for DV encodes
- `mediainfo` — Detailed media analysis

**macOS (in `macos/` folder):**
- `recode-remote` — RRP server binary (ARM64, with FUSE support)
- `ffmpeg` / `ffprobe` — Jellyfin static build with VideoToolbox

## Remote GPU Encoding

Recode includes RRP (Recode Remote Protocol) for offloading encodes to remote GPU servers. Supports both Linux (NVENC) and macOS (VideoToolbox) servers.

### Linux GPU Server Setup

1. **Install Recode** on the GPU server (same installer)
2. **Enable GPU Server Mode** in Settings → Remote GPU, set a port and shared secret
3. **Add Remote Servers** on the client in Settings → Remote GPU
4. **Select "Remote" targets** in the GPU Target dropdown

### macOS GPU Server Setup

The Mac runs as a headless GPU server only (no web UI needed):

```bash
# 1. Install macFUSE (required for FUSE mount mode)
brew install macfuse
# Approve the kernel extension in System Settings → Privacy & Security, then reboot

# 2. Copy binaries from the macos/ folder in the release package
mkdir -p ~/recode
cp macos/recode-remote macos/ffmpeg macos/ffprobe ~/recode/
chmod +x ~/recode/*

# 3. Start the GPU server
~/recode/recode-remote server --port 9878 --secret YOUR_SECRET --ffmpeg ~/recode/ffmpeg

# 4. On the Linux Recode instance, add the Mac as a remote server:
#    Settings → Remote GPU → Add Server → enter Mac IP:9878 and the secret
```

The encoder type (VideoToolbox) is auto-detected — no manual configuration needed. The GPU Target dropdown will show `🟢 Server Name (videotoolbox)`.

To run on startup, create a launchd plist:
```bash
cat > ~/Library/LaunchAgents/com.recode.remote.plist << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.recode.remote</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/YOUR_USER/recode/recode-remote</string>
        <string>server</string>
        <string>--port</string><string>9878</string>
        <string>--secret</string><string>YOUR_SECRET</string>
        <string>--ffmpeg</string><string>/Users/YOUR_USER/recode/ffmpeg</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>StandardOutPath</key><string>/tmp/recode-remote.log</string>
    <key>StandardErrorPath</key><string>/tmp/recode-remote.log</string>
</dict>
</plist>
EOF
launchctl load ~/Library/LaunchAgents/com.recode.remote.plist
```

### How It Works

The remote server mounts input files on-demand via FUSE — encoding starts immediately without uploading. Only the bytes ffmpeg reads are transferred. Output is returned with SHA256 verification.

### Firewall

Open the RRP port (default 9878) on the GPU server:

```bash
# Linux: firewalld (RHEL/Fedora/Rocky/Alma)
firewall-cmd --permanent --add-port=9878/tcp && firewall-cmd --reload

# Linux: ufw (Ubuntu/Debian)
ufw allow 9878/tcp

# macOS
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add ~/recode/recode-remote --unblockapp ~/recode/recode-remote
```

## Configuration

On first launch, a setup wizard guides you through:
1. System capability detection (GPU, ffmpeg features, tools)
2. GPU configuration and concurrency limits
3. Plex integration (auto-detects token)
4. Default encoding settings
5. Queue behavior
6. GPU Server Mode (optional)

All settings are configurable via the web UI at any time.

## Plex Webhook

Auto-encode new media added to Plex:
1. Plex → Settings → Webhooks
2. Add: `http://your-server:9877/api/plex-webhook`
3. New media is automatically queued using library profiles

## Service Management

```bash
systemctl start recode      # Start
systemctl stop recode       # Stop
systemctl restart recode    # Restart
systemctl status recode     # Status
journalctl -u recode -f     # Live logs
```

## License

Copyright (C) 2026 Tarquin Douglass

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

See [LICENSE](LICENSE) for the full text.

### Third-Party

This software uses: ffmpeg (LGPL/GPL), dovi_tool (MIT), mkvtoolnix (GPL), NVIDIA NVENC (proprietary). Dolby, Dolby Vision, and Dolby Digital are trademarks of Dolby Laboratories. Plex is a trademark of Plex, Inc.
