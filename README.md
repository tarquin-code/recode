# Plex Re-Encoder

**GPU-accelerated HEVC (H.265) re-encoding for Plex media libraries with Dolby Vision support.**

![Version](https://img.shields.io/badge/version-2.16.0-blue)
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
- **Load Balancing** — Distributes encodes across GPUs based on availability
- **VRAM-Based Concurrency** — 1 encode per 2 GB VRAM, configurable
- **GPU Selection** — Auto, specific GPU, or all GPUs
- **OOM Recovery** — Automatic re-queue on GPU out-of-memory (up to 3 retries)

### Remote GPU (RRP)
- **Remote Encoding** — Offload encodes to remote GPU servers over TCP
- **FUSE Mount** — Stream input files on-demand (no upload wait, only reads what's needed)
- **HMAC-SHA256 Auth** — Secure authentication with shared secret and timestamp
- **SHA256 Verification** — Output file integrity check
- **CUDA Hwaccel** — Hardware decode on the remote GPU
- **Live Progress** — Real-time encoding progress streamed back
- **Cancel Propagation** — Cancelling kills ffmpeg on the remote server
- **Auto Cleanup** — Temp files removed on cancel, failure, or disconnect
- **Load Balancing** — Distribute jobs across multiple remote servers
- **GPU Server Mode** — Share your GPU with other Recode instances
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
- **Real-Time Progress** — Live encoding stats via WebSocket
- **System Monitoring** — CPU, RAM, GPU utilization, temperature, VRAM in header
- **GPU Graphs** — Per-GPU utilization, VRAM, and temperature charts
- **Dark / Light Theme** — Toggle with header icon or in Settings
- **Mobile Responsive** — Adapts to desktop, tablet, and mobile screens
- **Settings Modal** — All settings in one place, save without closing
- **Unsaved Indicator** — Shows when settings have been modified
- **System Transcodes** — View all ffmpeg/Plex processes on the system
- **Remote GPU Jobs** — See incoming RRP jobs on GPU servers with client IP
- **Find Duplicates** — Detect original + encoded file pairs
- **Space Savings** — Total potential savings for scanned libraries
- **Scan Cache** — Instant re-scans with local metadata cache

### Updates & Deployment
- **Auto-Update Check** — Daily GitHub release check with banner notification
- **One-Click Update** — Download, backup, extract, restart from the web UI
- **Setup Wizard** — First-run wizard for system detection and configuration
- **Bundled Binaries** — ffmpeg, ffprobe, recode-remote, dovi_tool, mediainfo, mkvmerge
- **Multi-Distro** — Ubuntu, Debian, Fedora, RHEL, Alma, Rocky, openSUSE, SLES, Arch, Manjaro
- **Per-Distro NVIDIA** — Separate driver install handlers per distribution
- **Systemd Service** — Auto-start on boot with restart watcher

## Quick Install

```bash
wget -qO- https://github.com/tarquin-code/plex-recencoder/releases/latest/download/plex-recode.tar.gz | tar xz && cd plex-recode && sudo bash install.sh
```

Then open `http://your-server:9877` in a browser.

## Requirements

- **Python 3.9+**
- **NVIDIA GPU** with drivers (for GPU encoding) — CPU-only mode available
- **Linux** (RHEL/Debian/Arch-based distros)

### Bundled Tools (no manual install needed)

- `ffmpeg` / `ffprobe` — Jellyfin static build with NVENC, QSV, VAAPI, libx265
- `recode-remote` — RRP remote GPU encoding (server + client + ping)
- `dovi_tool` — Dolby Vision metadata conversion
- `mkvmerge` / `mkvextract` — MKV muxing for DV encodes
- `mediainfo` — Detailed media analysis

## Remote GPU Encoding

Recode includes RRP (Recode Remote Protocol) for offloading encodes to remote GPU servers:

1. **Install Recode** on the GPU server (same installer)
2. **Enable GPU Server Mode** in Settings → Remote GPU, set a port and shared secret
3. **Add Remote Servers** on the client in Settings → Remote GPU
4. **Select "Remote" targets** in the GPU Target dropdown

The remote server mounts input files on-demand via FUSE — encoding starts immediately without uploading. Only the bytes ffmpeg reads are transferred. Output is returned with SHA256 verification.

### Firewall

Open the RRP port (default 9878) on the GPU server:

```bash
# firewalld (RHEL/Fedora/Rocky/Alma)
firewall-cmd --permanent --add-port=9878/tcp && firewall-cmd --reload

# ufw (Ubuntu/Debian)
ufw allow 9878/tcp

# iptables
iptables -A INPUT -p tcp --dport 9878 -j ACCEPT
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
