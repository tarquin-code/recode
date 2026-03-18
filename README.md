# Plex Re-Encoder

**GPU-accelerated HEVC (H.265) re-encoding for Plex media libraries with Dolby Vision support.**

![Version](https://img.shields.io/badge/version-2.14.0-blue)
![License](https://img.shields.io/badge/license-GPL--3.0-green)
![Python](https://img.shields.io/badge/python-3.9+-yellow)

## Features

- **GPU Encoding** — NVIDIA NVENC hardware acceleration (10-50x faster than realtime)
- **Multi-GPU** — Automatic load balancing across multiple GPUs with VRAM-based concurrency
- **Dolby Vision** — DV P5/P7/P8 to P8.4 conversion, HDR10 to DV upgrade
- **HDR Support** — HDR10, HLG, Dolby Vision passthrough and conversion
- **Audio Flexibility** — Passthrough, Opus, AAC, AC3, EAC3 with per-stream control
- **Plex Integration** — Library scanning, webhook auto-encoding, post-encode library rescan
- **Web UI** — Real-time progress, system monitoring, drag-and-drop queue management
- **Automation** — Folder watch, scheduled encoding, library profiles
- **Mobile Responsive** — Works on desktop, tablet, and mobile

## Quick Install

```bash
wget -qO- https://github.com/tarquin-code/plex-recencoder/releases/latest/download/plex-recode.tar.gz | tar xz && cd plex-recode && sudo bash install.sh
```

Then open `http://your-server:9877` in a browser.

## Requirements

- **Python 3.9+**
- **NVIDIA GPU** with drivers (for GPU encoding) — CPU-only mode available
- **ffmpeg** with NVENC + libplacebo support (for DV P5 conversion)
- **Linux** (RHEL/Debian/Arch-based distros)

### Optional Tools (auto-installed)

- `dovi_tool` — Dolby Vision metadata conversion (bundled)
- `mkvmerge` — MKV muxing for DV encodes
- `mediainfo` — Detailed media analysis

## Configuration

On first launch, a setup wizard guides you through:
1. System capability detection (GPU, ffmpeg features, tools)
2. GPU configuration and concurrency limits
3. Plex integration (auto-detects token)
4. Default encoding settings
5. Queue behavior

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
