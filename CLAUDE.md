# Recode - Claude Code Instructions

## Project Overview
Plex Re-Encoder — GPU-accelerated HEVC re-encoding web UI with Dolby Vision support, remote GPU servers via RRP (Recode Remote Protocol), and real-time WebSocket progress.

## Architecture
- **Backend**: Python (`recode_server.py`) — FastAPI + WebSocket, async encode queue
- **Frontend**: Single-file (`static/index.html`) — vanilla JS, no framework
- **Remote Protocol**: Rust (`rrp/`) — FUSE-based file streaming, reverse-connect topology
- **Tools**: ffmpeg, ffprobe, dovi_tool, mkvmerge (bundled in `bin/`)

## Key Files
- `recode_server.py` — Main server, encode queue, ffmpeg command builder, DV pipelines
- `static/index.html` — Full UI (HTML/CSS/JS in one file)
- `install.sh` — Multi-distro installer
- `package.sh` — Build release tarball
- `rrp/rrp-proto/src/lib.rs` — RRP protocol types (shared between listener/connect)
- `rrp/rrp-app/src/connect.rs` — Remote GPU connector (runs on GPU servers)
- `rrp/rrp-app/src/listener.rs` — Job dispatcher (runs on main server)
- `rrp/rrp-app/src/server.rs` — FUSE mount, ffmpeg execution, post-commands

## Servers
- **Local (main)**: This machine — runs recode_server.py + listener
- **Dev**: 114.23.41.67 — NVIDIA A16 vGPU (6x 4Q), remote GPU server
- **Tesla**: 103.18.58.206 — Tesla M6, remote GPU server
- **DS2**: 45.64.60.243 — RTX 4060, remote GPU server

## Deployment Rules
- **Never restart remote servers directly** — stage `.new` files, user restarts from GUI
- **Never overwrite running binaries** — stage as `.new`, swap on restart via `swap-staged.sh`
- **Server runs as plex user** — use `systemctl restart recode`, never start manually
- **Always restart server after backend changes** — `systemctl restart recode`
- **Frontend-only changes** — just refresh browser, no restart needed
- **Stage remotes after every backend change**: `scp recode_server.py <host>:/opt/Recode/recode_server.py.new`
- **Stage binary on remotes after Rust changes**: `scp bin/recode-remote.new <host>:/opt/Recode/bin/recode-remote.new`

## Version & Release Process
- Version lives in `recode_server.py` line ~67: `VERSION = "x.y.z"`
- Bump VERSION by 0.0.0.1 for every change before deploying - only use "w.x.y.z" format whilst developing and when releasing use "x.y.z"
- Bump VERSION by 0.0.1 for every git push and release
- When releasing update all the help, readme and installer with all changes, bump the version and commit, push to git and add a release
#- Don't bump version mid-work — bump once when ready to release
- Update About tab changelog in `static/index.html` (Version History section)
- Update `README.md` if features changed
- Package: `bash package.sh`
- Always upload both versioned and generic tarball to GitHub releases
- Release: `GH_TOKEN=<token> gh release create v<version> <files> --title "..." --notes "..."`

## DV Pipeline Summary
| Source | Mode | Pipeline |
|--------|------|----------|
| DV P5 | encode_dv | Vulkan/libplacebo color convert + NVENC → local RPU extract/inject/mux |
| DV P7/P8 | encode_dv | Full CUDA + NVENC → local RPU extract/inject/mux |
| HDR10 | encode_dv | Full CUDA + NVENC → local RPU generate/inject/mux |
| Any DV | keep | CUDA decode (no output_format for P5) + NVENC, DV NALs preserved |
| Any DV | hdr10 | Encode + strip DV NALs |
| Any DV | skip | Job immediately skipped |

Post-processing (RPU pipeline) always runs on the **local server** where source files are on local disk. Remote GPU does the encode, sends file back, then local runs RPU extraction/injection. FUSE streaming of full source for RPU extraction is too slow/unreliable.

## Coding Style
- Python: no type stubs, minimal comments, pragmatic error handling
- Rust: standard cargo project, `cargo build --release`
- Frontend: inline everything in index.html, no build step
- Keep changes minimal — don't refactor surrounding code
