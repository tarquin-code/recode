#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Plex Re-Encoder - recode_server.py
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
"""
Plex Re-Encoder Server v2.0.0
FastAPI backend for H.265 re-encoding with GPU acceleration,
Dolby Vision conversion, and real-time WebSocket progress.

Author:  Tarquin Douglass
License: GPL-3.0-or-later
URL:     https://github.com/tarquin-code/plex-recencoder
"""

import asyncio
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import uuid
import shutil
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Optional

import logging
import re
from collections import deque

import psutil
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import requests as http_requests
import uvicorn

# =============================================================================
# Configuration & Constants
# =============================================================================
# When running from PyInstaller, __file__ points to a temp dir — use the working directory instead
if getattr(sys, 'frozen', False):
    BASE_DIR = os.getcwd()
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

VERSION = "2.21.4"
BIN_DIR = os.path.join(BASE_DIR, "bin")
os.makedirs(BIN_DIR, exist_ok=True)


def _find_bin(name: str) -> str:
    """Find a binary: check app bin dir first, then /usr/local/bin, then system PATH."""
    app_bin = os.path.join(BIN_DIR, name)
    if os.path.isfile(app_bin) and os.access(app_bin, os.X_OK):
        return app_bin
    local_bin = f"/usr/local/bin/{name}"
    if os.path.isfile(local_bin) and os.access(local_bin, os.X_OK):
        return local_bin
    return shutil.which(name) or name


FFMPEG = _find_bin("ffmpeg")
FFPROBE = _find_bin("ffprobe")
DOVI_TOOL = _find_bin("dovi_tool")

VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".mpg", ".mpeg", ".ts", ".m2ts", ".vob", ".3gp", ".ogv", ".divx", ".asf", ".f4v"}
HOSTNAME = socket.gethostname().split(".")[0]  # short hostname, e.g. "streamer"

# Encoded output file tag: e.g. "_h265_HDR10_Streamer"
ENCODE_TAG_RE = re.compile(r"_h26[45]_[A-Za-z0-9-]+(?:_[A-Za-z0-9-]+){0,3}$")  # matches encoded output stems (2-5 parts)


def build_encode_tag(video_codec: str, info: dict, dv_mode: str = "skip", resize: str = "original") -> str:
    """Build the filename tag for encoded output, e.g. '_h265_1080p_DV-P8_recode'."""
    codec_tag = "h265" if video_codec == "hevc" else "h264"

    # Resolution tag
    if resize and resize != "original":
        res_tag = resize  # e.g. "1080p", "720p"
    else:
        w = info.get("width", 0)
        if w >= 3840:
            res_tag = "2160p"
        elif w >= 2560:
            res_tag = "1440p"
        elif w >= 1920:
            res_tag = "1080p"
        elif w >= 1280:
            res_tag = "720p"
        else:
            res_tag = "480p"

    hdr_type = info.get("hdr_type", "SDR")
    is_dv = hdr_type.startswith("Dolby Vision")

    if dv_mode == "encode_dv" and (is_dv or hdr_type == "HDR10"):
        hdr_tag = "DV-P8"
    elif dv_mode == "keep" and is_dv:
        hdr_tag = "DV"
    elif dv_mode == "hdr10" and is_dv:
        hdr_tag = "HDR10"
    elif hdr_type == "HDR10":
        hdr_tag = "HDR10"
    elif hdr_type == "HLG":
        hdr_tag = "HLG"
    elif is_dv:
        hdr_tag = "DV"
    else:
        hdr_tag = "SDR"

    suffix = app_settings.get("encode_suffix", "recode")
    return f"_{codec_tag}_{res_tag}_{hdr_tag}_{suffix}"


def is_encoded_output(filename: str) -> bool:
    """Check if a filename looks like one of our encoded outputs."""
    return bool(ENCODE_TAG_RE.search(os.path.splitext(filename)[0]))

MANIFEST_NAME = ".recode.json"

def read_recode_manifest(directory: str) -> dict:
    """Read the .recode.json manifest from a directory. Returns {source_filename: {...info...}}."""
    mpath = os.path.join(directory, MANIFEST_NAME)
    try:
        with open(mpath) as f:
            return json.load(f)
    except Exception:
        return {}

def write_recode_manifest_entry(source_path: str, output_path: str, version: str = None):
    """Add/update an entry in the .recode.json manifest for a completed encode."""
    directory = os.path.dirname(source_path)
    source_name = os.path.basename(source_path)
    mpath = os.path.join(directory, MANIFEST_NAME)
    manifest = read_recode_manifest(directory)
    manifest[source_name] = {
        "output": os.path.basename(output_path),
        "encoded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "version": version or VERSION,
    }
    try:
        with open(mpath, "w") as f:
            json.dump(manifest, f, indent=2)
    except Exception as e:
        log.warning(f"Failed to write recode manifest: {e}")

PRESETS = {
    "stream":    {"cq": 20, "maxbitrate": "25M", "speed": "p6", "desc": "High quality 1080p streaming"},
    "4kstream":  {"cq": 20, "maxbitrate": "50M", "speed": "p5", "desc": "High quality 4K streaming"},
    "archive":   {"cq": 18, "maxbitrate": "40M", "speed": "p7", "desc": "Maximum quality, slow encode"},
    "slow":      {"cq": 20, "maxbitrate": "30M", "speed": "p7", "desc": "Slow encode, best compression at high quality"},
    "small":     {"cq": 28, "maxbitrate": "10M", "speed": "p4", "desc": "Smaller files, acceptable quality"},
    "fast":      {"cq": 30, "maxbitrate": "8M",  "speed": "p2", "desc": "Fastest encode, lower quality"},
    # Balanced presets — good quality vs size for each resolution
    "4k_balanced":   {"cq": 24, "maxbitrate": "35M", "speed": "p5", "desc": "4K balanced — great quality, ~40-50% savings"},
    "4k_compact":    {"cq": 28, "maxbitrate": "20M", "speed": "p5", "desc": "4K compact — good quality, ~55-65% savings"},
    "1080_balanced": {"cq": 22, "maxbitrate": "15M", "speed": "p5", "desc": "1080p balanced — great quality, ~45-55% savings"},
    "1080_compact":  {"cq": 26, "maxbitrate": "8M",  "speed": "p5", "desc": "1080p compact — good quality, ~60-70% savings"},
    "720_balanced":  {"cq": 24, "maxbitrate": "8M",  "speed": "p5", "desc": "720p balanced — great quality, ~50-60% savings"},
    "sd_balanced":   {"cq": 26, "maxbitrate": "4M",  "speed": "p4", "desc": "SD balanced — great quality, ~50-60% savings"},
    # HQ presets — near-transparent quality, slower encode
    "4k_hq":         {"cq": 18, "maxbitrate": "60M", "speed": "p6", "desc": "4K HQ — near-transparent, slow"},
    "1080_hq":       {"cq": 18, "maxbitrate": "25M", "speed": "p6", "desc": "1080p HQ — near-transparent, slow"},
    "720_hq":        {"cq": 20, "maxbitrate": "15M", "speed": "p6", "desc": "720p HQ — near-transparent, slow"},
    # Max compression presets — slow encode, best quality per byte
    "4k_maxsave":    {"cq": 30, "maxbitrate": "15M", "speed": "p7", "desc": "4K max savings — good quality, ~65-75% savings, very slow"},
    "1080_maxsave":  {"cq": 28, "maxbitrate": "6M",  "speed": "p7", "desc": "1080p max savings — good quality, ~70-80% savings, very slow"},
    "720_maxsave":   {"cq": 28, "maxbitrate": "4M",  "speed": "p7", "desc": "720p max savings — good quality, ~65-75% savings, very slow"},
    "sd_maxsave":    {"cq": 30, "maxbitrate": "2M",  "speed": "p7", "desc": "SD max savings — good quality, ~65-75% savings, very slow"},
}

NVENC_TO_X265 = {
    "p1": "ultrafast", "p2": "ultrafast", "p3": "veryfast",
    "p4": "fast", "p5": "medium", "p6": "slow", "p7": "veryslow",
}

AUTO_PRESETS = {
    "4k":    {"cq": 20, "maxbitrate": "50M", "speed": "p5"},
    "1080p": {"cq": 20, "maxbitrate": "20M", "speed": "p6"},
    "sd":    {"cq": 30, "maxbitrate": "8M",  "speed": "p2"},
}


# =============================================================================
# Plex Integration
# =============================================================================

PLEX_URL = "http://localhost:32400"
PLEX_PREFS_FILE = "/var/lib/plexmediaserver/Library/Application Support/Plex Media Server/Preferences.xml"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
log = logging.getLogger("recode")

# Ring buffer for in-memory log capture (last 500 entries)
class _LogBuffer(logging.Handler):
    def __init__(self, maxlen=500):
        super().__init__()
        from collections import deque
        self.buffer = deque(maxlen=maxlen)
    def emit(self, record):
        self.buffer.append({
            "ts": record.created,
            "level": record.levelname,
            "msg": self.format(record),
        })
_log_buffer = _LogBuffer()
_log_buffer.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%d-%m-%Y %H:%M:%S"))
logging.getLogger().addHandler(_log_buffer)

def read_plex_token():
    """Read Plex token from Plex Preferences.xml."""
    try:
        with open(PLEX_PREFS_FILE, "r") as f:
            prefs = f.read()
        match = re.search(r'PlexOnlineToken="([^"]+)"', prefs)
        if match:
            log.info("Plex token loaded from Preferences.xml")
            return match.group(1)
        log.warning("Could not find PlexOnlineToken in Preferences.xml")
        return None
    except FileNotFoundError:
        log.warning(f"Plex Preferences.xml not found at: {PLEX_PREFS_FILE}")
        return None
    except Exception as e:
        log.error(f"Error reading Plex token: {e}")
        return None

PLEX_TOKEN = read_plex_token()

def plex_headers():
    return {"X-Plex-Token": PLEX_TOKEN, "Accept": "application/json"}

def get_library_section_for_path(file_path: str) -> Optional[str]:
    """Find which Plex library section contains the given file path."""
    if not PLEX_TOKEN:
        return None
    try:
        r = http_requests.get(f"{PLEX_URL}/library/sections", headers=plex_headers(), timeout=10)
        r.raise_for_status()
        sections = r.json().get("MediaContainer", {}).get("Directory", [])
        for section in sections:
            for location in section.get("Location", []):
                if file_path.startswith(location["path"]):
                    return section["key"]
    except Exception as e:
        log.error(f"Failed to get library sections: {e}")
    return None

def trigger_plex_rescan(section_key: str):
    """Tell Plex to rescan a specific library section."""
    if not PLEX_TOKEN:
        return
    try:
        r = http_requests.get(
            f"{PLEX_URL}/library/sections/{section_key}/refresh",
            headers=plex_headers(), timeout=10
        )
        r.raise_for_status()
        log.info(f"Triggered Plex rescan for section {section_key}")
    except Exception as e:
        log.error(f"Failed to trigger Plex rescan: {e}")

# Webhook default encode settings
SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")

APP_DEFAULTS = {
    # Encode defaults (sidebar)
    "preset": "auto",
    "cq": 24,
    "maxbitrate": "20M",
    "speed": "p5",
    "encoder": "gpu",
    "video_codec": "hevc",
    "skip_4k": False,
    "hdr_only": False,
    "dv_mode": "skip",  # "skip", "keep" (preserve DV metadata), "hdr10" (convert to HDR10), "encode_dv" (DV→P8.4)
    "discard_larger": False,
    "delete_original": False,
    "tmp_dir": "/var/lib/plex/tmp",
    # Audio defaults
    "audio_filter": "all",
    "audio_codec": "libopus",
    "audio_bitrate": "448k",
    "english_only": False,
    "audio_lang_mode": "all",  # "all" or "langs"
    "audio_langs": "eng",
    # Subtitle defaults
    "subtitle_mode": "all",  # "all", "langs", or "none"
    "subtitle_langs": "eng",
    # Concurrent encoding
    "max_concurrent_encodes": 1,
    "auto_start_queue": False,
    # Scheduled encoding (24h format, empty = disabled)
    "schedule_enabled": False,
    "schedule_start": "00:00",
    "schedule_end": "08:00",
    # Folder watch
    "watch_enabled": False,
    "watch_paths": [],
    "watch_interval": 300,
    # Library profiles: { "library_path": { preset, cq, ... } }
    "library_profiles": {},
    # Theme
    "theme": "dark",
    # Allowed paths for scanning/browsing
    "allowed_paths": ["/mnt"],
    # Test mode: limit encodes to 5 minutes for quick iteration
    "test_mode": False,
    # Suffix appended to encoded filenames (e.g. "recode" → Movie_h265_HDR10_recode.mkv)
    "encode_suffix": "recode",
    # Remote GPU servers (RRP)
    # Each: {"name": "Server A", "address": "192.168.1.100:5050", "secret": "shared-secret"}
    "remote_gpu_servers": [],  # [{name, address, secret, enabled, transfer_mode}]
    # GPU server mode — allow other Recode instances to use this machine's GPU
    "ffmpeg_server_enabled": False,
    "ffmpeg_server_port": 9878,
    "ffmpeg_server_secret": "",
    # Disabled GPUs — list of GPU indices to exclude from encoding
    "disabled_gpus": [],
    # Per-GPU max concurrent jobs — {"0": 1, "1": 2} — if not set, auto-calculated from VRAM
    "gpu_max_jobs": {},
    # Remote Clients (reverse-connect) — GPU servers behind NAT connect to us
    "remote_client_enabled": False,
    "remote_client_port": 9879,
    "remote_client_secret": "",
}

def load_settings() -> dict:
    """Load settings from JSON file, merging with defaults."""
    settings = APP_DEFAULTS.copy()
    try:
        with open(SETTINGS_FILE, "r") as f:
            saved = json.load(f)
        settings.update(saved)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    # Migrate convert_dv (bool) → dv_mode (str)
    if "convert_dv" in settings:
        if settings.pop("convert_dv", False):
            settings.setdefault("dv_mode", "hdr10")
    # Backfill missing keys in library profiles so webhooks/folder-watch work correctly
    profile_defaults = {
        "preset": settings.get("preset", "auto"),
        "video_codec": settings.get("video_codec", "hevc"),
        "encoder": settings.get("encoder", "gpu"),
        "resize": settings.get("resize", "original"),
        "speed": settings.get("speed", "p5"),
        "cq": settings.get("cq", 24),
        "maxbitrate": settings.get("maxbitrate", "20M"),
        "audio_filter": settings.get("audio_filter", "all"),
        "audio_codec": settings.get("audio_codec", "libopus"),
        "audio_bitrate": settings.get("audio_bitrate", "448k"),
        "skip_4k": False, "hdr_only": False, "dv_mode": settings.get("dv_mode", "skip"),
        "discard_larger": False, "delete_original": False, "english_only": False,
    }
    for lpath, lprofile in settings.get("library_profiles", {}).items():
        for k, v in profile_defaults.items():
            if k not in lprofile:
                lprofile[k] = v
    # Clean empty remote GPU servers
    settings["remote_gpu_servers"] = [
        s for s in settings.get("remote_gpu_servers", [])
        if s.get("name", "").strip() and s.get("address", "").strip() and s.get("secret", "").strip()
    ]
    return settings

def save_settings(settings: dict):
    """Save settings to JSON file."""
    # Clean empty remote GPU servers before saving
    settings["remote_gpu_servers"] = [
        s for s in settings.get("remote_gpu_servers", [])
        if s.get("name", "").strip() and s.get("address", "").strip() and s.get("secret", "").strip()
    ]
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)
def _get_remote_ffmpeg_bin(server_index: int) -> str:
    """Get the RRP remote binary."""
    return _find_bin("recode-remote")

def _get_remote_ffmpeg_env(server_index: int) -> dict:
    """Get environment variables for the RRP remote client."""
    servers = app_settings.get("remote_gpu_servers", [])
    if server_index < len(servers):
        srv = servers[server_index]
        addr = (srv.get("address") or "").strip()
        secret = (srv.get("secret") or "").strip()
        # Append default port if not specified
        if addr and ":" not in addr.rsplit(".", 1)[-1]:
            addr = f"{addr}:9878"
        return {"RRP_SERVER_ADDRESS": addr, "RRP_SERVER_SECRET": secret}
    return {}

app_settings = load_settings()

def is_path_allowed(path: str) -> bool:
    """Check if a path is under one of the configured allowed paths."""
    resolved = str(Path(path).resolve())
    allowed = app_settings.get("allowed_paths", ["/mnt"])
    return any(resolved.startswith(str(Path(p).resolve())) for p in allowed)

def build_default_profile() -> dict:
    """Build the default encode profile from app_settings."""
    return {
        "preset": app_settings.get("preset", "auto"),
        "cq": app_settings.get("cq", 24),
        "maxbitrate": app_settings.get("maxbitrate", "20M"),
        "speed": app_settings.get("speed", "p5"),
        "encoder": app_settings.get("encoder", "gpu"),
        "use_cpu": app_settings.get("encoder", "gpu") == "cpu",
        "video_codec": app_settings.get("video_codec", "hevc"),
        "dv_mode": app_settings.get("dv_mode", "skip"),
        "skip_4k": app_settings.get("skip_4k", False),
        "hdr_only": app_settings.get("hdr_only", False),
        "delete_original": app_settings.get("delete_original", False),
        "discard_larger": app_settings.get("discard_larger", False),
        "tmp_dir": app_settings.get("tmp_dir", "/var/lib/plex/tmp"),
        "english_only": app_settings.get("english_only", False),
        "audio_filter": app_settings.get("audio_filter", "all"),
        "audio_codec": app_settings.get("audio_codec", "libopus"),
        "audio_bitrate": app_settings.get("audio_bitrate", "448k"),
        "resize": app_settings.get("resize", "original"),
    }

WEBHOOK_DEFAULTS = build_default_profile()

# Track recently processed webhook files to avoid duplicate processing
_webhook_processed: dict[str, float] = {}  # file_path -> timestamp when processed
WEBHOOK_DEDUP_SECS = 300  # Ignore duplicate file paths within this window


# =============================================================================
# Data Models
# =============================================================================

class JobStatus(str, Enum):
    QUEUED = "queued"
    ENCODING = "encoding"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class ScanRequest(BaseModel):
    path: str
    skip_4k: bool = False
    hdr_only: bool = False
    dv_mode: str = "skip"


class AudioStreamConfig(BaseModel):
    index: int
    include: bool = True
    codec: str = "auto"       # "auto", "copy", "libopus", "aac", "ac3", "eac3"
    bitrate: str = "192k"     # e.g. "192k", "256k", "320k", "448k", "640k"


class QueueAddRequest(BaseModel):
    files: list[str]
    preset: str = "auto"
    cq: int = 24
    maxbitrate: str = "20M"
    speed: str = "p5"
    encoder: str = "gpu"
    use_cpu: bool = False
    gpu_id: str = "auto"
    gpu_target: str = "auto"
    video_codec: str = "hevc"
    dv_mode: str = "skip"
    resize: str = "original"
    skip_4k: bool = False
    hdr_only: bool = False
    delete_original: bool = False
    discard_larger: bool = False
    english_only: bool = False
    audio_filter: str = "all"
    audio_codec: str = "libopus"
    audio_bitrate: str = "448k"
    tmp_dir: str = "/var/lib/plex/tmp"
    # Per-file audio config: { "filepath": [ {index, include, codec, bitrate}, ... ] }
    audio_config: dict[str, list[AudioStreamConfig]] = {}
    # Per-file subtitle config: { "filepath": [ {index, include}, ... ] }
    subtitle_config: dict[str, list[dict]] = {}
    # Pre-built file info from scan results (avoids re-probing)
    file_info: dict[str, dict] = {}


class EstimateRequest(BaseModel):
    path: str
    preset: str = "auto"
    cq: int = 24
    maxbitrate: str = "20M"
    speed: str = "p5"
    use_cpu: bool = False
    dv_mode: str = "skip"
    sample_secs: int = 60


class ReorderRequest(BaseModel):
    job_ids: list[str]


@dataclass
class AudioStream:
    index: int
    codec: str
    language: str
    title: str
    action: str  # "copy" or "libopus"
    reason: str


@dataclass
class FileInfo:
    path: str
    filename: str
    dirname: str
    size_bytes: int
    size_human: str
    codec: str
    width: int
    height: int
    resolution_label: str
    pix_fmt: str
    hdr_type: str
    is_hdr: bool
    color_transfer: str
    color_primaries: str
    duration_secs: float
    audio_streams: list[dict]
    sub_streams: list[dict]
    is_hevc: bool
    has_dovi: bool
    dovi_profile: Optional[int]
    hdr10_metadata: dict
    output_exists: bool
    recode_tag: str = ""


@dataclass
class EncodeProgress:
    pct: int = 0
    elapsed_secs: float = 0
    eta_secs: float = 0
    speed: str = "0x"
    bitrate: str = "0kbits/s"
    frame: int = 0
    current_time: float = 0
    total_time: float = 0


@dataclass
class EncodeJob:
    id: str
    file_info: dict
    settings: dict
    status: str = JobStatus.QUEUED
    progress: dict = field(default_factory=lambda: asdict(EncodeProgress()))
    result: dict = field(default_factory=dict)
    error: str = ""
    started_at: float = 0
    finished_at: float = 0
    paused: bool = False


# =============================================================================
# Media Probe Functions
# =============================================================================

async def probe_file(path: str) -> dict:
    """Run ffprobe to extract video and audio stream info.
    Uses -read_intervals to only scan the first few frames for speed."""
    cmd = [
        FFPROBE, "-v", "error",
        "-read_intervals", "%+#1",
        "-show_entries", "stream=index,codec_type,codec_name,width,height,pix_fmt,color_transfer,color_primaries,channels,sample_rate,duration,bit_rate",
        "-show_entries", "stream_tags=language,title",
        "-show_entries", "stream_side_data=side_data_type",
        "-show_entries", "format=duration,bit_rate",
        "-show_entries", "format_tags=RECODE",
        "-of", "json",
        path
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    data = json.loads(stdout.decode())

    # If audio streams have no bit_rate, do a full format probe to get stream bit_rates
    streams = data.get("streams", [])
    needs_bitrate = any(
        s.get("codec_type") == "audio" and not s.get("bit_rate")
        for s in streams
    )
    if needs_bitrate:
        cmd2 = [
            FFPROBE, "-v", "error",
            "-show_entries", "stream=index,codec_type,bit_rate",
            "-of", "json",
            path
        ]
        proc2 = await asyncio.create_subprocess_exec(
            *cmd2, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout2, _ = await proc2.communicate()
        try:
            data2 = json.loads(stdout2.decode())
            br_map = {s["index"]: s.get("bit_rate") for s in data2.get("streams", []) if s.get("bit_rate")}
            for s in streams:
                if not s.get("bit_rate") and s.get("index") in br_map:
                    s["bit_rate"] = br_map[s["index"]]
        except Exception:
            pass

    return data


async def detect_dolby_vision(path: str) -> tuple[bool, Optional[int]]:
    """Check for Dolby Vision using mediainfo. Returns (has_dovi, profile_number)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "mediainfo", "--Inform=Video;%HDR_Format/String%", path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        text = stdout.decode().lower()
        if "dolby vision" not in text:
            return False, None
        # Extract profile number — mediainfo outputs e.g. "Dolby Vision, Version 1.0, dvhe.05.06, ..."
        # or "Dolby Vision / ... / dvhe.05 ..." — profile is after "dvhe." or "dvav."
        import re
        m = re.search(r'dvhe\.(\d+)|dvav\.(\d+)', text)
        if m:
            profile = int(m.group(1) or m.group(2))
            return True, profile
        # Fallback: look for "profile X" pattern
        m = re.search(r'profile\s+(\d+)', text)
        if m:
            return True, int(m.group(1))
        return True, None
    except Exception:
        return False, None


def resolution_label(w: int, h: int) -> str:
    if w >= 3840 or h >= 2160:
        return "4K"
    elif w >= 2560 or h >= 1440:
        return "1440p"
    elif w >= 1920 or h >= 1080:
        return "1080p"
    elif w >= 1280 or h >= 720:
        return "720p"
    elif h >= 480:
        return "480p"
    elif h >= 360:
        return "360p"
    return "SD"


def patch_dvvc_compat_id(mkv_path: str, compat_id: int = 4):
    """Patch the dvvC configuration record in an MKV to set the DV compatibility_id.
    mkvmerge always writes compatibility_id=1; this fixes it to the desired value (default 4 = HLG/P8.4).
    """
    try:
        with open(mkv_path, "rb") as f:
            data = f.read()
        # dvvC config: 01 00 [profile<<1] [level/flags] [compat_id<<4]
        # P8: profile=8 → byte2=0x10, level6 → byte3=0x35, compat=1 → byte4=0x10
        target = bytes([0x01, 0x00, 0x10, 0x35, 0x10])
        pos = data.find(target)
        if pos >= 0:
            patched = bytearray(data)
            patched[pos + 4] = (compat_id << 4) & 0xF0
            with open(mkv_path, "wb") as f:
                f.write(patched)
            log.info(f"Patched dvvC compatibility_id to {compat_id} at offset {pos}")
        else:
            log.warning(f"dvvC config record not found in {mkv_path} — skipping patch")
    except Exception as e:
        log.warning(f"Failed to patch dvvC in {mkv_path}: {e}")


def human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024.0:
            return f"{nbytes:.2f} {unit}"
        nbytes /= 1024.0
    return f"{nbytes:.2f} PB"


def classify_audio(codec: str, title: str) -> tuple[str, str]:
    """Decide whether to copy or re-encode an audio stream."""
    is_atmos = "atmos" in (title or "").lower()
    if codec == "truehd" or is_atmos:
        return "copy", "TrueHD/Atmos — preserving"
    elif codec == "eac3":
        return "copy", "EAC3/DD+ — preserving"
    elif codec == "ac3":
        return "copy", "AC3/DD — preserving"
    else:
        return "libopus", f"{codec} → Opus"


async def get_file_info(path: str) -> Optional[FileInfo]:
    """Full file analysis combining ffprobe + mediainfo (run in parallel)."""
    try:
        # Run ffprobe and mediainfo concurrently
        data, dovi_result = await asyncio.gather(
            probe_file(path),
            detect_dolby_vision(path),
        )
        has_dovi, dovi_profile = dovi_result
    except Exception:
        return None

    streams = data.get("streams", [])
    fmt = data.get("format", {})

    # Find first video stream
    video = None
    for s in streams:
        if s.get("codec_type") == "video":
            video = s
            break
    if not video:
        return None

    codec = video.get("codec_name", "unknown")
    w = int(video.get("width", 0))
    h = int(video.get("height", 0))
    pix_fmt = video.get("pix_fmt", "unknown")
    ct = video.get("color_transfer", "")
    cp = video.get("color_primaries", "")

    # Also check ffprobe side_data for DV (fallback) and extract HDR10 metadata
    hdr10_metadata = {}
    for sd in video.get("side_data_list", []):
        sdt = sd.get("side_data_type", "").lower()
        if "dolby" in sdt and not has_dovi:
            has_dovi = True
        elif "content light level" in sdt:
            hdr10_metadata["max_cll"] = int(sd.get("max_content", 0))
            hdr10_metadata["max_fall"] = int(sd.get("max_average", 0))
        elif "mastering display" in sdt:
            try:
                max_lum = sd.get("max_luminance", "1000/1")
                min_lum = sd.get("min_luminance", "1/10000")
                # Parse fractions like "4000/1" and "1/200"
                mn, md = min_lum.split("/")
                xn, xd = max_lum.split("/")
                hdr10_metadata["min_lum"] = round(int(mn) / int(md), 4)
                hdr10_metadata["max_lum"] = round(int(xn) / int(xd))
            except Exception:
                pass

    # Duration
    dur = video.get("duration")
    if not dur or dur == "N/A":
        dur = fmt.get("duration", "0")
    try:
        dur = float(dur)
    except (ValueError, TypeError):
        dur = 0.0

    # HDR detection
    is_hdr = False
    hdr_type = "SDR"
    if ct == "smpte2084":
        is_hdr = True
        hdr_type = "HDR10"
    elif ct == "arib-std-b67":
        is_hdr = True
        hdr_type = "HLG"

    if has_dovi:
        is_hdr = True
        hdr_type = f"Dolby Vision P{dovi_profile}" if dovi_profile is not None else "Dolby Vision"

    # Audio streams
    audio_streams = []
    audio_idx = 0
    for s in streams:
        if s.get("codec_type") != "audio":
            continue
        acodec = s.get("codec_name", "unknown")
        tags = s.get("tags", {})
        lang = tags.get("language", "und")
        title = tags.get("title", "")
        action, reason = classify_audio(acodec, title)
        # Audio bitrate and estimated size
        abit_rate = 0
        try:
            abit_rate = int(s.get("bit_rate", 0) or 0)
        except (ValueError, TypeError):
            pass
        asize = int(abit_rate * dur / 8) if abit_rate > 0 and dur > 0 else 0
        audio_streams.append({
            "index": audio_idx,
            "codec": acodec,
            "language": lang,
            "title": title,
            "action": action,
            "reason": reason,
            "channels": s.get("channels", 0),
            "bit_rate": abit_rate,
            "bit_rate_human": f"{abit_rate // 1000}k" if abit_rate > 0 else "N/A",
            "size_bytes": asize,
            "size_human": human_size(asize) if asize > 0 else "N/A",
        })
        audio_idx += 1

    # Subtitle streams — track codec names for filtering unsupported codecs
    sub_streams = []
    sub_idx = 0
    for s in streams:
        if s.get("codec_type") != "subtitle":
            continue
        sub_streams.append({
            "index": sub_idx,
            "codec": s.get("codec_name", "unknown"),
            "language": (s.get("tags") or {}).get("language", "und"),
            "title": (s.get("tags") or {}).get("title", ""),
        })
        sub_idx += 1

    p = Path(path)
    size = p.stat().st_size
    nameonly = p.stem
    dirname = str(p.parent)
    # Check if this file was encoded by Recode — RECODE metadata tag in MKV container
    recode_tag = fmt.get("tags", {}).get("RECODE", "")
    output_exists = bool(recode_tag)

    return FileInfo(
        path=path,
        filename=p.name,
        dirname=dirname,
        size_bytes=size,
        size_human=human_size(size),
        codec=codec,
        width=w,
        height=h,
        resolution_label=resolution_label(w, h),
        pix_fmt=pix_fmt,
        hdr_type=hdr_type,
        is_hdr=is_hdr,
        color_transfer=ct,
        color_primaries=cp,
        duration_secs=dur,
        audio_streams=audio_streams,
        sub_streams=sub_streams,
        is_hevc=(codec == "hevc"),
        has_dovi=has_dovi,
        dovi_profile=dovi_profile,
        hdr10_metadata=hdr10_metadata,
        output_exists=output_exists,
        recode_tag=recode_tag,
    )


def compute_suggestion(info: FileInfo) -> dict:
    """Analyze a file and suggest whether re-encoding would save space at similar quality.
    Returns {"level": "high"|"medium"|"low"|null, "text": str, "savings_pct": int}"""
    codec = info.codec
    w, h = info.width, info.height
    dur = info.duration_secs
    size = info.size_bytes

    if dur <= 0 or size <= 0 or w <= 0:
        return {"level": None, "text": "", "savings_pct": 0}

    # Estimate audio size to get video-only bitrate
    audio_size = sum(a.get("size_bytes", 0) for a in info.audio_streams)
    video_size = max(size - audio_size, size * 0.8)  # fallback: assume 80% video
    video_bitrate_mbps = (video_size * 8) / dur / 1_000_000

    pixels = w * h
    # Bits per pixel per second (bpp) — key quality metric
    bpp = (video_size * 8) / dur / pixels if pixels > 0 else 0

    # Reference H.265 bpp targets for good quality (CQ 20-24 range)
    # These are approximate — actual results vary by content
    h265_ref_bpp = 0.07  # good quality H.265 baseline
    if info.is_hdr:
        h265_ref_bpp = 0.09  # HDR needs ~25% more bits

    # Codec efficiency multipliers (relative to H.265)
    # Higher = less efficient = more savings potential
    codec_factor = {
        "mpeg2video": 3.5, "mpeg4": 2.8, "vc1": 2.5, "msmpeg4v3": 3.0,
        "wmv3": 2.5, "vp8": 2.2, "h264": 1.6, "vp9": 1.1, "hevc": 1.0,
        "av1": 0.85,
    }.get(codec, 1.5)

    if codec == "hevc" or codec == "av1":
        # Already efficient codec — only suggest if bitrate is very high
        if bpp > h265_ref_bpp * 2.5:
            savings = int((1 - (h265_ref_bpp * 1.3) / bpp) * 100)
            savings = min(savings, 70)
            return {"level": "low", "text": f"High bitrate HEVC — ~{savings}% smaller possible", "savings_pct": savings}
        return {"level": None, "text": "", "savings_pct": 0}

    # For non-HEVC codecs, estimate savings
    current_equivalent_h265_bpp = bpp / codec_factor
    # If current content is already very low bitrate, savings will be minimal
    if current_equivalent_h265_bpp < h265_ref_bpp * 0.5:
        return {"level": "low", "text": f"Low bitrate {codec.upper()} — small savings possible", "savings_pct": 10}

    expected_h265_bpp = max(h265_ref_bpp, current_equivalent_h265_bpp * 0.85)
    savings = int((1 - expected_h265_bpp / bpp) * 100)
    savings = max(0, min(savings, 80))

    if savings >= 40:
        level = "high"
        text = f"{codec.upper()} → H.265 — ~{savings}% smaller"
    elif savings >= 20:
        level = "medium"
        text = f"{codec.upper()} → H.265 — ~{savings}% smaller"
    elif savings >= 10:
        level = "low"
        text = f"{codec.upper()} → H.265 — ~{savings}% smaller"
    else:
        return {"level": None, "text": "", "savings_pct": 0}

    return {"level": level, "text": text, "savings_pct": savings}


# =============================================================================
# Scan Cache (SQLite)
# =============================================================================

CACHE_DB_PATH = Path(__file__).parent / "scan_cache.db"

def get_cache_db() -> sqlite3.Connection:
    """Open (and create if needed) the scan cache database."""
    conn = sqlite3.connect(str(CACHE_DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS file_cache (
        path            TEXT PRIMARY KEY,
        mtime           REAL NOT NULL,
        size_bytes      INTEGER NOT NULL,
        filename        TEXT NOT NULL,
        dirname         TEXT NOT NULL,
        size_human      TEXT NOT NULL,
        codec           TEXT NOT NULL,
        width           INTEGER NOT NULL,
        height          INTEGER NOT NULL,
        resolution_label TEXT NOT NULL,
        pix_fmt         TEXT NOT NULL,
        hdr_type        TEXT NOT NULL,
        is_hdr          INTEGER NOT NULL,
        color_transfer  TEXT NOT NULL,
        color_primaries TEXT NOT NULL,
        duration_secs   REAL NOT NULL,
        audio_streams   TEXT NOT NULL,
        is_hevc         INTEGER NOT NULL,
        has_dovi        INTEGER NOT NULL,
        dovi_profile    INTEGER,
        suggestion_level TEXT,
        suggestion_text  TEXT NOT NULL DEFAULT '',
        savings_pct      INTEGER NOT NULL DEFAULT 0
    )""")
    conn.commit()
    # Migrate: add dovi_profile column if missing
    try:
        conn.execute("ALTER TABLE file_cache ADD COLUMN dovi_profile INTEGER")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
    # Migrate: add sub_streams column if missing
    try:
        conn.execute("ALTER TABLE file_cache ADD COLUMN sub_streams TEXT NOT NULL DEFAULT '[]'")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
    # Migrate: add recode_tag column if missing
    try:
        conn.execute("ALTER TABLE file_cache ADD COLUMN recode_tag TEXT NOT NULL DEFAULT ''")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
    return conn


def save_to_cache(conn: sqlite3.Connection, info: "FileInfo", suggestion: dict):
    """Save a probed file's info to the cache."""
    try:
        st = os.stat(info.path)
        conn.execute("""INSERT OR REPLACE INTO file_cache
            (path, mtime, size_bytes, filename, dirname, size_human, codec, width, height,
             resolution_label, pix_fmt, hdr_type, is_hdr, color_transfer, color_primaries,
             duration_secs, audio_streams, sub_streams, is_hevc, has_dovi, dovi_profile,
             suggestion_level, suggestion_text, savings_pct, recode_tag)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (info.path, st.st_mtime, info.size_bytes, info.filename, info.dirname,
             info.size_human, info.codec, info.width, info.height,
             info.resolution_label, info.pix_fmt, info.hdr_type, int(info.is_hdr),
             info.color_transfer, info.color_primaries, info.duration_secs,
             json.dumps(info.audio_streams), json.dumps(info.sub_streams),
             int(info.is_hevc), int(info.has_dovi),
             info.dovi_profile,
             suggestion.get("level"), suggestion.get("text", ""), suggestion.get("savings_pct", 0),
             info.recode_tag))
        conn.commit()
    except Exception:
        pass


def cache_row_to_dict(row: sqlite3.Row) -> dict:
    """Convert a cache DB row to the frontend result dict."""
    p = Path(row["path"])
    nameonly = p.stem
    dirname = Path(row["dirname"])
    # Check RECODE metadata tag from cache
    try:
        recode_tag = row["recode_tag"] or ""
    except (IndexError, KeyError):
        recode_tag = ""
    output_exists = bool(recode_tag)
    return {
        "path": row["path"],
        "filename": row["filename"],
        "dirname": row["dirname"],
        "size_bytes": row["size_bytes"],
        "size_human": row["size_human"],
        "codec": row["codec"],
        "width": row["width"],
        "height": row["height"],
        "resolution_label": row["resolution_label"],
        "pix_fmt": row["pix_fmt"],
        "hdr_type": row["hdr_type"],
        "is_hdr": bool(row["is_hdr"]),
        "color_transfer": row["color_transfer"],
        "color_primaries": row["color_primaries"],
        "duration_secs": row["duration_secs"],
        "audio_streams": json.loads(row["audio_streams"]),
        "sub_streams": json.loads(row["sub_streams"]) if "sub_streams" in row.keys() else [],
        "is_hevc": bool(row["is_hevc"]),
        "has_dovi": bool(row["has_dovi"]),
        "dovi_profile": row["dovi_profile"] if "dovi_profile" in row.keys() else None,
        "output_exists": output_exists,
        "suggestion": {
            "level": row["suggestion_level"],
            "text": row["suggestion_text"],
            "savings_pct": row["savings_pct"],
        },
    }


# =============================================================================
# FFmpeg Command Builder
# =============================================================================

def resolve_preset(preset_name: str, width: int, height: int) -> dict:
    """Resolve preset name to CQ/bitrate/speed values."""
    if preset_name == "auto":
        if width >= 3840 or height >= 2160:
            return {**AUTO_PRESETS["4k"], "resolved": "4kstream"}
        elif width >= 1920 or height >= 1080:
            return {**AUTO_PRESETS["1080p"], "resolved": "stream"}
        else:
            return {**AUTO_PRESETS["sd"], "resolved": "fast"}
    elif preset_name in PRESETS:
        p = PRESETS[preset_name]
        return {"cq": p["cq"], "maxbitrate": p["maxbitrate"], "speed": p["speed"], "resolved": preset_name}
    else:
        return {"cq": 24, "maxbitrate": "20M", "speed": "p5", "resolved": "custom"}


def build_ffmpeg_cmd(info: dict, settings: dict, ffmpeg_bin: str = None) -> tuple[list[str], str]:
    """Build the full ffmpeg command. Returns (cmd_list, output_path)."""
    if ffmpeg_bin is None:
        ffmpeg_bin = FFMPEG
    path = info["path"]
    p = Path(path)
    nameonly = p.stem
    dirname = info["dirname"]
    tmp_dir = app_settings.get("tmp_dir", settings.get("tmp_dir", "/var/lib/plex/tmp"))
    os.makedirs(tmp_dir, exist_ok=True)

    video_codec = settings.get("video_codec", "hevc")
    dv_mode = settings.get("dv_mode", "skip")
    encode_tag = build_encode_tag(video_codec, info, dv_mode, settings.get("resize", "original"))
    output_file = str(Path(dirname) / f"{nameonly}{encode_tag}.mkv")
    tmp_output = str(Path(tmp_dir) / f"{nameonly}{encode_tag}.mkv")

    preset_name = settings.get("preset", "auto")
    use_cpu = settings.get("use_cpu", False)

    # Resolve preset
    if preset_name in ("auto",) or preset_name in PRESETS:
        resolved = resolve_preset(preset_name, info["width"], info["height"])
        cq = resolved["cq"]
        maxbitrate = resolved["maxbitrate"]
        speed = resolved["speed"]
    else:
        cq = settings.get("cq", 24)
        maxbitrate = settings.get("maxbitrate", "20M")
        speed = settings.get("speed", "p5")

    # Build command
    cmd = [ffmpeg_bin, "-y", "-nostdin"]

    # Hardware acceleration for GPU only
    dovi_p5 = info.get("dovi_profile") == 5

    is_remote = settings.get("_remote_server_idx", -1) >= 0
    remote_encoder_type = settings.get("_remote_encoder_type", "nvenc")
    is_videotoolbox = is_remote and remote_encoder_type == "videotoolbox"

    # Get target server's Vulkan capability from reported capabilities
    _target_has_vulkan = _has_libplacebo  # local default
    if is_remote:
        remote_gpu_name = settings.get("_remote_gpu_name", "")
        try:
            _lsf = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-status.json")
            with open(_lsf) as _f:
                for _rg in json.load(_f).get("gpus", []):
                    if _rg.get("name") == remote_gpu_name:
                        for _rc in _rg.get("gpu_capabilities", []):
                            if "vulkan_libplacebo" in _rc:
                                _target_has_vulkan = _rc["vulkan_libplacebo"]
                                break
                        break
        except Exception:
            pass

    if not use_cpu:
        pix_fmt = info.get("pix_fmt", "unknown")
        cuda_safe_fmts = ("yuv420p", "nv12", "p010le", "yuv420p10le")
        gpu_id = str(settings.get("gpu_id", 0))
        if is_remote or gpu_id == "-1":
            gpu_id = "0"  # remote assign_gpu rewrites device IDs
        if is_remote and is_videotoolbox:
            # VideoToolbox — no hwaccel flags needed (Apple handles decode internally)
            pass
        elif dovi_p5 and dv_mode not in ("skip", "keep") and _target_has_vulkan:
            # DV Profile 5 — Vulkan/libplacebo for color conversion + NVENC encode
            # No CUDA hwaccel — Vulkan and CUDA compete for GPU resources at 4K
            log.info(f"DV Profile 5 — Vulkan libplacebo + GPU encode (GPU {gpu_id})")
            cmd += ["-init_hw_device", f"vulkan=vk:{gpu_id}", "-filter_hw_device", "vk"]
        elif pix_fmt in cuda_safe_fmts:
            cmd += ["-hwaccel", "cuda", "-hwaccel_device", gpu_id, "-extra_hw_frames", "16"]
            if not dovi_p5:
                # Full CUDA pipeline — frames stay in GPU memory
                cmd += ["-hwaccel_output_format", "cuda"]
            else:
                # DV P5: CUDA decode to system memory — DOVI config record in container
                # causes auto_scale filter conflict with hwaccel_output_format cuda
                log.info("DV P5: CUDA decode without output_format cuda (DOVI config incompatible)")
        else:
            log.info(f"Pixel format '{pix_fmt}' — using software decode + GPU encode to avoid color issues")

    # Test mode: limit to 5 minutes for quick iteration
    if app_settings.get("test_mode"):
        cmd += ["-t", "300"]
        log.info("Test mode enabled — limiting encode to 5 minutes")

    cmd += ["-i", path]

    # Map video
    cmd += ["-map", "0:v:0"]

    # Map and configure audio — use custom config if provided, else defaults
    audio_streams = info.get("audio_streams", [])
    custom_audio = settings.get("audio_config", [])
    audio_filter = settings.get("audio_filter", "all")

    # Build effective audio config per stream
    out_audio_idx = 0
    for i, astream in enumerate(audio_streams):
        # Find custom config for this stream index
        custom = None
        for ca in custom_audio:
            if ca.get("index") == i:
                custom = ca
                break

        # Check if stream is included (per-stream toggle)
        if custom and not custom.get("include", True):
            continue  # Skip this stream entirely

        # Apply audio type filter
        if audio_filter != "all":
            acodec = astream.get("codec", "").lower()
            atitle = astream.get("title", "").lower()
            channels = astream.get("channels", 2)
            is_atmos = "atmos" in atitle
            match = False
            if audio_filter == "truehd":
                match = acodec == "truehd" or is_atmos
            elif audio_filter == "eac3":
                match = acodec == "eac3"
            elif audio_filter == "ac3":
                match = acodec == "ac3"
            elif audio_filter == "aac":
                match = acodec == "aac"
            elif audio_filter == "stereo":
                match = channels <= 2
            elif audio_filter == "surround":
                match = channels > 2
            if not match:
                log.info(f"Audio stream {i}: filtered out ({acodec}, {channels}ch, filter={audio_filter})")
                continue

        cmd += ["-map", f"0:a:{i}"]

        # Determine codec
        if custom and custom.get("codec", "auto") != "auto":
            codec_choice = custom["codec"]
            bitrate = custom.get("bitrate", settings.get("audio_bitrate", "448k"))
            log.info(f"Audio stream {i}: explicit codec={codec_choice}, bitrate={bitrate} (from per-stream config)")
        else:
            # Use the profile's audio codec setting; only copy if codec is explicitly "copy"
            default_codec = settings.get("audio_codec", "libopus")
            if default_codec == "copy":
                codec_choice = "copy"
            else:
                # Re-encode all streams to the chosen codec
                codec_choice = default_codec
            bitrate = settings.get("audio_bitrate", "448k")
            log.info(f"Audio stream {i}: codec={codec_choice}, bitrate={bitrate} (source={astream.get('codec','?')}, default_action={astream['action']})")

        if codec_choice == "copy":
            cmd += [f"-c:a:{out_audio_idx}", "copy"]
        elif codec_choice == "libopus":
            # libopus max bitrate is 512k — clamp if higher
            opus_br = bitrate
            try:
                br_val = int(bitrate.lower().rstrip("k")) if bitrate.lower().endswith("k") else int(bitrate) // 1000
                if br_val > 512:
                    opus_br = "512k"
                    log.info(f"Audio stream {i}: clamped bitrate {bitrate} → 512k (libopus max)")
            except (ValueError, TypeError):
                pass
            cmd += [f"-c:a:{out_audio_idx}", "libopus", f"-b:a:{out_audio_idx}", opus_br,
                    f"-filter:a:{out_audio_idx}", "aformat=channel_layouts=7.1|5.1|stereo|mono"]
        elif codec_choice == "aac":
            cmd += [f"-c:a:{out_audio_idx}", "aac", f"-b:a:{out_audio_idx}", bitrate]
        elif codec_choice == "ac3":
            cmd += [f"-c:a:{out_audio_idx}", "ac3", f"-b:a:{out_audio_idx}", bitrate]
        elif codec_choice == "eac3":
            cmd += [f"-c:a:{out_audio_idx}", "eac3", f"-b:a:{out_audio_idx}", bitrate]
        else:
            # Fallback — copy
            cmd += [f"-c:a:{out_audio_idx}", "copy"]

        out_audio_idx += 1

    # Map subtitles — skip streams with unsupported codecs (e.g. codec 94213)
    # mov_text (tx3g) is MP4-only and must be converted to srt for MKV output
    # Respects subtitle_config if provided (include/exclude per track)
    supported_sub_codecs = {"srt", "subrip", "ass", "ssa", "mov_text", "webvtt", "dvd_subtitle", "dvdsub", "hdmv_pgs_subtitle", "pgssub", "text", "ttml"}
    sub_streams = info.get("sub_streams", [])
    sub_config = settings.get("subtitle_config", [])
    sub_codec_overrides = []
    mapped_sub_idx = 0
    if sub_streams:
        for ss in sub_streams:
            codec = ss["codec"].lower()
            if codec not in supported_sub_codecs:
                continue
            # Check subtitle_config — if provided, only include tracks marked as include
            if sub_config:
                scfg = next((sc for sc in sub_config if sc.get("index") == ss["index"]), None)
                if scfg and not scfg.get("include", True):
                    continue
            cmd += ["-map", f"0:s:{ss['index']}"]
            if codec == "mov_text":
                sub_codec_overrides += [f"-c:s:{mapped_sub_idx}", "srt"]
            mapped_sub_idx += 1
    else:
        # No sub stream info (e.g. old queue entries) — skip subs to avoid codec 94213 failures
        pass

    # Video encoding args
    is_h264 = video_codec == "h264"
    cpu_threads = app_settings.get("cpu_threads", 0)
    if use_cpu:
        cpu_preset = NVENC_TO_X265.get(speed, "medium")
        if is_h264:
            cmd += [
                "-c:v", "libx264", "-crf", str(cq),
                "-preset", cpu_preset, "-profile:v", "high",
                "-maxrate", maxbitrate, "-bufsize", maxbitrate,
            ]
        else:
            cmd += [
                "-c:v", "libx265", "-crf", str(cq),
                "-preset", cpu_preset, "-profile:v", "main10",
                "-maxrate", maxbitrate, "-bufsize", maxbitrate,
            ]
        if cpu_threads and cpu_threads > 0:
            cmd += ["-threads", str(cpu_threads)]
    elif is_videotoolbox:
        # Apple VideoToolbox hardware encoder
        # Use bitrate-based encoding (FFmpeg 8+ dropped -q:v for VideoToolbox)
        if is_h264:
            cmd += [
                "-c:v", "h264_videotoolbox",
                "-b:v", maxbitrate,
                "-allow_sw", "1",
            ]
        else:
            cmd += [
                "-c:v", "hevc_videotoolbox",
                "-b:v", maxbitrate,
                "-tag:v", "hvc1",
                "-allow_sw", "1",
            ]
    else:
        gpu_id = str(settings.get("gpu_id", 0))
        if is_h264:
            cmd += [
                "-c:v", "h264_nvenc", "-rc", "vbr", "-cq", str(cq),
                "-preset", speed, "-profile:v", "high",
                "-b:v", "0", "-bufsize", maxbitrate, "-maxrate", maxbitrate,
                "-multipass", "qres", "-spatial-aq", "1", "-temporal-aq", "1",
                "-aq-strength", "8",
            ]
        else:
            cmd += [
                "-c:v", "hevc_nvenc", "-rc", "vbr", "-cq", str(cq),
                "-preset", speed, "-profile:v", "main10",
                "-b:v", "0", "-bufsize", maxbitrate, "-maxrate", maxbitrate,
                "-multipass", "qres", "-spatial-aq", "1", "-temporal-aq", "1",
                "-aq-strength", "8",
            ]
        # Only add -gpu flag for local GPU encoding, not remote
        if not is_remote:
            cmd += ["-gpu", gpu_id]
        # Limit CPU threads for GPU encodes — GPU does the heavy lifting,
        # only need a few CPU threads for demux/mux/BSF pipeline
        cmd += ["-threads", "8"]

    # HDR / DV metadata (HEVC only — H.264 does not support HDR/DV)
    hdr_type = info.get("hdr_type", "SDR")
    if not is_h264:
        if dovi_p5 and dv_mode not in ("skip", "keep"):
            if _target_has_vulkan:
                # DV Profile 5: libplacebo converts IPTPQc2 → BT.2020+PQ (proper color conversion)
                cmd += [
                    "-vf", "hwupload,libplacebo=colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084:format=yuv420p10le,hwdownload,format=yuv420p10le",
                    "-pix_fmt", "p010le",
                    "-color_primaries", "bt2020", "-color_trc", "smpte2084",
                    "-colorspace", "bt2020nc", "-color_range", "tv",
                    "-bsf:v", "filter_units=remove_types=62",
                ]
            elif dv_mode == "hdr10":
                # No libplacebo — strip DV NALs and set HDR10 metadata, let ffmpeg handle color
                log.warning(f"DV P5 without libplacebo — stripping DV, color may not be accurate")
                # Strip CUDA hwaccel — pix_fmt p010le conflicts with hwaccel_output_format cuda
                cuda_flags = {"-hwaccel", "-hwaccel_output_format", "-hwaccel_device", "-extra_hw_frames"}
                cmd = [c for i, c in enumerate(cmd) if not (
                    c in cuda_flags
                    or c == "cuda"
                    or (c == "16" and i > 0 and cmd[i-1] == "-extra_hw_frames")
                    or (i > 0 and cmd[i-1] in cuda_flags)
                )]
                cmd += [
                    "-pix_fmt", "p010le",
                    "-color_primaries", "bt2020", "-color_trc", "smpte2084",
                    "-colorspace", "bt2020nc", "-color_range", "tv",
                    "-bsf:v", "filter_units=remove_types=62",
                ]
            else:
                # encode_dv mode requires libplacebo for proper P5→P8 conversion — fall back to HDR10
                log.warning(f"DV P5 encode_dv without Vulkan — falling back to HDR10 conversion")
                # Strip CUDA hwaccel — pix_fmt p010le conflicts with hwaccel_output_format cuda
                cuda_flags = {"-hwaccel", "-hwaccel_output_format", "-hwaccel_device", "-extra_hw_frames"}
                cmd = [c for i, c in enumerate(cmd) if not (
                    c in cuda_flags
                    or c == "cuda"
                    or (c == "16" and i > 0 and cmd[i-1] == "-extra_hw_frames")
                    or (i > 0 and cmd[i-1] in cuda_flags)
                )]
                cmd += [
                    "-pix_fmt", "p010le",
                    "-color_primaries", "bt2020", "-color_trc", "smpte2084",
                    "-colorspace", "bt2020nc", "-color_range", "tv",
                    "-bsf:v", "filter_units=remove_types=62",
                ]
        elif hdr_type.startswith("Dolby Vision") and dv_mode == "keep":
            # Keep original DV: preserve RPU NAL units, just set color metadata
            log.info(f"DV Keep Original — preserving DV metadata through re-encode")
            cmd += [
                "-color_primaries", "bt2020", "-color_trc", "smpte2084",
                "-colorspace", "bt2020nc",
            ]
        elif hdr_type.startswith("Dolby Vision") and dv_mode not in ("skip", "keep"):
            # DV P7/P8: strip DV NALs during encode, preserve HDR10 color metadata
            # encode_dv mode adds RPU extraction/injection post-process
            cmd += [
                "-color_primaries", "bt2020", "-color_trc", "smpte2084",
                "-colorspace", "bt2020nc",
                "-bsf:v", "filter_units=remove_types=62",
            ]
        elif info.get("is_hdr", False):
            cp = info.get("color_primaries", "")
            ct = info.get("color_transfer", "")
            if cp:
                cmd += ["-color_primaries", cp]
            if ct:
                cmd += ["-color_trc", ct]
            cmd += ["-colorspace", "bt2020nc"]

    # Resize if requested
    resize = settings.get("resize", "original")
    if resize and resize != "original":
        res_map = {
            "2160p": (3840, 2160), "1440p": (2560, 1440),
            "1080p": (1920, 1080), "720p": (1280, 720), "480p": (854, 480),
        }
        target = res_map.get(resize)
        if target:
            src_w, src_h = info.get("width", 0), info.get("height", 0)
            tgt_w, tgt_h = target
            if (src_w, src_h) != (tgt_w, tgt_h):
                # Check if -vf already exists in cmd (DV P5 libplacebo)
                vf_idx = None
                for i, c in enumerate(cmd):
                    if c == "-vf":
                        vf_idx = i
                        break
                # Strip CUDA hwaccel for resize — scale_cuda fails on some pixel formats
                # Software decode + scale + GPU encode is reliable across all formats
                has_cuda_hwaccel = "-hwaccel" in cmd and "cuda" in cmd
                if has_cuda_hwaccel:
                    cuda_flags = {"-hwaccel", "-hwaccel_output_format", "-hwaccel_device", "-extra_hw_frames"}
                    cmd = [c for i, c in enumerate(cmd) if not (
                        c in cuda_flags
                        or c == "cuda"
                        or (c == "16" and i > 0 and cmd[i-1] == "-extra_hw_frames")
                        or (i > 0 and cmd[i-1] in cuda_flags)
                    )]
                    # Re-find vf index after stripping
                    vf_idx = None
                    for i, c in enumerate(cmd):
                        if c == "-vf":
                            vf_idx = i
                            break
                    log.info(f"Resize: stripped CUDA hwaccel for software scale")
                scale_filter = f"scale={tgt_w}:{tgt_h}:flags=lanczos"
                if vf_idx is not None:
                    cmd[vf_idx + 1] = cmd[vf_idx + 1] + "," + scale_filter
                else:
                    cmd += ["-vf", scale_filter]
                log.info(f"Resize: {'scale_cuda (GPU)' if has_cuda_hwaccel else 'software scale'}")
                direction = "downscale" if (tgt_w * tgt_h) < (src_w * src_h) else "upscale"
                log.info(f"Resize: {src_w}x{src_h} → {tgt_w}x{tgt_h} ({direction})")

    # Subtitles + misc — copy by default, with per-stream overrides for mov_text→srt
    cmd += ["-c:s", "copy"]
    cmd += sub_codec_overrides
    cmd += ["-metadata", f"RECODE={VERSION}"]
    cmd += ["-max_muxing_queue_size", "9999"]

    # Progress output — remote jobs use -stats on stderr since pipe:1 doesn't tunnel
    # Progress output — remote jobs use -stats on stderr since pipe:1 doesn't tunnel
    if is_remote:
        cmd += ["-stats", "-loglevel", "warning"]
    else:
        cmd += ["-progress", "pipe:1", "-nostats", "-loglevel", "error"]

    cmd.append(tmp_output)

    resolved_values = {"resolved_cq": cq, "resolved_maxbitrate": maxbitrate, "resolved_speed": speed, "video_codec": video_codec}
    return cmd, tmp_output, output_file, resolved_values


# =============================================================================
# WebSocket Connection Manager
# =============================================================================

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# =============================================================================
# Encode Queue & Worker
# =============================================================================

QUEUE_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queue_state.json")


class EncodeQueue:
    def __init__(self):
        self.jobs: dict[str, EncodeJob] = {}
        self.queue_order: list[str] = []
        self.active_jobs: dict[str, EncodeJob] = {}  # job_id -> EncodeJob (currently encoding)
        self.ffmpeg_procs: dict[str, asyncio.subprocess.Process] = {}  # job_id -> process
        self.history: list[dict] = []
        self.running = False
        self.queue_enabled = False  # Start paused — user must start manually or enable auto_start_queue
        self.worker_tasks: list[asyncio.Task] = []
        self.ffmpeg_logs: dict[str, list[str]] = {}  # job_id -> stderr lines
        self._claiming = False  # simple flag to prevent race
        self._proc_ended_at: dict[str, float] = {}  # track when ffmpeg procs exit for watchdog
        self.job_gpus: dict[str, int] = {}  # job_id -> gpu index
        self._last_save = 0  # throttle _save_state
        self._load_state()

    def get_gpu_loads(self) -> dict:
        """Return active encode count per LOCAL GPU (excludes remote jobs)."""
        gpu_loads = {i: 0 for i in range(GPU_COUNT)}
        for jid, j in self.active_jobs.items():
            if not j.paused:
                gpu_id = self.job_gpus.get(jid, 0)
                if gpu_id >= 0:  # skip remote jobs (gpu_id=-1)
                    gpu_loads[gpu_id] = gpu_loads.get(gpu_id, 0) + 1
        return gpu_loads

    @staticmethod
    def gpu_max_encodes(gpu_idx: int, is_4k: bool = False) -> int:
        """Max concurrent encodes for a GPU.
        Uses user-set per-GPU limit if configured, otherwise auto-calculates from VRAM.
        4K jobs excluded from GPUs with <=2GB VRAM."""
        info = per_gpu_info.get(gpu_idx, {})
        mem_total_mb = info.get("mem_total", 0)
        if is_4k and mem_total_mb > 0 and mem_total_mb <= 2048:
            return 0  # 2GB GPUs OOM on 4K HEVC decode+encode
        # Check user-set per-GPU limit
        gpu_max_jobs = app_settings.get("gpu_max_jobs", {})
        user_limit = gpu_max_jobs.get(str(gpu_idx))
        if user_limit is not None:
            return int(user_limit)
        # Auto-calculate from VRAM: ~1.5 GB per encode, 1 per 2GB, minimum 1
        if mem_total_mb <= 0:
            return 1
        return max(1, mem_total_mb // 2048)

    def get_least_loaded_gpu(self, is_4k: bool = False, is_hdr: bool = False, is_10bit: bool = False):
        """Return the GPU index with the fewest active encodes, or None if all at max.
        Skips GPUs that cannot handle the job's resolution/HDR/10bit and disabled GPUs."""
        gpu_loads = self.get_gpu_loads()
        disabled = set(app_settings.get("disabled_gpus", []))
        available = {}
        for g, load in gpu_loads.items():
            if g < 0:  # skip remote pseudo-GPU
                continue
            if g in disabled:
                continue
            if not gpu_can_handle(g, is_4k, is_hdr, is_10bit):
                continue
            max_enc = self.gpu_max_encodes(g, is_4k=is_4k)
            if max_enc > 0 and load < max_enc:
                available[g] = load
        if not available:
            return None
        return min(available, key=available.get)

    def _load_state(self):
        """Load persisted queue and history from disk."""
        try:
            with open(QUEUE_STATE_FILE, "r") as f:
                data = json.load(f)
            # Restore history; queue starts paused unless auto_start_queue is enabled
            self.history = data.get("history", [])[-200:]
            self.queue_enabled = app_settings.get("auto_start_queue", False)
            # Restore queued jobs
            for job_data in data.get("queued", []):
                job = EncodeJob(
                    id=job_data["id"],
                    file_info=job_data["file_info"],
                    settings=job_data["settings"],
                )
                self.jobs[job.id] = job
                self.queue_order.append(job.id)
            # Re-queue interrupted jobs (were actively encoding when service stopped)
            interrupted = data.get("interrupted", [])
            if isinstance(interrupted, dict):
                interrupted = [interrupted]
            for job_data in interrupted:
                s = job_data["settings"]
                # Clear remote assignment so auto-scheduling re-evaluates
                for k in ("_remote_server_idx", "_remote_gpu_name", "_remote_encoder_type"):
                    s.pop(k, None)
                if s.get("encoder") == "remote":
                    s["encoder"] = "gpu"
                if s.get("gpu_target") == "remote":
                    s["gpu_target"] = "auto"
                job = EncodeJob(
                    id=job_data["id"],
                    file_info=job_data["file_info"],
                    settings=s,
                )
                self.jobs[job.id] = job
                self.queue_order.insert(0, job.id)  # front of queue
            if interrupted:
                log.info(f"Re-queued {len(interrupted)} interrupted jobs")
            if self.queue_order:
                log.info(f"Restored {len(self.queue_order)} queued jobs and {len(self.history)} history entries")
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            pass

    def _save_state(self, force=False):
        """Persist queue and history to disk. Throttled to max once per 10s unless force=True."""
        now = time.time()
        if not force and (now - self._last_save) < 10:
            return
        self._last_save = now
        queued = []
        for jid in self.queue_order:
            if jid in self.jobs and self.jobs[jid].status == JobStatus.QUEUED:
                j = self.jobs[jid]
                queued.append({"id": j.id, "file_info": j.file_info, "settings": j.settings})
        interrupted = []
        for j in self.active_jobs.values():
            if j.status == JobStatus.ENCODING:
                interrupted.append({"id": j.id, "file_info": j.file_info, "settings": j.settings})
        state = {
            "queued": queued,
            "interrupted": interrupted,
            "history": self.history,
            "queue_enabled": self.queue_enabled,
        }
        try:
            with open(QUEUE_STATE_FILE, "w") as f:
                json.dump(state, f)
        except Exception as e:
            log.error(f"Failed to save queue state: {e}")

    def is_duplicate(self, file_info: dict, settings: dict = None) -> str:
        """Check if a file is already queued or actively encoding with the same settings.
        Returns empty string if not duplicate, or a reason string if duplicate."""
        path = file_info.get("path", "")
        def _settings_match(existing_settings: dict) -> bool:
            if not settings:
                return True  # No settings to compare — treat as duplicate
            # Compare using resolved values (what actually gets encoded) when available
            # The preset determines the resolved values, so compare preset + non-resolvable keys
            keys_to_compare = ["preset", "video_codec", "dv_mode", "resize", "audio_codec"]
            # Defaults for keys that may be missing
            defaults = {"video_codec": "hevc", "dv_mode": "skip", "resize": "original", "audio_codec": "libopus"}
            for k in keys_to_compare:
                new_val = str(settings.get(k, "") or defaults.get(k, ""))
                exist_val = str(existing_settings.get(k, "") or defaults.get(k, ""))
                if new_val != exist_val:
                    return False
            # For cq/maxbitrate/speed: if preset is the same, the resolved values will be the same
            # But if preset is "custom", compare the raw values
            if settings.get("preset") == "custom":
                for raw, resolved in [("cq", "resolved_cq"), ("maxbitrate", "resolved_maxbitrate"), ("speed", "resolved_speed")]:
                    new_val = str(settings.get(raw, ""))
                    exist_val = str(existing_settings.get(resolved, existing_settings.get(raw, "")))
                    if new_val != exist_val:
                        return False
            return True
        # Check queued jobs
        for jid in self.queue_order:
            if jid in self.jobs and self.jobs[jid].status == JobStatus.QUEUED:
                if self.jobs[jid].file_info.get("path") == path:
                    if _settings_match(self.jobs[jid].settings):
                        return "Already queued with identical settings"
        # Check active jobs
        for j in self.active_jobs.values():
            if j.file_info.get("path") == path:
                if _settings_match(j.settings):
                    return "Already encoding with identical settings"
        return ""

    def add(self, file_info: dict, settings: dict) -> EncodeJob:
        dup_reason = self.is_duplicate(file_info, settings)
        if dup_reason:
            return None
        job_id = str(uuid.uuid4())[:8]
        job = EncodeJob(id=job_id, file_info=file_info, settings=settings.copy())
        self.jobs[job_id] = job
        self.queue_order.append(job_id)
        self._save_state(force=True)
        return job

    def remove(self, job_id: str) -> bool:
        if job_id in self.jobs and self.jobs[job_id].status == JobStatus.QUEUED:
            del self.jobs[job_id]
            self.queue_order = [j for j in self.queue_order if j != job_id]
            self._save_state(force=True)
            return True
        return False

    def remove_all(self) -> int:
        """Remove all queued (non-active) jobs."""
        queued = [jid for jid, j in self.jobs.items() if j.status == JobStatus.QUEUED]
        for jid in queued:
            del self.jobs[jid]
        self.queue_order = [j for j in self.queue_order if j not in queued]
        self._save_state(force=True)
        return len(queued)

    def cleanup_stale_active(self):
        """Remove active jobs whose processes are dead (e.g. after a restart)."""
        stale = []
        for jid, job in list(self.active_jobs.items()):
            proc = self.ffmpeg_procs.get(jid)
            if proc is None or proc.returncode is not None:
                stale.append(jid)
        for jid in stale:
            job = self.active_jobs.pop(jid, None)
            if job:
                job.status = JobStatus.CANCELLED
                job.error = "Interrupted by service restart"
                job.finished_at = time.time()
                self.history.append({
                    "id": job.id, "file_info": job.file_info, "settings": job.settings,
                    "status": job.status, "error": job.error,
                    "started_at": job.started_at, "finished_at": job.finished_at,
                    "result": {}, "log": [],
                })
                _record_encode_stat("cancelled", {}, job.started_at, job.finished_at)
            self.ffmpeg_procs.pop(jid, None)
            self.ffmpeg_logs.pop(jid, None)
        if stale:
            self.running = len(self.active_jobs) > 0
            self._save_state()
            log.info(f"Cleaned up {len(stale)} stale active job(s) from previous run")

    def reorder(self, job_ids: list[str]):
        valid = [j for j in job_ids if j in self.jobs and self.jobs[j].status == JobStatus.QUEUED]
        remaining = [j for j in self.queue_order if j not in valid and j in self.jobs and self.jobs[j].status == JobStatus.QUEUED]
        self.queue_order = valid + remaining
        self._save_state()

    def get_state(self) -> dict:
        queued = []
        for jid in self.queue_order:
            if jid in self.jobs and self.jobs[jid].status == JobStatus.QUEUED:
                j = self.jobs[jid]
                queued.append({"id": j.id, "file_info": j.file_info, "settings": j.settings, "status": j.status})

        active = []
        for j in self.active_jobs.values():
            active.append({
                "id": j.id, "file_info": j.file_info, "settings": j.settings,
                "status": j.status, "progress": j.progress, "result": j.result,
                "error": j.error, "started_at": j.started_at, "paused": j.paused,
            })

        return {
            "queued": queued,
            "current": active[0] if len(active) == 1 else None,  # backward compat
            "active": active,
            "history": self.history,
            "running": self.running,
            "queue_enabled": self.queue_enabled,
            "queue_count": len(queued),
            "active_count": len(active),
            "gpu_scan_complete": _gpu_scan_complete,
        }

    async def cancel_job(self, job_id: str = None):
        """Cancel a specific active job, or all active jobs if job_id is None."""
        async def _kill_proc(jid):
            job = self.active_jobs.get(jid)
            if job:
                job.status = JobStatus.CANCELLED
                # Resume first if paused — frozen processes ignore signals
                if job.paused:
                    try:
                        proc = self.ffmpeg_procs.get(jid)
                        if proc and proc.returncode is None:
                            os.kill(proc.pid, signal.SIGCONT)
                        job.paused = False
                    except Exception:
                        pass
            proc = self.ffmpeg_procs.get(jid)
            if proc:
                try:
                    proc.kill()
                except Exception:
                    pass

        if job_id:
            await _kill_proc(job_id)
        else:
            for jid in list(self.active_jobs.keys()):
                await _kill_proc(jid)

        # Give workers a moment to notice, then force-clean any stuck cancelled jobs
        await asyncio.sleep(2)
        stale = [jid for jid, j in list(self.active_jobs.items()) if j.status == JobStatus.CANCELLED]
        for jid in stale:
            job = self.active_jobs.pop(jid, None)
            if job:
                job.finished_at = time.time()
                log_lines = self.ffmpeg_logs.get(jid, [])[-100:]
                self.history.append({
                    "id": job.id, "file_info": job.file_info, "settings": job.settings,
                    "status": job.status, "error": "Cancelled by user",
                    "started_at": job.started_at, "finished_at": job.finished_at,
                    "result": {}, "log": log_lines,
                })
                _record_encode_stat("cancelled", {}, job.started_at, job.finished_at)
            self.ffmpeg_procs.pop(jid, None)
            self.ffmpeg_logs.pop(jid, None)
            self.queue_order = [j for j in self.queue_order if j != jid]
            if jid in self.jobs:
                del self.jobs[jid]
        if stale:
            self.running = len(self.active_jobs) > 0
            self._save_state(force=True)
            log.info(f"Force-cleaned {len(stale)} cancelled job(s)")


    async def pause_job(self, job_id: str):
        """Pause an active encode by sending SIGSTOP to ffmpeg."""
        if job_id in self.active_jobs and job_id in self.ffmpeg_procs:
            job = self.active_jobs[job_id]
            proc = self.ffmpeg_procs[job_id]
            if not job.paused and proc.returncode is None:
                try:
                    os.kill(proc.pid, signal.SIGSTOP)
                    job.paused = True
                except Exception:
                    pass

    async def resume_job(self, job_id: str):
        """Resume a paused encode by sending SIGCONT to ffmpeg."""
        if job_id in self.active_jobs and job_id in self.ffmpeg_procs:
            job = self.active_jobs[job_id]
            proc = self.ffmpeg_procs[job_id]
            if job.paused and proc.returncode is None:
                try:
                    os.kill(proc.pid, signal.SIGCONT)
                    job.paused = False
                except Exception:
                    pass


encode_queue = EncodeQueue()

# Scan cancellation
scan_cancel_event = asyncio.Event()

# =============================================================================
# System Stats Collection (CPU/GPU)
# =============================================================================

def detect_gpu_count() -> int:
    """Detect number of NVIDIA GPUs available."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            count = len([l for l in result.stdout.strip().splitlines() if l.strip()])
            log.info(f"Detected {count} GPU(s)")
            return count
    except Exception:
        pass
    return 0


GPU_COUNT = detect_gpu_count()

def _seed_gpu_info():
    """Pre-populate per_gpu_info with VRAM totals so gpu_max_encodes works before stats_collector runs."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3:
                    gi = int(parts[0])
                    per_gpu_info[gi] = {"name": parts[1], "mem_used": 0, "mem_total": int(parts[2])}
    except Exception:
        pass

# Vulkan/libplacebo test — run once at startup
_has_libplacebo = False
_vulkan_is_software = False
_gpu_scan_complete = False

def _run_vulkan_test():
    """Test Vulkan/libplacebo availability. Called from background startup scan."""
    global _has_libplacebo, _vulkan_is_software
    try:
        _lp_test = subprocess.run(
            [FFMPEG, "-hide_banner", "-loglevel", "error", "-y",
             "-f", "lavfi", "-i", "color=black:s=1920x1080:d=2:r=25",
             "-frames:v", "50", "-init_hw_device", "vulkan",
             "-vf", "hwupload,libplacebo=format=yuv420p10le,hwdownload,format=yuv420p10le",
             "-c:v", "libx265", "-f", "null", "-"],
            capture_output=True, text=True, timeout=30
        )
        _has_libplacebo = _lp_test.returncode == 0
    except Exception:
        pass
    if _has_libplacebo:
        try:
            _vk_probe = subprocess.run(
                [FFMPEG, "-hide_banner", "-loglevel", "verbose",
                 "-init_hw_device", "vulkan=vk:0",
                 "-f", "lavfi", "-i", "nullsrc=s=64x64:d=0.01",
                 "-vf", "hwupload,libplacebo=format=yuv420p10le,hwdownload,format=yuv420p10le",
                 "-f", "null", "-"],
                capture_output=True, text=True, timeout=15
            )
            _vk_out = _vk_probe.stderr or ""
            import re as _re
            _vk_dev = _re.search(r'Using device:\s*(.+)', _vk_out)
            _vk_dev_name = _vk_dev.group(1).strip() if _vk_dev else ""
            if "llvmpipe" in _vk_dev_name or "software" in _vk_dev_name.lower():
                _vulkan_is_software = True
                log.warning(f"Vulkan/libplacebo: using SOFTWARE renderer ({_vk_dev_name}) — install mesa-dri-drivers mesa-libEGL mesa-libgbm mesa-libGL libXext for GPU Vulkan")
            elif _vk_dev_name:
                log.info(f"Vulkan/libplacebo: functional — {_vk_dev_name}")
            else:
                log.info("Vulkan/libplacebo: functional")
        except Exception:
            log.info("Vulkan/libplacebo: functional")
    else:
        log.info("Vulkan/libplacebo: not available — DV P5 will use HDR10 fallback")

def _get_vulkan_version():
    import re, glob
    # Check actual library file for version
    for pattern in ["/usr/lib64/libvulkan.so.*.*", "/usr/lib/x86_64-linux-gnu/libvulkan.so.*.*", "/usr/lib/libvulkan.so.*.*"]:
        for path in sorted(glob.glob(pattern)):
            m = re.search(r'libvulkan\.so\.(\d+\.\d+\.\d+)', path)
            if m:
                return m.group(1)
    # Fallback: check if library exists at all
    for path in ["/usr/lib64/libvulkan.so.1", "/usr/lib/x86_64-linux-gnu/libvulkan.so.1"]:
        if os.path.exists(path):
            real = os.path.realpath(path)
            m = re.search(r'libvulkan\.so\.(\d+\.\d+\.\d+)', real)
            if m:
                return m.group(1)
            return "1.0"
    return None

# GPU capability testing — run once at startup, cache results
_gpu_capabilities: dict[int, dict] = {}  # gpu_index -> {"1080p_sdr": True, "1080p_hdr": True, "4k_sdr": True, "4k_hdr": False}

def _test_gpu_capabilities():
    """Test each local GPU for 1080p/4K SDR/HDR encoding capability."""
    if GPU_COUNT <= 0:
        return
    disabled = set(app_settings.get("disabled_gpus", []))
    tests = [
        ("1080p_sdr",   "1920x1080", "yuv420p",    False),
        ("1080p_10bit", "1920x1080", "yuv420p10le", False),
        ("1080p_hdr",   "1920x1080", "yuv420p10le", True),
        ("4k_sdr",      "3840x2160", "yuv420p",     False),
        ("4k_10bit",    "3840x2160", "yuv420p10le", False),
        ("4k_hdr",      "3840x2160", "yuv420p10le", True),
    ]
    tmp_dir = app_settings.get("tmp_dir", "/tmp/recode")
    os.makedirs(tmp_dir, exist_ok=True)
    for gpu in range(GPU_COUNT):
        if gpu in disabled:
            continue
        caps = {}
        for name, size, pix_fmt, hdr in tests:
            # Step 1: Generate a test HEVC file
            test_file = os.path.join(tmp_dir, f"_captest_{gpu}_{name}.mkv")
            gen_cmd = [
                FFMPEG, "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", f"color=black:s={size}:d=2:r=25",
                "-frames:v", "50", "-pix_fmt", pix_fmt,
                "-c:v", "hevc_nvenc", "-gpu", str(gpu),
            ]
            if hdr:
                gen_cmd += ["-color_primaries", "bt2020", "-color_trc", "smpte2084", "-colorspace", "bt2020nc"]
            gen_cmd.append(test_file)
            # Step 2: Decode with CUDA hwaccel + re-encode (the real pipeline)
            enc_cmd = [
                FFMPEG, "-hide_banner", "-loglevel", "error", "-y",
                "-hwaccel", "cuda", "-hwaccel_device", str(gpu), "-hwaccel_output_format", "cuda",
                "-extra_hw_frames", "16",
                "-i", test_file,
                "-c:v", "hevc_nvenc", "-gpu", str(gpu),
                "-rc", "vbr", "-cq", "28", "-preset", "p4",
                "-multipass", "qres", "-spatial-aq", "1", "-aq-strength", "8",
            ]
            if hdr:
                enc_cmd += ["-color_primaries", "bt2020", "-color_trc", "smpte2084", "-colorspace", "bt2020nc"]
            enc_cmd += ["-f", "null", "-"]
            try:
                # Generate test file
                r1 = subprocess.run(gen_cmd, capture_output=True, timeout=30)
                if r1.returncode != 0:
                    caps[name] = False
                    log.info(f"GPU {gpu} capability {name}: FAIL (generate)")
                    continue
                # Test full decode+encode pipeline
                r2 = subprocess.run(enc_cmd, capture_output=True, timeout=60)
                caps[name] = r2.returncode == 0
            except Exception:
                caps[name] = False
            finally:
                try: os.remove(test_file)
                except Exception: pass
            log.info(f"GPU {gpu} capability {name}: {'OK' if caps[name] else 'FAIL'}")
        _gpu_capabilities[gpu] = caps
    log.info(f"Local GPU capabilities: {_gpu_capabilities}")

def _run_startup_gpu_scan():
    """Run all GPU tests (Vulkan + capabilities) in background. Called from startup event."""
    global _gpu_scan_complete
    log.info("Background GPU scan starting...")
    _run_vulkan_test()
    _test_gpu_capabilities()
    _gpu_scan_complete = True
    log.info("Background GPU scan complete")

# Not called at module load — deferred to startup event for fast server start

def gpu_can_handle(gpu_idx: int, is_4k: bool, is_hdr: bool, is_10bit: bool = False) -> bool:
    """Check if a GPU can handle this job based on capability test results and VRAM."""
    # VRAM-based guard: GPUs with < 3GB can't reliably handle 4K (test passes but real content OOMs)
    if is_4k:
        vram = per_gpu_info.get(gpu_idx, {}).get("mem_total", 0)
        if 0 < vram < 3072:
            return False
    caps = _gpu_capabilities.get(gpu_idx)
    if not caps:
        return True  # no test data — assume capable
    if is_4k and is_hdr:
        return caps.get("4k_hdr", False)
    elif is_4k and is_10bit:
        return caps.get("4k_10bit", False)
    elif is_4k:
        return caps.get("4k_sdr", True)
    elif is_hdr:
        return caps.get("1080p_hdr", True)
    elif is_10bit:
        return caps.get("1080p_10bit", False)
    return caps.get("1080p_sdr", True)

def remote_gpu_can_handle(gpu_info: dict, is_4k: bool, is_hdr: bool, is_10bit: bool = False) -> bool:
    """Check if a remote GPU can handle this job based on reported capabilities."""
    caps_list = gpu_info.get("gpu_capabilities", [])
    if not caps_list:
        return True  # no capability data — assume capable
    # Aggregate: if ANY GPU on the server can handle it, it's ok
    for caps in caps_list:
        if is_4k and is_hdr:
            if caps.get("4k_hdr", False): return True
        elif is_4k and is_10bit:
            if caps.get("4k_10bit", False): return True
        elif is_4k:
            if caps.get("4k_sdr", True): return True
        elif is_hdr:
            if caps.get("1080p_hdr", True): return True
        elif is_10bit:
            if caps.get("1080p_10bit", False): return True
        else:
            if caps.get("1080p_sdr", True): return True
    return False

MAX_STATS_POINTS = 120  # ~4 minutes at 2s intervals
stats_history = {
    "cpu": deque(maxlen=MAX_STATS_POINTS),
    "gpu": deque(maxlen=MAX_STATS_POINTS),
    "gpu_mem": deque(maxlen=MAX_STATS_POINTS),
    "gpu_temp": deque(maxlen=MAX_STATS_POINTS),
}
# Per-GPU stats history: gpu_N_util, gpu_N_temp for each GPU
per_gpu_stats: dict[int, dict[str, deque]] = {}
per_gpu_info: dict[int, dict] = {}  # name, mem_used, mem_total per GPU (updated each poll)
_seed_gpu_info()

for _gi in range(max(GPU_COUNT, 1)):
    per_gpu_stats[_gi] = {
        "util": deque(maxlen=MAX_STATS_POINTS),
        "temp": deque(maxlen=MAX_STATS_POINTS),
        "mem_pct": deque(maxlen=MAX_STATS_POINTS),
    }

def _init_stats_table():
    """Create stats_history and encode_stats tables if they don't exist."""
    conn = get_cache_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS stats_history (
            ts REAL PRIMARY KEY, cpu REAL, gpu REAL, gpu_mem REAL, gpu_temp REAL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS encode_stats (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            done INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            skipped INTEGER DEFAULT 0,
            total_orig_bytes INTEGER DEFAULT 0,
            total_new_bytes INTEGER DEFAULT 0,
            total_saved_bytes INTEGER DEFAULT 0,
            total_encode_time REAL DEFAULT 0,
            savings_pct_sum REAL DEFAULT 0,
            savings_pct_count INTEGER DEFAULT 0
        )""")
        conn.execute("INSERT OR IGNORE INTO encode_stats (id) VALUES (1)")
        # Per-day encode history for charts
        conn.execute("""CREATE TABLE IF NOT EXISTS encode_daily (
            date TEXT PRIMARY KEY,
            done INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            orig_bytes INTEGER DEFAULT 0,
            new_bytes INTEGER DEFAULT 0,
            saved_bytes INTEGER DEFAULT 0,
            encode_time REAL DEFAULT 0
        )""")
        conn.commit()
    finally:
        conn.close()

def _load_stats():
    """Load saved stats from DB on startup."""
    _init_stats_table()
    conn = get_cache_db()
    try:
        rows = conn.execute(
            "SELECT ts, cpu, gpu, gpu_mem, gpu_temp FROM stats_history ORDER BY ts DESC LIMIT ?",
            (MAX_STATS_POINTS,)
        ).fetchall()
        for row in reversed(rows):
            stats_history["cpu"].append({"t": row["ts"], "v": row["cpu"]})
            stats_history["gpu"].append({"t": row["ts"], "v": row["gpu"]})
            stats_history["gpu_mem"].append({"t": row["ts"], "v": row["gpu_mem"]})
            stats_history["gpu_temp"].append({"t": row["ts"], "v": row["gpu_temp"]})
        log.info(f"Loaded {len(rows)} stats points from database")
    except Exception as e:
        log.warning(f"Failed to load stats history: {e}")
    finally:
        conn.close()

def _save_stats_batch(points: list):
    """Save a batch of stats points to DB."""
    conn = get_cache_db()
    try:
        conn.executemany(
            "INSERT OR REPLACE INTO stats_history (ts, cpu, gpu, gpu_mem, gpu_temp) VALUES (?, ?, ?, ?, ?)",
            points
        )
        # Prune old entries beyond MAX_STATS_POINTS
        conn.execute(
            "DELETE FROM stats_history WHERE ts NOT IN (SELECT ts FROM stats_history ORDER BY ts DESC LIMIT ?)",
            (MAX_STATS_POINTS,)
        )
        conn.commit()
    except Exception as e:
        log.warning(f"Failed to save stats: {e}")
    finally:
        conn.close()

def _record_encode_stat(status: str, result: dict, started_at: float, finished_at: float):
    """Record a completed encode job into persistent stats."""
    conn = get_cache_db()
    try:
        if status == "done":
            ob = result.get("orig_bytes", 0)
            nb = result.get("new_bytes", 0)
            saved = max(ob - nb, 0) if ob > 0 and nb > 0 else 0
            pct = result.get("saved_pct", 0)
            elapsed = (finished_at - started_at) if started_at and finished_at else 0
            conn.execute("""UPDATE encode_stats SET
                done = done + 1,
                total_orig_bytes = total_orig_bytes + ?,
                total_new_bytes = total_new_bytes + ?,
                total_saved_bytes = total_saved_bytes + ?,
                total_encode_time = total_encode_time + ?,
                savings_pct_sum = savings_pct_sum + ?,
                savings_pct_count = savings_pct_count + ?
                WHERE id = 1""",
                (ob, nb, saved, elapsed, pct if ob > 0 and nb > 0 else 0, 1 if ob > 0 and nb > 0 else 0))
        elif status == "failed":
            conn.execute("UPDATE encode_stats SET failed = failed + 1 WHERE id = 1")
        elif status in ("skipped", "cancelled"):
            conn.execute("UPDATE encode_stats SET skipped = skipped + 1 WHERE id = 1")
        # Record daily history
        import datetime
        today = datetime.date.today().isoformat()
        conn.execute("INSERT OR IGNORE INTO encode_daily (date) VALUES (?)", (today,))
        if status == "done":
            conn.execute("""UPDATE encode_daily SET done = done + 1,
                orig_bytes = orig_bytes + ?, new_bytes = new_bytes + ?,
                saved_bytes = saved_bytes + ?, encode_time = encode_time + ?
                WHERE date = ?""", (ob, nb, saved, elapsed, today))
        elif status == "failed":
            conn.execute("UPDATE encode_daily SET failed = failed + 1 WHERE date = ?", (today,))
        conn.commit()
    except Exception as e:
        log.warning(f"Failed to record encode stat: {e}")
    finally:
        conn.close()

def _get_encode_stats() -> dict:
    """Load persistent encode stats from DB."""
    conn = get_cache_db()
    try:
        row = conn.execute("SELECT * FROM encode_stats WHERE id = 1").fetchone()
        if not row:
            return {}
        return dict(row)
    except Exception as e:
        log.warning(f"Failed to load encode stats: {e}")
        return {}
    finally:
        conn.close()

_load_stats()

def _safe_int(v):
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


def get_gpu_stats() -> dict:
    """Get GPU utilization, memory, and temperature via nvidia-smi.
    Returns aggregated stats across all GPUs (max util, sum memory, max temp)
    plus per-GPU breakdown."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            gpus = []
            for line in result.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 7:
                    gpus.append({
                        "index": _safe_int(parts[0]),
                        "name": parts[1],
                        "gpu_util": _safe_int(parts[2]),
                        "gpu_mem_util": _safe_int(parts[3]),
                        "gpu_mem_used": _safe_int(parts[4]),
                        "gpu_mem_total": _safe_int(parts[5]),
                        "gpu_temp": _safe_int(parts[6]),
                    })
            if gpus:
                # Aggregate: max utilization, sum memory, max temp
                return {
                    "gpu_util": max(g["gpu_util"] for g in gpus),
                    "gpu_mem_util": max(g["gpu_mem_util"] for g in gpus),
                    "gpu_mem_used": sum(g["gpu_mem_used"] for g in gpus),
                    "gpu_mem_total": sum(g["gpu_mem_total"] for g in gpus),
                    "gpu_temp": max(g["gpu_temp"] for g in gpus),
                    "gpus": gpus,
                }
    except Exception:
        pass
    return {"gpu_util": 0, "gpu_mem_util": 0, "gpu_mem_used": 0, "gpu_mem_total": 0, "gpu_temp": 0, "gpus": []}

async def stats_collector():
    """Background task to collect CPU/GPU stats every 2 seconds, flush to DB every 30s."""
    pending_points = []
    while True:
        cpu = psutil.cpu_percent(interval=None)
        gpu = get_gpu_stats()
        ts = time.time()
        stats_history["cpu"].append({"t": ts, "v": cpu})
        stats_history["gpu"].append({"t": ts, "v": gpu["gpu_util"]})
        stats_history["gpu_mem"].append({"t": ts, "v": gpu["gpu_mem_util"]})
        stats_history["gpu_temp"].append({"t": ts, "v": gpu["gpu_temp"]})
        # Per-GPU stats
        for g in gpu.get("gpus", []):
            gi = g["index"]
            if gi not in per_gpu_stats:
                per_gpu_stats[gi] = {"util": deque(maxlen=MAX_STATS_POINTS), "temp": deque(maxlen=MAX_STATS_POINTS), "mem_pct": deque(maxlen=MAX_STATS_POINTS)}
            per_gpu_stats[gi]["util"].append({"t": ts, "v": g["gpu_util"]})
            per_gpu_stats[gi]["temp"].append({"t": ts, "v": g["gpu_temp"]})
            mem_total = g["gpu_mem_total"] or 1
            mem_pct = round(g["gpu_mem_used"] / mem_total * 100, 1)
            if "mem_pct" not in per_gpu_stats[gi]:
                per_gpu_stats[gi]["mem_pct"] = deque(maxlen=MAX_STATS_POINTS)
            per_gpu_stats[gi]["mem_pct"].append({"t": ts, "v": mem_pct})
            per_gpu_info[gi] = {"name": g.get("name", ""), "mem_used": g["gpu_mem_used"], "mem_total": g["gpu_mem_total"]}
        pending_points.append((ts, cpu, gpu["gpu_util"], gpu["gpu_mem_util"], gpu["gpu_temp"]))
        if len(pending_points) >= 15:  # ~30 seconds worth
            await asyncio.get_event_loop().run_in_executor(None, _save_stats_batch, pending_points)
            pending_points = []
        await asyncio.sleep(2 if encode_queue.active_jobs else 5)


def is_within_schedule() -> bool:
    """Check if current time is within the scheduled encoding window."""
    if not app_settings.get("schedule_enabled", False):
        return True  # No schedule = always allowed
    start_str = app_settings.get("schedule_start", "00:00")
    end_str = app_settings.get("schedule_end", "08:00")
    try:
        now = time.localtime()
        now_mins = now.tm_hour * 60 + now.tm_min
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
        start_mins = sh * 60 + sm
        end_mins = eh * 60 + em
        if start_mins <= end_mins:
            return start_mins <= now_mins < end_mins
        else:
            # Wraps midnight (e.g. 22:00 - 06:00)
            return now_mins >= start_mins or now_mins < end_mins
    except Exception:
        return True


async def _encode_worker_safe(worker_id: int):
    """Wrapper that restarts encode_worker on unhandled exceptions."""
    while True:
        try:
            await encode_worker(worker_id)
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception(f"[worker-{worker_id}] Crashed — restarting in 5s")
            # Clean up claiming lock if this worker held it
            encode_queue._claiming = False
            await asyncio.sleep(5)


async def encode_worker(worker_id: int):
    """Background worker that processes the encode queue. Multiple workers run concurrently."""
    await asyncio.sleep(2)  # let startup complete before claiming jobs
    while True:
        # Check if queue processing is enabled
        if not encode_queue.queue_enabled:
            await asyncio.sleep(1)
            continue

        # Check schedule
        if not is_within_schedule():
            await asyncio.sleep(10)
            continue

        # Claiming — use a simple flag but stagger workers to avoid race
        if encode_queue._claiming:
            await asyncio.sleep(0.1 + worker_id * 0.05)
            continue
        encode_queue._claiming = True
        # Find the next job that can run on an available GPU.
        # Smaller jobs can jump the queue if a capable GPU is idle.
        next_job = None
        available_gpus = set()
        disabled_gpus = set(app_settings.get("disabled_gpus", []))
        if GPU_COUNT > 0:
            gpu_loads = encode_queue.get_gpu_loads()
            # Build available_gpus purely from per-GPU VRAM capacity
            # (don't gate on max_concurrent — that's enforced at assignment time)
            for g in range(GPU_COUNT):
                if g in disabled_gpus:
                    continue
                if gpu_loads.get(g, 0) < encode_queue.gpu_max_encodes(g):
                    available_gpus.add(g)

        for jid in list(encode_queue.queue_order):
            if jid not in encode_queue.jobs or encode_queue.jobs[jid].status != JobStatus.QUEUED:
                continue
            candidate = encode_queue.jobs[jid]
            job_is_4k = candidate.file_info.get("width", 0) >= 3800
            job_encoder = candidate.settings.get("encoder", "gpu")
            job_target = candidate.settings.get("gpu_target", "auto")

            # Normalize: encoder=remote → gpu with auto target
            if job_encoder == "remote":
                job_encoder = "gpu"
                candidate.settings["encoder"] = "gpu"
            if job_target == "remote" and not candidate.settings.get("_remote_gpu_name"):
                job_target = "auto"
                candidate.settings["gpu_target"] = "auto"

            # Pre-flight skip checks — don't wait for GPU to skip these
            _dv_mode = candidate.settings.get("dv_mode", "skip")
            _hdr_type = candidate.file_info.get("hdr_type", "")
            if _hdr_type.startswith("Dolby Vision") and _dv_mode == "skip":
                next_job = candidate
                break

            # CPU jobs can always run
            if job_encoder == "cpu" or candidate.settings.get("use_cpu", False):
                next_job = candidate
                break

            # Specific remote target — check if remote GPUs are available
            if job_target and job_target.startswith("remote:"):
                next_job = candidate
                break

            # Local GPU jobs — check if any available GPU can handle this job
            job_is_hdr = candidate.file_info.get("is_hdr", False) or candidate.file_info.get("hdr_type", "SDR") != "SDR"
            job_is_10bit = "10" in (candidate.file_info.get("pix_fmt", "") or "")
            if available_gpus:
                # Check if at least one available GPU can handle this resolution + HDR
                can_run = False
                for g in available_gpus:
                    if encode_queue.gpu_max_encodes(g, is_4k=job_is_4k) > 0 and gpu_can_handle(g, job_is_4k, job_is_hdr, job_is_10bit):
                        can_run = True
                        break
                if can_run:
                    next_job = candidate
                    break
                # No local GPU can handle this — fall through to remote check

            # Auto mode — try remote GPUs
            _skip_fname = candidate.file_info.get("filename", "?")[:35]
            _skip_res = "4K" if job_is_4k else "1080p"
            _skip_fmt = "HDR" if job_is_hdr else "10bit" if job_is_10bit else "SDR"
            if job_target == "auto":
                # Check if remote GPUs are available
                try:
                    _lsf = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-status.json")
                    with open(_lsf) as _f:
                        _all_remote = json.load(_f).get("gpus", [])
                    # Check if any capable remote GPU has an available slot
                    _remote_loads = {}
                    for _aj in encode_queue.active_jobs.values():
                        _rn = _aj.settings.get("_remote_gpu_name", "")
                        if _rn and not _aj.paused:
                            _remote_loads[_rn] = _remote_loads.get(_rn, 0) + 1
                    _auto_gpus = [g for g in _all_remote
                        if g.get("online")
                        and remote_gpu_can_handle(g, job_is_4k, job_is_hdr, job_is_10bit)
                        and _remote_loads.get(g.get("name", ""), 0) < g.get("max_jobs", 1)]
                    if _auto_gpus:
                        next_job = candidate
                        break
                except Exception:
                    pass
                # No capable remote — check if any local GPU can handle it
                if available_gpus and any(gpu_can_handle(g, job_is_4k, job_is_hdr, job_is_10bit) for g in available_gpus):
                    next_job = candidate
                    break
                # No GPU anywhere can handle this job right now — skip to next
                if not hasattr(candidate, '_skip_logged'):
                    log.info(f"[queue] Skip: {_skip_fname} ({_skip_res} {_skip_fmt}) — no capable GPU with available slot")
                    candidate._skip_logged = True
                continue

            # No available GPUs — skip, let it wait
            continue

        if not next_job:
            encode_queue.running = len(encode_queue.active_jobs) > 0
            encode_queue._claiming = False
            await asyncio.sleep(1)
            continue
        else:
            # Assign GPU target based on encoder + gpu_target settings
            # gpu_target values: "auto", "local", "remote", "gpu:N", "remote:N"
            encoder = next_job.settings.get("encoder", "gpu")
            gpu_target = next_job.settings.get("gpu_target", "auto")
            # Normalize: encoder="remote" → gpu with auto target (let scheduler decide)
            if encoder == "remote":
                encoder = "gpu"
                if gpu_target == "auto" or not gpu_target.startswith("remote"):
                    gpu_target = "auto"
            is_cpu_job = encoder == "cpu" or next_job.settings.get("use_cpu", False)
            _job_is_4k = next_job.file_info.get("width", 0) >= 3800
            _job_is_hdr = next_job.file_info.get("is_hdr", False) or next_job.file_info.get("hdr_type", "SDR") != "SDR"
            _job_is_10bit = "10" in (next_job.file_info.get("pix_fmt", "") or "")

            gpu_id = -1  # -1 = no GPU (CPU encode)
            remote_server_idx = -1  # -1 = not remote

            if is_cpu_job:
                pass  # gpu_id stays -1
            elif gpu_target.startswith("remote:"):
                # Specific remote GPU by name — check if it has capacity
                remote_gpu_name = gpu_target.split(":", 1)[1]
                # Count active jobs on this GPU
                active_on_gpu = sum(1 for j in encode_queue.active_jobs.values()
                    if j.settings.get("_remote_gpu_name") == remote_gpu_name)
                # Get max_jobs and capabilities for this GPU from listener status
                gpu_max = 1
                _remote_info = {}
                try:
                    _lsf = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-status.json")
                    with open(_lsf) as _f:
                        for g in json.load(_f).get("gpus", []):
                            if g.get("name") == remote_gpu_name:
                                gpu_max = g.get("max_jobs", 1)
                                _remote_info = g
                                break
                except Exception:
                    pass
                # Check capability
                if _remote_info and not remote_gpu_can_handle(_remote_info, _job_is_4k, _job_is_hdr, _job_is_10bit):
                    _cap_desc = f"{'4K' if _job_is_4k else '1080p'} {'HDR' if _job_is_hdr else 'SDR'}"
                    _skip_msg = f"{remote_gpu_name} cannot handle {_cap_desc} (failed capability test)"
                    log.warning(f"[{next_job.id}] {_skip_msg}")
                    encode_queue.ffmpeg_logs.setdefault(next_job.id, []).append(_skip_msg)
                    _finish_job(JobStatus.FAILED, _skip_msg, no_retry=True)
                    encode_queue._claiming = False
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    continue
                if active_on_gpu >= gpu_max:
                    encode_queue._claiming = False
                    await asyncio.sleep(2)
                    continue
                next_job.settings["_remote_gpu_name"] = remote_gpu_name
                remote_server_idx = 0
                next_job.settings["encoder"] = "remote"
            elif gpu_target == "remote":
                # Auto-balance across connected GPUs (from listener) or configured remote servers
                # Check listener for connected GPUs first
                listener_status_file = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-status.json")
                _connected_gpus = []
                try:
                    with open(listener_status_file) as _f:
                        _ls = json.load(_f)
                    _connected_gpus = [g for g in _ls.get("gpus", []) if g.get("online")]
                except Exception:
                    pass
                # Use connected GPUs from listener
                if _connected_gpus:
                    # Filter by capability — only send to servers that can handle this job
                    enabled_idxs = [i for i in range(len(_connected_gpus)) if remote_gpu_can_handle(_connected_gpus[i], _job_is_4k, _job_is_hdr, _job_is_10bit)]
                    # Build a virtual servers list from connected GPUs for load balancing
                    servers = [{"max_jobs": g.get("max_jobs", 1), "_online": True, "enabled": True, "name": g.get("name", f"GPU {i}")} for i, g in enumerate(_connected_gpus)]
                else:
                    # Fallback: check remote_gpu_servers config
                    servers = app_settings.get("remote_gpu_servers", [])
                    enabled_idxs = [i for i, s in enumerate(servers)
                                    if s.get("enabled", True) is not False and s.get("_online", False)]
                if enabled_idxs:
                    # Count active jobs by GPU name
                    remote_loads = {}
                    for jid, j in encode_queue.active_jobs.items():
                        gname = j.settings.get("_remote_gpu_name", "")
                        if gname:
                            remote_loads[gname] = remote_loads.get(gname, 0) + 1
                    available_remotes = [i for i in enabled_idxs
                                        if remote_loads.get(servers[i]["name"], 0) < servers[i].get("max_jobs", 1)]
                    if available_remotes:
                        remote_server_idx = min(available_remotes, key=lambda i: remote_loads.get(servers[i]["name"], 0))
                        next_job.settings["_remote_gpu_name"] = servers[remote_server_idx]["name"]
                        next_job.settings["encoder"] = "remote"
                    else:
                        encode_queue._claiming = False
                        await asyncio.sleep(2)
                        continue
                else:
                    # No remote servers configured — fall back to local GPU or CPU
                    log.warning(f"[{next_job.id}] No remote GPU servers configured, falling back to local")
                    if GPU_COUNT > 0:
                        gpu_id = encode_queue.get_least_loaded_gpu(is_4k=_job_is_4k, is_hdr=_job_is_hdr, is_10bit=_job_is_10bit)
                        if gpu_id is None:
                            encode_queue._claiming = False
                            await asyncio.sleep(2)
                            continue
                    # else stays cpu (gpu_id = -1)
            elif gpu_target.startswith("gpu:"):
                # Specific local GPU
                try:
                    wanted = int(gpu_target.split(":")[1])
                    gpu_loads = encode_queue.get_gpu_loads()
                    if not gpu_can_handle(wanted, _job_is_4k, _job_is_hdr, _job_is_10bit):
                        _cap_desc = f"{'4K' if _job_is_4k else '1080p'} {'HDR' if _job_is_hdr else 'SDR'}"
                        _skip_msg = f"GPU {wanted} cannot handle {_cap_desc} (failed capability test)"
                        log.warning(f"[{next_job.id}] {_skip_msg}")
                        encode_queue.ffmpeg_logs.setdefault(next_job.id, []).append(_skip_msg)
                        _finish_job(JobStatus.FAILED, _skip_msg, no_retry=True)
                        encode_queue._claiming = False
                        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                        continue
                    if encode_queue.gpu_max_encodes(wanted, is_4k=_job_is_4k) > 0 and gpu_loads.get(wanted, 0) < encode_queue.gpu_max_encodes(wanted, is_4k=_job_is_4k):
                        gpu_id = wanted
                    else:
                        gpu_id = None
                except (ValueError, IndexError):
                    gpu_id = None
                if gpu_id is None:
                    encode_queue._claiming = False
                    await asyncio.sleep(2)
                    continue
            elif gpu_target == "local" or (encoder == "gpu" and gpu_target != "auto"):
                # Balance across local GPUs only
                if GPU_COUNT > 0:
                    # Handle legacy gpu_id values ("auto", "0", "1")
                    legacy_gpu = next_job.settings.get("gpu_id", "auto")
                    gpu_id = None
                    if legacy_gpu not in (None, "auto", ""):
                        try:
                            wanted = int(legacy_gpu)
                            gpu_loads = encode_queue.get_gpu_loads()
                            if encode_queue.gpu_max_encodes(wanted, is_4k=_job_is_4k) > 0 and gpu_loads.get(wanted, 0) < encode_queue.gpu_max_encodes(wanted, is_4k=_job_is_4k):
                                gpu_id = wanted
                        except (ValueError, TypeError):
                            pass
                    if gpu_id is None:
                        gpu_id = encode_queue.get_least_loaded_gpu(is_4k=_job_is_4k, is_hdr=_job_is_hdr, is_10bit=_job_is_10bit)
                    if gpu_id is None:
                        encode_queue._claiming = False
                        await asyncio.sleep(2)
                        continue
                # else stays cpu (gpu_id = -1)
            else:
                # "auto" — per-GPU limits control local capacity, plus remote servers independently
                if GPU_COUNT > 0:
                    candidate_gpu = encode_queue.get_least_loaded_gpu(is_4k=_job_is_4k, is_hdr=_job_is_hdr, is_10bit=_job_is_10bit)
                    if candidate_gpu is not None:
                        gpu_id = candidate_gpu
                    else:
                        gpu_id = -1  # all local GPUs at per-GPU capacity, try remote
                else:
                    gpu_id = -1  # no local GPUs, try remote

                # If no local slot, try remote (check listener connected GPUs first)
                if gpu_id == -1 and not is_cpu_job:
                    _auto_connected = []
                    try:
                        _auto_lsf = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-status.json")
                        with open(_auto_lsf) as _f:
                            _auto_connected = [g for g in json.load(_f).get("gpus", []) if g.get("online")]
                    except Exception:
                        pass
                    if _auto_connected:
                        servers = [{"max_jobs": g.get("max_jobs", 1), "name": g.get("name", f"GPU {i}"), "gpu_capabilities": g.get("gpu_capabilities", [])} for i, g in enumerate(_auto_connected)]
                        # Filter by capability — only send to servers that can handle this job
                        enabled_idxs = [i for i in range(len(servers)) if remote_gpu_can_handle(_auto_connected[i], _job_is_4k, _job_is_hdr, _job_is_10bit)]
                    else:
                        servers = app_settings.get("remote_gpu_servers", [])
                        enabled_idxs = [i for i, s in enumerate(servers)
                                        if s.get("enabled", True) is not False and s.get("_online", False)]
                    # Count remote loads by name
                    name_loads = {}
                    for j in encode_queue.active_jobs.values():
                        gname = j.settings.get("_remote_gpu_name", "")
                        if gname and not j.paused:
                            name_loads[gname] = name_loads.get(gname, 0) + 1
                    available = [i for i in enabled_idxs
                                 if name_loads.get(servers[i]["name"], 0) < servers[i].get("max_jobs", 1)]
                    if available:
                        import random
                        min_load = min(name_loads.get(servers[i]["name"], 0) for i in available)
                        tied = [i for i in available if name_loads.get(servers[i]["name"], 0) == min_load]
                        remote_server_idx = random.choice(tied)
                        next_job.settings["_remote_gpu_name"] = servers[remote_server_idx]["name"]
                        next_job.settings["encoder"] = "remote"
                    else:
                        # Nothing available — wait
                        encode_queue._claiming = False
                        await asyncio.sleep(2)
                        continue

            next_job.settings["_remote_server_idx"] = remote_server_idx
            if remote_server_idx < 0:
                next_job.settings.pop("_remote_gpu_name", None)
                next_job.settings.pop("_remote_encoder_type", None)
                if next_job.settings.get("encoder") == "remote":
                    next_job.settings["encoder"] = "gpu"
            # Store the detected encoder type for this remote server
            if remote_server_idx >= 0:
                srv_list = app_settings.get("remote_gpu_servers", [])
                if remote_server_idx < len(srv_list):
                    next_job.settings["_remote_encoder_type"] = srv_list[remote_server_idx].get("_encoder_type", "nvenc")
            encode_queue.running = True
            next_job.status = JobStatus.ENCODING
            next_job.started_at = time.time()
            encode_queue.active_jobs[next_job.id] = next_job
            encode_queue.job_gpus[next_job.id] = gpu_id
            _rname = next_job.settings.get("_remote_gpu_name", "")
            _fname = next_job.file_info.get("filename", "?")[:40]
            _res = "4K" if _job_is_4k else "1080p"
            _hdr_label = "HDR" if _job_is_hdr else "10bit" if _job_is_10bit else "SDR"
            _dest = f"remote:{_rname}" if _rname else f"GPU {gpu_id}"
            log.info(f"[{next_job.id}] Dispatched: {_fname} → {_dest} | {_res} {_hdr_label} | target={gpu_target}")
            encode_queue._claiming = False

        job = next_job
        info = job.file_info
        settings = job.settings
        # Migrate legacy convert_dv → dv_mode
        if "convert_dv" in settings:
            if settings.pop("convert_dv", False):
                settings.setdefault("dv_mode", "hdr10")
        settings["gpu_id"] = encode_queue.job_gpus.get(job.id, 0)
        # Pre-resolve preset so display values are correct immediately
        preset_name = settings.get("preset", "auto")
        if preset_name in ("auto",) or preset_name in PRESETS:
            pre_resolved = resolve_preset(preset_name, info.get("width", 1920), info.get("height", 1080))
            settings["resolved_cq"] = pre_resolved["cq"]
            settings["resolved_maxbitrate"] = pre_resolved["maxbitrate"]
            settings["resolved_speed"] = pre_resolved["speed"]
        else:
            settings["resolved_cq"] = settings.get("cq", 24)
            settings["resolved_maxbitrate"] = settings.get("maxbitrate", "20M")
            settings["resolved_speed"] = settings.get("speed", "p5")
        encode_queue._save_state()
        log.info(f"[{job.id}] Starting encode: {info['filename']} ({info['size_human']}) | {info['codec']} {info['resolution_label']} {info.get('hdr_type','SDR')}")
        log.info(f"[{job.id}] Settings: preset={settings.get('preset')}, video_codec={settings.get('video_codec', 'hevc')}, cq={settings['resolved_cq']}, maxbitrate={settings['resolved_maxbitrate']}, speed={settings['resolved_speed']}, encoder={'CPU' if settings.get('use_cpu') else 'GPU'}, audio={settings.get('audio_codec')} {settings.get('audio_bitrate')}")

        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})

        def _finish_job(status=None, error=None, no_retry=False):
            """Helper to clean up a finished job. Failed jobs auto-retry up to 3 times."""
            if status:
                job.status = status
            if error:
                job.error = error
            job.finished_at = time.time()

            # Auto-retry failed jobs (up to 3 attempts) unless no_retry is set
            max_retries = 3
            retries = job.settings.get("_retry_count", 0)
            if job.status == JobStatus.FAILED and not no_retry and retries < max_retries:
                log.info(f"[{job.id}] Auto-retry {retries + 1}/{max_retries}: {error}")
                # Clean up current attempt
                encode_queue.active_jobs.pop(job.id, None)
                encode_queue.ffmpeg_procs.pop(job.id, None)
                encode_queue.ffmpeg_logs.pop(job.id, None)
                encode_queue._proc_ended_at.pop(job.id, None)
                encode_queue.job_gpus.pop(job.id, None)
                # Reset job for retry — clear remote assignment
                job.status = JobStatus.QUEUED
                job.started_at = None
                job.finished_at = None
                job.progress = None
                job.error = None
                job.settings["_retry_count"] = retries + 1
                for k in ("_remote_server_idx", "_remote_gpu_name", "_remote_encoder_type"):
                    job.settings.pop(k, None)
                if job.settings.get("encoder") == "remote":
                    job.settings["encoder"] = "gpu"
                if job.settings.get("gpu_target", "auto") == "remote":
                    job.settings["gpu_target"] = "auto"
                # Put at front of queue
                encode_queue.queue_order = [j for j in encode_queue.queue_order if j != job.id]
                encode_queue.queue_order.append(job.id)  # back of queue
                encode_queue.running = len(encode_queue.active_jobs) > 0
                encode_queue._save_state()
                return

            encode_queue.queue_order = [j for j in encode_queue.queue_order if j != job.id]
            log_lines = encode_queue.ffmpeg_logs.get(job.id, [])[-100:]
            encode_queue.history.append({
                "id": job.id, "file_info": info, "settings": job.settings,
                "status": job.status,
                "error": job.error, "started_at": job.started_at,
                "finished_at": job.finished_at, "result": job.result or {},
                "log": log_lines,
            })
            _record_encode_stat(job.status, job.result or {}, job.started_at, job.finished_at)
            encode_queue.active_jobs.pop(job.id, None)
            encode_queue.ffmpeg_procs.pop(job.id, None)
            encode_queue.ffmpeg_logs.pop(job.id, None)
            encode_queue._proc_ended_at.pop(job.id, None)
            encode_queue.job_gpus.pop(job.id, None)
            encode_queue.running = len(encode_queue.active_jobs) > 0

        # Check source file still exists
        if not os.path.exists(info.get("path", "")):
            log.warning(f"[{job.id}] Source file no longer exists: {info.get('path')}")
            _finish_job(JobStatus.FAILED, "Source file no longer exists", no_retry=True)
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            continue

        # Check write permissions before encoding
        tmp_dir = app_settings.get("tmp_dir", settings.get("tmp_dir", "/tmp/recode"))
        output_dir = info.get("dirname", "")
        perm_errors = []
        for check_dir, label in [(tmp_dir, "Temp directory"), (output_dir, "Output directory")]:
            if check_dir:
                os.makedirs(check_dir, exist_ok=True)
                test_file = os.path.join(check_dir, f".recode_write_test_{job.id}")
                try:
                    with open(test_file, "w") as tf:
                        tf.write("test")
                    os.remove(test_file)
                except PermissionError:
                    perm_errors.append(f"{label} ({check_dir})")
                except Exception:
                    pass
        if perm_errors:
            err_msg = f"Permission denied: cannot write to {', '.join(perm_errors)}"
            log.error(f"[{job.id}] {err_msg}")
            encode_queue.ffmpeg_logs.setdefault(job.id, []).append(err_msg)
            _finish_job(JobStatus.FAILED, "Permission denied", no_retry=True)
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            continue

        # Check skip conditions
        skip_reason = None
        dv_mode = settings.get("dv_mode", "skip")
        if info.get("hdr_type", "").startswith("Dolby Vision") and dv_mode == "skip":
            skip_reason = "Dolby Vision (set DV mode to 'Keep Original', 'Convert to HDR10', or 'Encode DV')"
        elif settings.get("skip_4k") and (info.get("width", 0) >= 3840 or info.get("height", 0) >= 2160):
            skip_reason = "4K file (Skip 4K enabled)"
        elif settings.get("hdr_only") and info.get("hdr_type") == "SDR":
            skip_reason = "SDR file (HDR Only enabled)"

        if skip_reason:
            log.info(f"[{job.id}] Skipped: {info['filename']} — {skip_reason}")
            _finish_job(JobStatus.SKIPPED, skip_reason)
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            continue

        # GPU fallback: if set to GPU but no GPU/nvenc available, fall back to CPU
        # Skip this check for remote jobs — the remote server handles GPU
        if not settings.get("use_cpu", False) and settings.get("_remote_server_idx", -1) < 0:
            gpu_ok = False
            try:
                gpu_check = await asyncio.create_subprocess_exec(
                    "nvidia-smi", stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                await gpu_check.communicate()
                if gpu_check.returncode != 0:
                    raise RuntimeError("nvidia-smi failed")
                # Also verify nvenc encoder is usable on the assigned GPU
                vc = settings.get("video_codec", "hevc")
                nvenc = "hevc_nvenc" if vc == "hevc" else "h264_nvenc"
                gpu_id = str(settings.get("gpu_id", 0))
                nvenc_check = await asyncio.create_subprocess_exec(
                    FFMPEG, "-hide_banner", "-f", "lavfi", "-i", "nullsrc=s=256x256:d=0.1",
                    "-c:v", nvenc, "-gpu", gpu_id, "-f", "null", "-",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                await nvenc_check.communicate()
                if nvenc_check.returncode != 0:
                    raise RuntimeError(f"{nvenc} not available")
                gpu_ok = True
            except Exception as e:
                log.warning(f"[{job.id}] GPU encoding not available ({e}), falling back to CPU")
                encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"GPU not available ({e}) — falling back to CPU encoding")
                settings["use_cpu"] = True
                await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})

        # Select ffmpeg binary — check if this is a remote GPU job via listener
        use_remote_listener = False
        remote_idx = settings.get("_remote_server_idx", -1)
        if remote_idx >= 0:
            # Check if we have connected GPUs via the listener
            listener_status_file = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-status.json")
            try:
                with open(listener_status_file) as _f:
                    _ls = json.load(_f)
                connected_gpus = [g for g in _ls.get("gpus", []) if g.get("online")]
                if connected_gpus:
                    use_remote_listener = True
                    # Set encoder type from target GPU (for VideoToolbox vs NVENC command building)
                    target_name = settings.get("_remote_gpu_name", "")
                    target_gpu = next((g for g in connected_gpus if g.get("name") == target_name), connected_gpus[0] if connected_gpus else None)
                    if target_gpu:
                        settings["_remote_encoder_type"] = target_gpu.get("encoder_type", "nvenc")
                    log.info(f"[{job.id}] Using remote GPU via listener ({len(connected_gpus)} GPUs available, encoder={settings.get('_remote_encoder_type', 'nvenc')})")
                else:
                    # No remote GPUs — put job back in queue and wait
                    log.info(f"[{job.id}] No remote GPUs connected — waiting in queue")
                    job.status = JobStatus.QUEUED
                    job.started_at = None
                    job.progress = None
                    encode_queue.active_jobs.pop(job.id, None)
                    encode_queue.ffmpeg_procs.pop(job.id, None)
                    encode_queue.job_gpus.pop(job.id, None)
                    encode_queue.queue_order = [j for j in encode_queue.queue_order if j != job.id]
                    encode_queue.queue_order.insert(0, job.id)  # front of queue
                    encode_queue.jobs[job.id] = job
                    encode_queue.running = len(encode_queue.active_jobs) > 0
                    encode_queue._save_state()
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    await asyncio.sleep(5)
                    continue
            except Exception:
                log.info(f"[{job.id}] Listener status not available — waiting in queue")
                await asyncio.sleep(5)
                continue

        ffmpeg_bin = FFMPEG

        # Build ffmpeg command
        try:
            cmd, tmp_output, output_file, resolved = build_ffmpeg_cmd(info, settings, ffmpeg_bin)
            job.settings.update(resolved)
        except Exception as e:
            log.error(f"[{job.id}] Failed to build command for {info['filename']}: {e}")
            encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"Failed to build command: {e}")
            _finish_job(JobStatus.FAILED, "Failed to build encode command")
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            continue

        # Remote listener dispatch — write job file, poll for result
        if use_remote_listener:
            jobs_dir = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-jobs")
            os.makedirs(jobs_dir, exist_ok=True)
            # Build input file info for the listener
            input_files_info = []
            local_paths = []
            for arg_i in range(len(cmd)):
                if cmd[arg_i] == "-i" and arg_i + 1 < len(cmd):
                    path = cmd[arg_i + 1]
                    if os.path.isfile(path):
                        local_paths.append(path)
                        fname = os.path.basename(path)
                        input_files_info.append({
                            "original_path": path,
                            "virtual_name": f"input_{len(input_files_info)}.{fname.rsplit('.', 1)[-1] if '.' in fname else 'mkv'}",
                            "size": os.path.getsize(path),
                        })
            # Determine target GPU name — use explicit name if set, otherwise any
            target_gpu_name = settings.get("_remote_gpu_name", "")
            job_data = {
                "job_id": job.id,
                "ffmpeg_args": cmd[1:],  # skip ffmpeg binary path
                "input_files": input_files_info,
                "local_paths": local_paths,
                "output_path": tmp_output,
                "target_gpu": target_gpu_name,
            }
            job_file = os.path.join(jobs_dir, f"{job.id}.json")
            progress_file = os.path.join(jobs_dir, f"{job.id}.progress")
            result_file = os.path.join(jobs_dir, f"{job.id}.result")
            # Clean any stale files
            for f in (progress_file, result_file):
                if os.path.exists(f): os.remove(f)
            with open(job_file, "w") as _f:
                json.dump(job_data, _f)
            log.info(f"[{job.id}] Remote job dispatched via listener")
            encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"Dispatched to remote GPU via listener")
            duration = info.get("duration_secs", 0) or 0
            job.progress = {
                "pct": 0, "elapsed_secs": 0, "eta_secs": 0,
                "speed": "0x", "bitrate": "0kbits/s", "frame": 0,
                "current_time": 0, "total_time": duration,
                "output_size": 0, "phase": "preparing",
            }
            await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})
            # Poll for progress and result
            import re as _re
            exit_code = 1
            error_msg = ""
            _listener_pid = _remote_client_proc.pid if _remote_client_proc else None
            while True:
                await asyncio.sleep(1)
                # Check if listener died/restarted — all remote jobs are dead
                if _remote_client_proc is None or _remote_client_proc.returncode is not None or (_listener_pid and _remote_client_proc.pid != _listener_pid):
                    log.warning(f"[{job.id}] Listener process died — remote encode lost")
                    exit_code = 1
                    error_msg = "Listener restarted — remote encode lost"
                    for f in (job_file, progress_file, result_file):
                        if os.path.exists(f): os.remove(f)
                    break
                # Check for cancellation
                if job.status == JobStatus.CANCELLED:
                    exit_code = -1
                    # Write cancel file so listener kills the remote encode
                    cancel_file = os.path.join(jobs_dir, f"{job.id}.cancel")
                    with open(cancel_file, "w") as _f:
                        _f.write("cancel")
                    # Wait briefly for result file from listener
                    for _ in range(10):
                        await asyncio.sleep(0.5)
                        if os.path.isfile(result_file):
                            break
                    for f in (job_file, progress_file, result_file, cancel_file):
                        if os.path.exists(f): os.remove(f)
                    break
                # Read progress
                if os.path.isfile(progress_file):
                    try:
                        with open(progress_file) as _f:
                            p = json.load(_f)
                        current_time = p.get("time_secs", 0)
                        frame = p.get("frame", 0)
                        spd = p.get("speed", 0)
                        br = p.get("bitrate_kbps", 0)
                        output_size = p.get("output_size", 0)
                        pct = min(100, round(current_time / duration * 100, 1)) if duration > 0 else 0
                        elapsed = time.time() - job.started_at if job.started_at else 0
                        remaining = max(duration - current_time, 0)
                        eta = remaining / spd if spd > 0 else 0
                        job.progress = {
                            "pct": pct, "elapsed_secs": elapsed, "eta_secs": eta,
                            "speed": f"{spd:.2f}x", "bitrate": f"{br:.1f}kbits/s", "frame": frame,
                            "current_time": current_time, "total_time": duration,
                            "output_size": output_size, "phase": "encoding",
                        }
                        await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})
                    except Exception:
                        pass
                # Check for result
                if os.path.isfile(result_file):
                    try:
                        with open(result_file) as _f:
                            result = json.load(_f)
                        exit_code = result.get("exit_code", 1)
                        error_msg = result.get("error", "")
                        # Capture remote stderr into job log for GUI display
                        stderr_text = result.get("stderr", "")
                        if stderr_text:
                            for line in stderr_text.strip().splitlines()[-20:]:
                                encode_queue.ffmpeg_logs.setdefault(job.id, []).append(line)
                        if exit_code != 0:
                            encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"Remote exit code: {exit_code}")
                            if error_msg:
                                encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"Error: {error_msg}")
                    except Exception as e:
                        exit_code = 1
                        error_msg = f"Failed to read result: {e}"
                    # Cleanup job files
                    for f in (job_file, progress_file, result_file):
                        if os.path.exists(f): os.remove(f)
                    break
            # Remote job finished — exit_code is set, skip local ffmpeg below

        if use_remote_listener:
            # Remote job complete — handle result and post-processing
            job.finished_at = time.time()
            if job.status == JobStatus.CANCELLED:
                _finish_job(JobStatus.CANCELLED, "Cancelled by user")
                await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                continue
            if exit_code != 0:
                _finish_job(JobStatus.FAILED, error_msg or f"Remote encode failed (exit {exit_code})")
                await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                continue
            # Output file is at tmp_output — run DV post-processing locally if needed, then move
            if os.path.exists(tmp_output):
                orig_bytes = info.get("size_bytes", 0)

                # DV RPU injection for remote encode_dv jobs (runs locally — source file is on local disk)
                _dv_mode = settings.get("dv_mode", "skip")
                _dovi_profile = info.get("dovi_profile")
                _is_dv = info.get("hdr_type", "").startswith("Dolby Vision")
                _needs_dv_inject = _is_dv and _dv_mode == "encode_dv"
                if _needs_dv_inject:
                    _is_p5 = _dovi_profile == 5
                    _dv_label = f"DV P{_dovi_profile}→P8.4"
                    log.info(f"[{job.id}] {_dv_label} — RPU injection starting (local post-process)")
                    encode_queue.ffmpeg_logs.get(job.id, []).append(f"=== {_dv_label} (local post-process) ===")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    tmp_dir = app_settings.get("tmp_dir", "/var/lib/plex/tmp")
                    src_path = info["path"]
                    rpu_bin = os.path.join(tmp_dir, f"{job.id}_rpu.bin")
                    encoded_hevc = os.path.join(tmp_dir, f"{job.id}_encoded.hevc")
                    injected_hevc = os.path.join(tmp_dir, f"{job.id}_injected.hevc")
                    remuxed_mkv = os.path.join(tmp_dir, f"{job.id}_dv.mkv")
                    dovi_cleanup = [rpu_bin, encoded_hevc, injected_hevc, remuxed_mkv]
                    try:
                        _dovi_mode_str = " -m 4"
                        _rpu_duration = "-t 300 " if app_settings.get("test_mode") else ""
                        log.info(f"[{job.id}] {_dv_label} step 1/4: extracting RPU")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 1/4: extracting RPU...")
                        pipe_cmd = (
                            f'{FFMPEG} {_rpu_duration}-i "{src_path}" -c:v copy -bsf:v hevc_mp4toannexb'
                            f' -an -sn -f hevc pipe:1 2>/dev/null'
                            f' | {DOVI_TOOL}{_dovi_mode_str} extract-rpu - -o "{rpu_bin}"'
                        )
                        p = await asyncio.create_subprocess_shell(
                            pipe_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, dovi_err = await p.communicate()
                        if p.returncode != 0 or not os.path.exists(rpu_bin) or os.path.getsize(rpu_bin) == 0:
                            raise RuntimeError(f"RPU extraction failed: {dovi_err.decode(errors='replace')[-200:]}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"RPU extracted ({human_size(os.path.getsize(rpu_bin))})")

                        log.info(f"[{job.id}] {_dv_label} step 2/4: extracting HEVC bitstream")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 2/4: extracting HEVC bitstream...")
                        p = await asyncio.create_subprocess_exec(
                            FFMPEG, "-y", "-i", tmp_output, "-c:v", "copy", "-an", "-sn",
                            "-bsf:v", "hevc_mp4toannexb,filter_units=remove_types=62", "-f", "hevc", encoded_hevc,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0:
                            raise RuntimeError(f"HEVC extract failed: {stderr_out.decode(errors='replace')[-200:]}")

                        log.info(f"[{job.id}] {_dv_label} step 3/4: injecting RPU")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 3/4: injecting RPU...")
                        p = await asyncio.create_subprocess_exec(
                            DOVI_TOOL, "inject-rpu", "-i", encoded_hevc, "--rpu-in", rpu_bin, "-o", injected_hevc,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0:
                            raise RuntimeError(f"RPU inject failed: {stderr_out.decode(errors='replace')[-200:]}")
                        for f in [encoded_hevc, rpu_bin]:
                            if os.path.exists(f): os.remove(f)

                        log.info(f"[{job.id}] {_dv_label} step 4/4: muxing with mkvmerge")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 4/4: muxing with mkvmerge...")
                        mkvmerge_bin = _find_bin("mkvmerge") if os.path.isfile(_find_bin("mkvmerge")) else None
                        if mkvmerge_bin:
                            p = await asyncio.create_subprocess_exec(
                                mkvmerge_bin, "-o", remuxed_mkv,
                                injected_hevc, "-D", tmp_output,
                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                            _, stderr_out = await p.communicate()
                            if p.returncode > 1:
                                raise RuntimeError(f"mkvmerge failed (exit {p.returncode}): {stderr_out.decode(errors='replace')[-200:]}")
                        else:
                            raise RuntimeError("mkvmerge not found")
                        if os.path.exists(injected_hevc): os.remove(injected_hevc)
                        patch_dvvc_compat_id(remuxed_mkv, 4)
                        os.remove(tmp_output)
                        shutil.move(remuxed_mkv, tmp_output)
                        log.info(f"[{job.id}] {_dv_label} complete — {human_size(os.path.getsize(tmp_output))}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"{_dv_label} complete — {human_size(os.path.getsize(tmp_output))}")
                    except Exception as e:
                        log.error(f"[{job.id}] {_dv_label} failed: {e}")
                        encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"DV conversion failed: {e}")
                        for f in dovi_cleanup:
                            if os.path.exists(f): os.remove(f)
                        if os.path.exists(tmp_output): os.remove(tmp_output)
                        _finish_job(JobStatus.FAILED, f"{_dv_label} failed")
                        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                        continue
                    finally:
                        for f in dovi_cleanup:
                            if os.path.exists(f): os.remove(f)

                # HDR10 → DV P8.4 upgrade (runs locally)
                _needs_dv_upgrade = (
                    _dv_mode == "encode_dv" and not _is_dv
                    and info.get("is_hdr", False) and info.get("hdr_type", "") == "HDR10"
                    and os.path.exists(tmp_output)
                )
                if _needs_dv_upgrade:
                    _dv_label = "HDR10→DV P8.4"
                    log.info(f"[{job.id}] {_dv_label} — generating RPU (local post-process)")
                    encode_queue.ffmpeg_logs.get(job.id, []).append(f"=== {_dv_label} (local post-process) ===")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    tmp_dir = app_settings.get("tmp_dir", "/var/lib/plex/tmp")
                    gen_json = os.path.join(tmp_dir, f"{job.id}_dv_gen.json")
                    rpu_bin = os.path.join(tmp_dir, f"{job.id}_rpu.bin")
                    encoded_hevc = os.path.join(tmp_dir, f"{job.id}_encoded.hevc")
                    injected_hevc = os.path.join(tmp_dir, f"{job.id}_injected.hevc")
                    remuxed_mkv = os.path.join(tmp_dir, f"{job.id}_dv.mkv")
                    dovi_cleanup = [gen_json, rpu_bin, encoded_hevc, injected_hevc, remuxed_mkv]
                    try:
                        p = await asyncio.create_subprocess_exec(
                            FFPROBE, "-v", "error", "-select_streams", "v:0",
                            "-show_entries", "stream=nb_frames,r_frame_rate,duration",
                            "-of", "csv=p=0", tmp_output,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        probe_out, _ = await p.communicate()
                        probe_parts = probe_out.decode().strip().split(",")
                        frame_rate_str = probe_parts[0] if probe_parts else "24000/1001"
                        frame_count = 0
                        if len(probe_parts) > 1 and probe_parts[1].strip().isdigit():
                            frame_count = int(probe_parts[1])
                        if frame_count == 0:
                            try: frn, frd = frame_rate_str.split("/"); fps = float(frn) / float(frd)
                            except Exception: fps = 23.976
                            dur = float(probe_parts[2]) if len(probe_parts) > 2 and probe_parts[2].strip() else info.get("duration_secs", 0)
                            frame_count = int(dur * fps) or 1000

                        hdr_meta = info.get("hdr10_metadata", {})
                        min_lum_u16 = int(round(hdr_meta.get("min_lum", 0.005) * 10000))
                        gen_config = {"cm_version": "V40", "length": frame_count, "level6": {
                            "max_display_mastering_luminance": int(hdr_meta.get("max_lum", 1000)),
                            "min_display_mastering_luminance": min_lum_u16,
                            "max_content_light_level": int(hdr_meta.get("max_cll", 1000)),
                            "max_frame_average_light_level": int(hdr_meta.get("max_fall", 400)),
                        }}
                        with open(gen_json, "w") as f: json.dump(gen_config, f)

                        p = await asyncio.create_subprocess_exec(
                            DOVI_TOOL, "generate", "-j", gen_json, "-o", rpu_bin, "--profile", "8.4",
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0 or not os.path.exists(rpu_bin) or os.path.getsize(rpu_bin) == 0:
                            raise RuntimeError(f"RPU generation failed: {stderr_out.decode(errors='replace')[-200:]}")

                        p = await asyncio.create_subprocess_exec(
                            FFMPEG, "-y", "-i", tmp_output, "-c:v", "copy", "-an", "-sn",
                            "-bsf:v", "hevc_mp4toannexb", "-f", "hevc", encoded_hevc,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0:
                            raise RuntimeError(f"HEVC extract failed: {stderr_out.decode(errors='replace')[-200:]}")

                        p = await asyncio.create_subprocess_exec(
                            DOVI_TOOL, "inject-rpu", "-i", encoded_hevc, "--rpu-in", rpu_bin, "-o", injected_hevc,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0:
                            raise RuntimeError(f"RPU inject failed: {stderr_out.decode(errors='replace')[-200:]}")
                        for f in [encoded_hevc, rpu_bin, gen_json]:
                            if os.path.exists(f): os.remove(f)

                        mkvmerge_bin = _find_bin("mkvmerge") if os.path.isfile(_find_bin("mkvmerge")) else None
                        if mkvmerge_bin:
                            p = await asyncio.create_subprocess_exec(
                                mkvmerge_bin, "-o", remuxed_mkv,
                                injected_hevc, "-D", tmp_output,
                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                            _, stderr_out = await p.communicate()
                            if p.returncode > 1:
                                raise RuntimeError(f"mkvmerge failed: {stderr_out.decode(errors='replace')[-200:]}")
                        else:
                            raise RuntimeError("mkvmerge not found")
                        if os.path.exists(injected_hevc): os.remove(injected_hevc)
                        patch_dvvc_compat_id(remuxed_mkv, 4)
                        os.remove(tmp_output)
                        shutil.move(remuxed_mkv, tmp_output)
                        log.info(f"[{job.id}] {_dv_label} complete — {human_size(os.path.getsize(tmp_output))}")
                    except Exception as e:
                        log.error(f"[{job.id}] {_dv_label} failed: {e}")
                        for f in dovi_cleanup:
                            if os.path.exists(f): os.remove(f)
                        if os.path.exists(tmp_output): os.remove(tmp_output)
                        _finish_job(JobStatus.FAILED, f"{_dv_label} failed")
                        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                        continue
                    finally:
                        for f in dovi_cleanup:
                            if os.path.exists(f): os.remove(f)

                new_bytes = os.path.getsize(tmp_output)
                if settings.get("discard_larger") and new_bytes >= orig_bytes and orig_bytes > 0:
                    log.info(f"[{job.id}] Encoded file larger ({new_bytes} >= {orig_bytes}) — discarding")
                    os.remove(tmp_output)
                    _finish_job(JobStatus.SKIPPED, "Encoded file is larger than original")
                else:
                    try:
                        shutil.move(tmp_output, output_file)
                        log.info(f"[{job.id}] Remote encode complete: {output_file} ({new_bytes/1e9:.2f} GB)")
                        saved_pct = round((1 - new_bytes / orig_bytes) * 100) if orig_bytes > 0 else 0
                        action = "kept original"
                        if settings.get("delete_original", False):
                            if app_settings.get("test_mode"):
                                log.info(f"[{job.id}] Test mode (5 min) — not deleting original")
                                encode_queue.ffmpeg_logs.setdefault(job.id, []).append("Test mode (5 min) — not deleting original")
                            else:
                                try:
                                    os.remove(info["path"])
                                    action = "deleted original"
                                    log.info(f"[{job.id}] Deleted original: {info['path']}")
                                except Exception as e:
                                    action = "failed to delete original"
                                    log.warning(f"[{job.id}] Failed to delete original: {e}")
                        # Write manifest entry for this encode
                        write_recode_manifest_entry(info["path"], output_file)
                        job.result = {
                            "output_path": output_file,
                            "orig_size": human_size(orig_bytes),
                            "new_size": human_size(new_bytes),
                            "orig_bytes": orig_bytes,
                            "new_bytes": new_bytes,
                            "saved_pct": saved_pct,
                            "action": action,
                            "larger": new_bytes >= orig_bytes,
                        }
                        log.info(f"[{job.id}] Remote encode complete: {output_file} ({human_size(new_bytes)}, saved {saved_pct}%)")
                        _finish_job(JobStatus.DONE, "")
                    except Exception as e:
                        log.error(f"[{job.id}] Failed to move output: {e}")
                        _finish_job(JobStatus.FAILED, f"Failed to move output: {e}")
            else:
                _finish_job(JobStatus.FAILED, "Output file not created (exit_code=0)")
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            continue

        log.info(f"[{job.id}] CMD: {' '.join(cmd[:20])}...")
        encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"$ {' '.join(cmd)}")
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=None,
            )
            encode_queue.ffmpeg_procs[job.id] = proc
            encode_queue.ffmpeg_logs[job.id] = []
            duration = info.get("duration_secs", 0) or 0

            # Read stderr in background for local jobs (captures errors/warnings)
            async def _read_stderr(proc, job_id):
                async for line in proc.stderr:
                    text = line.decode(errors='replace').rstrip()
                    if text:
                        logs = encode_queue.ffmpeg_logs.get(job_id, [])
                        logs.append(text)
                        if len(logs) > 500:
                            encode_queue.ffmpeg_logs[job_id] = logs[-500:]
            stderr_task = None
            is_stats_mode = settings.get("_remote_server_idx", -1) >= 0
            if not is_stats_mode:
                stderr_task = asyncio.create_task(_read_stderr(proc, job.id))

            # Broadcast initial "preparing" progress so UI shows feedback immediately
            job.progress = {
                "pct": 0, "elapsed_secs": 0, "eta_secs": 0,
                "speed": "0x", "bitrate": "0kbits/s", "frame": 0,
                "current_time": 0, "total_time": duration,
                "output_size": 0, "phase": "preparing",
            }
            await manager.broadcast({
                "type": "progress_update",
                "data": {"id": job.id, "progress": job.progress}
            })

            # Parse progress — remote uses stderr (-stats), local uses stdout (-progress pipe:1)
            current_time = 0
            speed = "0x"
            bitrate = "0kbits/s"
            frame = 0
            _fps = 0.0
            _total_size = 0
            # Remote: read stderr in chunks (stats uses \r not \n)
            # Local: read stdout lines (-progress pipe:1 outputs key=value pairs)
            if is_stats_mode:
                import re as _re
                _stats_buf = ""
                while True:
                    chunk = await proc.stderr.read(1024)
                    if not chunk:
                        break
                    _stats_buf += chunk.decode(errors="replace")
                    parts = _stats_buf.replace("\r", "\n").split("\n")
                    _stats_buf = parts[-1]
                    for line_text in parts[:-1]:
                        line_text = line_text.strip()
                        if not line_text:
                            continue
                        if "frame=" in line_text and "time=" in line_text:
                            m = _re.search(r'frame=\s*(\d+)', line_text)
                            if m: frame = int(m.group(1))
                            m = _re.search(r'time=(\d+):(\d+):(\d+\.?\d*)', line_text)
                            if m: current_time = int(m.group(1))*3600 + int(m.group(2))*60 + float(m.group(3))
                            m = _re.search(r'speed=\s*([\d.]+)x', line_text)
                            if m: speed = m.group(1) + "x"
                            m = _re.search(r'bitrate=\s*([\d.]+\s*\w+/s)', line_text)
                            if m: bitrate = m.group(1)
                            m = _re.search(r'[Ll]?size=\s*(\d+)\s*[kK]i?B', line_text)
                            stats_size = int(m.group(1)) * 1024 if m else 0
                            pct = min(100, round(current_time / duration * 100, 1)) if duration > 0 else 0
                            elapsed = time.time() - job.started_at if job.started_at else 0
                            remaining = max(duration - current_time, 0)
                            try:
                                speed_num = float(speed.rstrip("x "))
                                eta = remaining / speed_num if speed_num > 0 else 0
                            except (ValueError, ZeroDivisionError):
                                eta = 0
                            try:
                                output_size = stats_size if stats_size > 0 else (os.path.getsize(tmp_output) if os.path.exists(tmp_output) else 0)
                            except OSError:
                                output_size = 0
                            job.progress = {
                                "pct": pct, "elapsed_secs": elapsed, "eta_secs": eta,
                                "speed": speed, "bitrate": bitrate, "frame": frame,
                                "current_time": current_time, "total_time": duration,
                                "output_size": output_size, "phase": "encoding",
                            }
                            if time.time() - getattr(job, '_last_broadcast', 0) > 1:
                                job._last_broadcast = time.time()
                                await manager.broadcast({
                                    "type": "progress_update",
                                    "data": {"id": job.id, "progress": job.progress}
                                })
                        else:
                            encode_queue.ffmpeg_logs.setdefault(job.id, []).append(line_text)
            else:
                async for line in proc.stdout:
                    if job.status == JobStatus.CANCELLED:
                        break
                    line = line.decode().strip()
                    if not line or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    if key == "out_time_us":
                        try: current_time = int(value) / 1_000_000
                        except ValueError: pass
                    elif key == "fps":
                        try: _fps = float(value)
                        except ValueError: pass
                    elif key == "speed":
                        speed = value
                    elif key == "bitrate":
                        bitrate = value
                    elif key == "total_size":
                        try: _total_size = int(value)
                        except ValueError: pass
                    elif key == "frame":
                        try: frame = int(value)
                        except ValueError: pass
                    elif key == "progress":
                        # Fallback: estimate current_time from frame count when out_time_us is N/A
                        est_time = current_time
                        if est_time <= 0 and frame > 0 and _fps > 0:
                            est_time = frame / _fps
                        pct = 0
                        eta = 0
                        if duration > 0 and est_time > 0:
                            pct = min(round(est_time * 100 / duration, 1), 100)
                            remaining = max(duration - est_time, 0)
                            try:
                                speed_num = float(speed.rstrip("x "))
                                if speed_num > 0: eta = remaining / speed_num
                            except (ValueError, ZeroDivisionError):
                                # speed is N/A — estimate from elapsed time
                                elapsed_so_far = time.time() - job.started_at
                                if elapsed_so_far > 0 and est_time > 0:
                                    speed_num = est_time / elapsed_so_far
                                    speed = f"{speed_num:.2f}x"
                                    eta = remaining / speed_num if speed_num > 0 else 0
                        elapsed = time.time() - job.started_at
                        try:
                            output_size = _total_size if _total_size > 0 else (os.path.getsize(tmp_output) if os.path.exists(tmp_output) else 0)
                        except OSError:
                            output_size = 0
                        job.progress = {
                            "pct": pct, "elapsed_secs": elapsed, "eta_secs": eta,
                            "speed": speed, "bitrate": bitrate, "frame": frame,
                            "current_time": est_time, "total_time": duration,
                            "output_size": output_size, "phase": "encoding",
                        }
                        await manager.broadcast({
                            "type": "progress_update",
                            "data": {"id": job.id, "progress": job.progress}
                        })

            await proc.wait()
            if stderr_task:
                await stderr_task
            exit_code = proc.returncode

        except Exception as e:
            # Check if job was requeued while encoding
            if job.status == JobStatus.QUEUED and job.id not in encode_queue.active_jobs:
                log.info(f"[{job.id}] Job was requeued — skipping error handling")
                continue
            log.error(f"[{job.id}] ffmpeg exception for {info['filename']}: {e}")
            if os.path.exists(tmp_output):
                os.remove(tmp_output)
            _finish_job(JobStatus.FAILED, str(e))
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            continue

        # Keep proc in ffmpeg_procs until _finish_job — watchdog uses it to detect DV post-processing
        job.finished_at = time.time()

        # Check if job was requeued while encoding (e.g. concurrent limit lowered)
        if job.status == JobStatus.QUEUED and job.id not in encode_queue.active_jobs:
            log.info(f"[{job.id}] Job was requeued — skipping post-encode processing")
            continue

        if job.status == JobStatus.CANCELLED:
            log.info(f"[{job.id}] Cancelled: {info['filename']}")
            if os.path.exists(tmp_output):
                os.remove(tmp_output)
            _finish_job(JobStatus.CANCELLED, "Cancelled by user")
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            continue

        if exit_code != 0:
            # Check if this is a CUDA decoder surfaces error — retry with software decode
            log_text = "\n".join(encode_queue.ffmpeg_logs.get(job.id, []))
            gpu_oom_errors = ("out of memory", "Cannot allocate memory")
            if job.status != JobStatus.CANCELLED and not settings.get("use_cpu") and any(e in log_text for e in gpu_oom_errors):
                oom_retries = settings.get("_oom_retries", 0) + 1
                if oom_retries > 3:
                    log.error(f"[{job.id}] GPU OOM after {oom_retries - 1} retries, failing: {info['filename']}")
                    _finish_job(JobStatus.FAILED, f"GPU out of memory after {oom_retries - 1} retries")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    continue
                log.warning(f"[{job.id}] GPU out of memory for {info['filename']}, re-queuing (attempt {oom_retries}/3)")
                encode_queue.ffmpeg_logs.get(job.id, []).append(f"=== GPU out of memory — re-queued (attempt {oom_retries}/3) ===")
                if os.path.exists(tmp_output):
                    os.remove(tmp_output)
                # Move back to queued state — put at END so other jobs get a turn
                settings["_oom_retries"] = oom_retries
                job.status = JobStatus.QUEUED
                job.started_at = None
                job.finished_at = None
                job.progress = None
                job.error = None
                encode_queue.active_jobs.pop(job.id, None)
                encode_queue.ffmpeg_procs.pop(job.id, None)
                encode_queue._proc_ended_at.pop(job.id, None)
                encode_queue.job_gpus.pop(job.id, None)
                encode_queue.queue_order = [j for j in encode_queue.queue_order if j != job.id]
                encode_queue.queue_order.append(job.id)  # back of queue
                encode_queue.jobs[job.id] = job
                encode_queue.running = len(encode_queue.active_jobs) > 0
                encode_queue._save_state()
                await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                continue
            cuda_errors = ("No decoder surfaces left", "Failed to inject frame into filter", "hwaccel initialisation returned error", "CUDA_ERROR")
            if job.status != JobStatus.CANCELLED and not settings.get("use_cpu") and any(e in log_text for e in cuda_errors):
                log.warning(f"[{job.id}] CUDA decode failed for {info['filename']}, retrying with software decode + GPU encode")
                encode_queue.ffmpeg_logs[job.id].append("=== Retrying with software decoding (GPU encode still active) ===")
                await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                if os.path.exists(tmp_output):
                    os.remove(tmp_output)
                # Rebuild command without hwaccel decode but keep GPU encode
                retry_cmd = [c for i, c in enumerate(cmd) if not (
                    c in ("-hwaccel", "cuda", "-hwaccel_output_format", "-hwaccel_device", "-extra_hw_frames", "16")
                    or (i > 0 and cmd[i-1] in ("-hwaccel", "-hwaccel_output_format", "-hwaccel_device", "-extra_hw_frames"))
                )]
                try:
                    proc = await asyncio.create_subprocess_exec(
                        *retry_cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    encode_queue.ffmpeg_procs[job.id] = proc
                    job.started_at = time.time()
                    log.info(f"[{job.id}] CUDA retry CMD: {' '.join(retry_cmd[:10])}... (PID {proc.pid})")
                    encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"Retry PID: {proc.pid}")

                    async def _read_stderr2(proc, job_id):
                        async for line in proc.stderr:
                            text = line.decode(errors='replace').rstrip()
                            if text:
                                logs = encode_queue.ffmpeg_logs.get(job_id, [])
                                logs.append(text)
                                if len(logs) > 500:
                                    encode_queue.ffmpeg_logs[job_id] = logs[-500:]
                    stderr_task2 = asyncio.create_task(_read_stderr2(proc, job.id))

                    current_time = 0
                    speed = "0x"
                    bitrate = "0kbits/s"
                    frame = 0
                    _fps2 = 0.0
                    _total_size2 = 0
                    async for line in proc.stdout:
                        if job.status == JobStatus.CANCELLED:
                            break
                        line = line.decode().strip()
                        if "=" not in line:
                            continue
                        key, _, value = line.partition("=")
                        key = key.strip()
                        value = value.strip()
                        if key == "out_time_us":
                            try: current_time = int(value) / 1_000_000
                            except ValueError: pass
                        elif key == "fps":
                            try: _fps2 = float(value)
                            except ValueError: pass
                        elif key == "speed": speed = value
                        elif key == "bitrate": bitrate = value
                        elif key == "total_size":
                            try: _total_size2 = int(value)
                            except ValueError: pass
                        elif key == "frame":
                            try: frame = int(value)
                            except ValueError: pass
                        elif key == "progress":
                            est_time = current_time
                            if est_time <= 0 and frame > 0 and _fps2 > 0:
                                est_time = frame / _fps2
                            pct = 0
                            eta = 0
                            if duration > 0 and est_time > 0:
                                pct = min(round(est_time * 100 / duration, 1), 100)
                                remaining = max(duration - est_time, 0)
                                try:
                                    speed_num = float(speed.rstrip("x "))
                                    if speed_num > 0: eta = remaining / speed_num
                                except (ValueError, ZeroDivisionError):
                                    elapsed_so_far = time.time() - job.started_at
                                    if elapsed_so_far > 0 and est_time > 0:
                                        speed_num = est_time / elapsed_so_far
                                        speed = f"{speed_num:.2f}x"
                                        eta = remaining / speed_num if speed_num > 0 else 0
                            elapsed_t = time.time() - job.started_at
                            try: output_size = _total_size2 if _total_size2 > 0 else (os.path.getsize(tmp_output) if os.path.exists(tmp_output) else 0)
                            except OSError: output_size = 0
                            latest_cpu = stats_history["cpu"][-1]["v"] if stats_history["cpu"] else 0
                            latest_gpu = stats_history["gpu"][-1]["v"] if stats_history["gpu"] else 0
                            latest_gpu_temp = stats_history["gpu_temp"][-1]["v"] if stats_history["gpu_temp"] else 0
                            job.progress = {"pct": pct, "elapsed_secs": elapsed_t, "eta_secs": eta, "speed": speed, "bitrate": bitrate, "frame": frame, "current_time": est_time, "total_time": duration, "output_size": output_size, "cpu": latest_cpu, "gpu": latest_gpu, "gpu_temp": latest_gpu_temp, "phase": "encoding"}
                            await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})

                    await proc.wait()
                    await stderr_task2
                    exit_code = proc.returncode
                    # Keep proc in ffmpeg_procs until _finish_job — watchdog uses it to detect DV post-processing
                    job.finished_at = time.time()
                except Exception as e:
                    if os.path.exists(tmp_output):
                        os.remove(tmp_output)
                    encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"Retry failed: {e}")
                    _finish_job(JobStatus.FAILED, "CUDA retry failed")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    continue

                if job.status == JobStatus.CANCELLED:
                    if os.path.exists(tmp_output):
                        os.remove(tmp_output)
                    _finish_job(JobStatus.CANCELLED, "Cancelled by user")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    continue

            # Check for subtitle codec errors — retry without subs
            if exit_code != 0 and job.status != JobStatus.CANCELLED:
                log_text2 = "\n".join(encode_queue.ffmpeg_logs.get(job.id, []))
                if "Subtitle codec" in log_text2 or ("subtitle" in log_text2.lower() and "not supported" in log_text2.lower()):
                    log.warning(f"[{job.id}] Subtitle codec unsupported, retrying without subtitles")
                    encode_queue.ffmpeg_logs.get(job.id, []).append("=== Subtitle codec unsupported, retrying without subtitles ===")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    if os.path.exists(tmp_output):
                        os.remove(tmp_output)
                    retry_cmd = [c for i, c in enumerate(cmd) if not (c == "-map" and i + 1 < len(cmd) and cmd[i + 1].startswith("0:s")) and not (i > 0 and cmd[i - 1] == "-map" and c.startswith("0:s")) and not (c == "-c:s" or (i > 0 and cmd[i - 1] == "-c:s"))]
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            *retry_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        encode_queue.ffmpeg_procs[job.id] = proc
                        job.started_at = time.time()

                        async def _read_stderr_sub(p, jid):
                            async for line in p.stderr:
                                encode_queue.ffmpeg_logs.setdefault(jid, []).append(line.decode(errors='replace').rstrip())
                        stderr_task_sub = asyncio.create_task(_read_stderr_sub(proc, job.id))

                        _fps3 = 0.0
                        _total_size3 = 0
                        async for line in proc.stdout:
                            if job.status == JobStatus.CANCELLED:
                                break
                            line = line.decode().strip()
                            if "=" not in line:
                                continue
                            key, _, value = line.partition("=")
                            key = key.strip(); value = value.strip()
                            if key == "out_time_us":
                                try: current_time = int(value) / 1_000_000
                                except ValueError: pass
                            elif key == "fps":
                                try: _fps3 = float(value)
                                except ValueError: pass
                            elif key == "speed": speed = value
                            elif key == "bitrate": bitrate = value
                            elif key == "total_size":
                                try: _total_size3 = int(value)
                                except ValueError: pass
                            elif key == "frame":
                                try: frame = int(value)
                                except ValueError: pass
                            elif key == "progress":
                                est_time = current_time
                                if est_time <= 0 and frame > 0 and _fps3 > 0:
                                    est_time = frame / _fps3
                                pct = min(round(est_time * 100 / duration, 1), 100) if duration > 0 and est_time > 0 else 0
                                remaining = max(duration - est_time, 0)
                                try:
                                    speed_num = float(speed.rstrip("x "))
                                    eta = remaining / speed_num if speed_num > 0 else 0
                                except (ValueError, ZeroDivisionError):
                                    elapsed_so_far = time.time() - job.started_at
                                    if elapsed_so_far > 0 and est_time > 0:
                                        speed_num = est_time / elapsed_so_far
                                        speed = f"{speed_num:.2f}x"
                                        eta = remaining / speed_num if speed_num > 0 else 0
                                    else:
                                        eta = 0
                                elapsed_s = time.time() - job.started_at
                                try: output_size = _total_size3 if _total_size3 > 0 else (os.path.getsize(tmp_output) if os.path.exists(tmp_output) else 0)
                                except OSError: output_size = 0
                                latest_cpu = stats_history["cpu"][-1]["v"] if stats_history["cpu"] else 0
                                latest_gpu = stats_history["gpu"][-1]["v"] if stats_history["gpu"] else 0
                                latest_gpu_temp = stats_history["gpu_temp"][-1]["v"] if stats_history["gpu_temp"] else 0
                                job.progress = {"pct": pct, "elapsed_secs": elapsed_s, "eta_secs": eta, "speed": speed, "bitrate": bitrate, "frame": frame, "current_time": est_time, "total_time": duration, "output_size": output_size, "cpu": latest_cpu, "gpu": latest_gpu, "gpu_temp": latest_gpu_temp, "phase": "encoding"}
                                await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})

                        await proc.wait()
                        await stderr_task_sub
                        exit_code = proc.returncode
                        # Keep proc in ffmpeg_procs until _finish_job — watchdog uses it to detect DV post-processing
                        job.finished_at = time.time()
                    except Exception as e:
                        if os.path.exists(tmp_output):
                            os.remove(tmp_output)
                        encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"Retry without subs failed: {e}")
                        _finish_job(JobStatus.FAILED, "Subtitle retry failed")
                        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                        continue

            if exit_code != 0:
                job.status = JobStatus.FAILED
                job.error = f"ffmpeg exited with code {exit_code}"
                log.error(f"[{job.id}] Failed: {info['filename']} — ffmpeg exited with code {exit_code}")
                if os.path.exists(tmp_output):
                    os.remove(tmp_output)
        elif not os.path.exists(tmp_output) or os.path.getsize(tmp_output) == 0:
            job.status = JobStatus.FAILED
            exists = os.path.exists(tmp_output)
            size = os.path.getsize(tmp_output) if exists else 0
            job.error = f"Output file {'empty' if exists else 'not created'} (exit_code={exit_code})"
            log.error(f"[{job.id}] Failed: {info['filename']} — output {'empty' if exists else 'missing'} (exit={exit_code}, path={tmp_output})")
            if exists:
                os.remove(tmp_output)
        else:
            orig_bytes = info["size_bytes"]
            new_bytes = os.path.getsize(tmp_output)
            saved_pct = int((orig_bytes - new_bytes) * 100 / orig_bytes) if orig_bytes > 0 else 0

            elapsed = time.time() - job.started_at
            elapsed_str = f"{int(elapsed//3600)}h{int((elapsed%3600)//60)}m{int(elapsed%60)}s" if elapsed >= 3600 else f"{int(elapsed//60)}m{int(elapsed%60)}s"
            log.info(f"[{job.id}] Encode finished: {info['filename']} | {human_size(orig_bytes)} -> {human_size(new_bytes)} ({saved_pct:+d}%) | took {elapsed_str}")

            if new_bytes >= orig_bytes and settings.get("discard_larger", False):
                job.status = JobStatus.SKIPPED
                job.error = f"Encoded file larger ({human_size(new_bytes)} vs {human_size(orig_bytes)}) — discarded"
                log.info(f"[{job.id}] Discarded: encoded file is larger ({human_size(new_bytes)} vs {human_size(orig_bytes)}) — deleted temp output")
                os.remove(tmp_output)
                job.result = {
                    "orig_size": human_size(orig_bytes),
                    "new_size": human_size(new_bytes),
                    "orig_bytes": orig_bytes,
                    "new_bytes": new_bytes,
                    "saved_pct": saved_pct,
                    "action": "discarded",
                }
            else:
                if new_bytes >= orig_bytes:
                    log.info(f"[{job.id}] Note: encoded file is larger ({human_size(new_bytes)} vs {human_size(orig_bytes)}) but discard_larger is off — keeping")

                # DV RPU injection post-processing (encode_dv mode only)
                dv_mode = settings.get("dv_mode", "skip")
                dovi_profile = info.get("dovi_profile")
                is_dv = info.get("hdr_type", "").startswith("Dolby Vision")
                needs_dv_inject = is_dv and dv_mode == "encode_dv"
                if needs_dv_inject:
                    is_p5 = dovi_profile == 5
                    dv_label = f"DV P{dovi_profile}→P8.4"
                    log.info(f"[{job.id}] {dv_label} — RPU injection starting")
                    encode_queue.ffmpeg_logs.get(job.id, []).append(f"=== {dv_label} ===")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    tmp_dir = settings.get("tmp_dir", "/var/lib/plex/tmp")
                    src_path = info["path"]
                    rpu_bin = os.path.join(tmp_dir, f"{job.id}_rpu.bin")
                    encoded_hevc = os.path.join(tmp_dir, f"{job.id}_encoded.hevc")
                    injected_hevc = os.path.join(tmp_dir, f"{job.id}_injected.hevc")
                    remuxed_mkv = os.path.join(tmp_dir, f"{job.id}_dv.mkv")
                    dovi_cleanup = [rpu_bin, encoded_hevc, injected_hevc, remuxed_mkv]
                    try:
                        # Step 1: Extract RPU from source and convert to P8.4
                        # P5: -m 4 (IPTPQc2 → P8.4 with HLG compatibility)
                        # All others: -m 4 (ensures P8.4)
                        if is_p5:
                            dovi_mode_str = " -m 4"
                            mode_desc = "P5 → P8.4 via -m 4"
                        else:
                            dovi_mode_str = " -m 4"
                            mode_desc = f"P{dovi_profile} → P8.4 via -m 4"
                        log.info(f"[{job.id}] {dv_label} step 1/4: extracting RPU ({mode_desc})")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"DV step 1/4: extracting RPU ({mode_desc})...")
                        _rpu_duration = "-t 300 " if app_settings.get("test_mode") else ""
                        pipe_cmd = (
                            f'{FFMPEG} {_rpu_duration}-i "{src_path}" -c:v copy -bsf:v hevc_mp4toannexb'
                            f' -an -sn -f hevc pipe:1 2>/dev/null'
                            f' | {DOVI_TOOL}{dovi_mode_str} extract-rpu - -o "{rpu_bin}"'
                        )
                        p = await asyncio.create_subprocess_shell(
                            pipe_cmd,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, dovi_err = await p.communicate()
                        if p.returncode != 0 or not os.path.exists(rpu_bin) or os.path.getsize(rpu_bin) == 0:
                            raise RuntimeError(f"RPU extraction failed: {dovi_err.decode(errors='replace')[-200:]}")
                        rpu_size = os.path.getsize(rpu_bin)
                        log.info(f"[{job.id}] RPU extracted ({human_size(rpu_size)})")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"RPU extracted ({human_size(rpu_size)})")
                        await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})

                        # Step 2: Extract raw HEVC from encoded file (strip any DV NALs)
                        log.info(f"[{job.id}] {dv_label} step 2/4: extracting encoded HEVC bitstream")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 2/4: extracting encoded HEVC bitstream...")
                        p = await asyncio.create_subprocess_exec(
                            FFMPEG, "-y", "-i", tmp_output, "-c:v", "copy", "-an", "-sn",
                            "-bsf:v", "hevc_mp4toannexb,filter_units=remove_types=62", "-f", "hevc", encoded_hevc,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0:
                            raise RuntimeError(f"Encoded HEVC extract failed: {stderr_out.decode(errors='replace')[-200:]}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"HEVC extracted ({human_size(os.path.getsize(encoded_hevc))})")
                        await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})

                        # Step 3: Inject RPU into encoded HEVC
                        log.info(f"[{job.id}] {dv_label} step 3/4: injecting RPU")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 3/4: injecting RPU...")
                        p = await asyncio.create_subprocess_exec(
                            DOVI_TOOL, "inject-rpu", "-i", encoded_hevc, "--rpu-in", rpu_bin, "-o", injected_hevc,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0:
                            raise RuntimeError(f"RPU inject failed: {stderr_out.decode(errors='replace')[-200:]}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"RPU injected ({human_size(os.path.getsize(injected_hevc))})")
                        for f in [encoded_hevc, rpu_bin]:
                            if os.path.exists(f):
                                os.remove(f)
                        await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})

                        # Step 4: Mux with mkvmerge (video from injected HEVC + audio/subs from encoded file)
                        log.info(f"[{job.id}] {dv_label} step 4/4: muxing with mkvmerge")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 4/4: muxing with mkvmerge...")
                        mkvmerge_bin = _find_bin("mkvmerge") if os.path.isfile(_find_bin("mkvmerge")) else None
                        if mkvmerge_bin:
                            p = await asyncio.create_subprocess_exec(
                                mkvmerge_bin, "-o", remuxed_mkv,
                                injected_hevc, "-D", tmp_output,
                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                            stdout_out, stderr_out = await p.communicate()
                            if p.returncode > 1:
                                raise RuntimeError(f"mkvmerge failed (exit {p.returncode}): {stderr_out.decode(errors='replace')[-200:]}")
                        else:
                            log.warning(f"[{job.id}] mkvmerge not found, using ffmpeg for DV mux (may lose DV metadata)")
                            fps = "24000/1001"
                            try:
                                fp = await asyncio.create_subprocess_exec(
                                    FFPROBE, "-v", "quiet", "-select_streams", "v:0",
                                    "-show_entries", "stream=r_frame_rate",
                                    "-of", "csv=p=0", tmp_output,
                                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                                fps_out, _ = await fp.communicate()
                                fps_str = fps_out.decode().strip().rstrip(",")
                                if fps_str:
                                    fps = fps_str
                            except Exception:
                                pass
                            p = await asyncio.create_subprocess_exec(
                                FFMPEG, "-y", "-fflags", "+genpts",
                                "-f", "hevc", "-r", fps, "-i", injected_hevc,
                                "-i", tmp_output,
                                "-map", "0:v:0", "-map", "1:a?", "-map", "1:s?",
                                "-c", "copy",
                                "-color_primaries", "bt2020", "-color_trc", "smpte2084",
                                "-colorspace", "bt2020nc",
                                remuxed_mkv,
                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                            _, stderr_out = await p.communicate()
                            if p.returncode != 0:
                                raise RuntimeError(f"ffmpeg remux failed: {stderr_out.decode(errors='replace')[-200:]}")

                        if os.path.exists(injected_hevc):
                            os.remove(injected_hevc)

                        # Patch dvvC compatibility_id (mkvmerge always writes 1, we need 4 for P8.4)
                        patch_dvvc_compat_id(remuxed_mkv, 4)

                        os.remove(tmp_output)
                        shutil.move(remuxed_mkv, tmp_output)
                        new_bytes = os.path.getsize(tmp_output)
                        saved_pct = int((orig_bytes - new_bytes) * 100 / orig_bytes) if orig_bytes > 0 else 0
                        log.info(f"[{job.id}] {dv_label} complete — final size {human_size(new_bytes)}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"{dv_label} complete — {human_size(new_bytes)}")

                    except Exception as e:
                        log.error(f"[{job.id}] {dv_label} failed: {e}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"DV conversion failed: {e}")
                        for f in dovi_cleanup:
                            if os.path.exists(f):
                                os.remove(f)
                        if os.path.exists(tmp_output):
                            os.remove(tmp_output)
                        encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"{dv_label} failed: {e}")
                        _finish_job(JobStatus.FAILED, f"{dv_label} failed")
                        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                        continue
                    finally:
                        for f in dovi_cleanup:
                            if os.path.exists(f):
                                os.remove(f)

                # HDR10 → DV P8.4 upgrade (generate RPU from HDR10 metadata)
                needs_dv_upgrade = (
                    dv_mode == "encode_dv"
                    and not is_dv
                    and info.get("is_hdr", False)
                    and info.get("hdr_type", "") == "HDR10"
                )
                if needs_dv_upgrade:
                    dv_label = "HDR10→DV P8.4"
                    log.info(f"[{job.id}] {dv_label} — generating DV RPU from HDR10 metadata")
                    encode_queue.ffmpeg_logs.get(job.id, []).append(f"=== {dv_label} ===")
                    encode_queue.ffmpeg_logs.get(job.id, []).append("Probing encoded file for frame count...")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    tmp_dir = settings.get("tmp_dir", "/var/lib/plex/tmp")
                    gen_json = os.path.join(tmp_dir, f"{job.id}_dv_gen.json")
                    rpu_bin = os.path.join(tmp_dir, f"{job.id}_rpu.bin")
                    encoded_hevc = os.path.join(tmp_dir, f"{job.id}_encoded.hevc")
                    injected_hevc = os.path.join(tmp_dir, f"{job.id}_injected.hevc")
                    remuxed_mkv = os.path.join(tmp_dir, f"{job.id}_dv.mkv")
                    dovi_cleanup = [gen_json, rpu_bin, encoded_hevc, injected_hevc, remuxed_mkv]
                    try:
                        # Get frame count from encoded file (use nb_frames or estimate — never -count_frames which decodes entire file)
                        p = await asyncio.create_subprocess_exec(
                            FFPROBE, "-v", "error", "-select_streams", "v:0",
                            "-show_entries", "stream=nb_frames,r_frame_rate,duration",
                            "-of", "csv=p=0", tmp_output,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        probe_out, _ = await p.communicate()
                        probe_parts = probe_out.decode().strip().split(",")
                        frame_rate_str = probe_parts[0] if probe_parts else "24000/1001"
                        # nb_frames from container metadata (instant, no decoding)
                        frame_count = 0
                        if len(probe_parts) > 1 and probe_parts[1].strip().isdigit():
                            frame_count = int(probe_parts[1])
                        if frame_count == 0:
                            # Estimate from duration × frame rate
                            try:
                                frn, frd = frame_rate_str.split("/")
                                fps = float(frn) / float(frd)
                            except Exception:
                                fps = 23.976
                            dur = 0
                            if len(probe_parts) > 2:
                                try:
                                    dur = float(probe_parts[2])
                                except Exception:
                                    pass
                            if dur <= 0:
                                dur = info.get("duration_secs", 0)
                            frame_count = int(dur * fps) or 1000
                        log.info(f"[{job.id}] {dv_label}: {frame_count} frames, fps={frame_rate_str}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"Frame count: {frame_count}")

                        # Step 1: Build generate config from HDR10 metadata
                        hdr_meta = info.get("hdr10_metadata", {})
                        # dovi_tool level6 fields are all u16 integers
                        # min_display_mastering_luminance is in 0.0001 nits (so 0.005 nits = 50)
                        min_lum_nits = hdr_meta.get("min_lum", 0.005)
                        min_lum_u16 = int(round(min_lum_nits * 10000))
                        gen_config = {
                            "cm_version": "V40",
                            "length": frame_count,
                            "level6": {
                                "max_display_mastering_luminance": int(hdr_meta.get("max_lum", 1000)),
                                "min_display_mastering_luminance": min_lum_u16,
                                "max_content_light_level": int(hdr_meta.get("max_cll", 1000)),
                                "max_frame_average_light_level": int(hdr_meta.get("max_fall", 400)),
                            },
                        }
                        with open(gen_json, "w") as f:
                            json.dump(gen_config, f, indent=2)
                        log.info(f"[{job.id}] {dv_label} step 1/4: generating RPU (MaxCLL={gen_config['level6']['max_content_light_level']}, MaxFALL={gen_config['level6']['max_frame_average_light_level']})")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"DV step 1/4: generating RPU ({frame_count} frames, MaxCLL={gen_config['level6']['max_content_light_level']})...")
                        p = await asyncio.create_subprocess_exec(
                            DOVI_TOOL, "generate", "-j", gen_json, "-o", rpu_bin, "--profile", "8.4",
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0 or not os.path.exists(rpu_bin) or os.path.getsize(rpu_bin) == 0:
                            raise RuntimeError(f"RPU generation failed: {stderr_out.decode(errors='replace')[-200:]}")
                        rpu_size = os.path.getsize(rpu_bin)
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"RPU generated ({human_size(rpu_size)})")
                        await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})

                        # Step 2: Extract raw HEVC from encoded file
                        log.info(f"[{job.id}] {dv_label} step 2/4: extracting HEVC bitstream")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 2/4: extracting HEVC bitstream...")
                        p = await asyncio.create_subprocess_exec(
                            FFMPEG, "-y", "-i", tmp_output, "-c:v", "copy", "-an", "-sn",
                            "-bsf:v", "hevc_mp4toannexb", "-f", "hevc", encoded_hevc,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0:
                            raise RuntimeError(f"HEVC extract failed: {stderr_out.decode(errors='replace')[-200:]}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"HEVC extracted ({human_size(os.path.getsize(encoded_hevc))})")
                        await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})

                        # Step 3: Inject RPU into HEVC
                        log.info(f"[{job.id}] {dv_label} step 3/4: injecting RPU")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 3/4: injecting RPU...")
                        p = await asyncio.create_subprocess_exec(
                            DOVI_TOOL, "inject-rpu", "-i", encoded_hevc, "--rpu-in", rpu_bin, "-o", injected_hevc,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        _, stderr_out = await p.communicate()
                        if p.returncode != 0:
                            raise RuntimeError(f"RPU inject failed: {stderr_out.decode(errors='replace')[-200:]}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"RPU injected ({human_size(os.path.getsize(injected_hevc))})")
                        for f in [encoded_hevc, rpu_bin, gen_json]:
                            if os.path.exists(f):
                                os.remove(f)
                        await manager.broadcast({"type": "progress_update", "data": {"id": job.id, "progress": job.progress}})

                        # Step 4: Mux with mkvmerge
                        log.info(f"[{job.id}] {dv_label} step 4/4: muxing with mkvmerge")
                        encode_queue.ffmpeg_logs.get(job.id, []).append("DV step 4/4: muxing with mkvmerge...")
                        mkvmerge_bin = _find_bin("mkvmerge") if os.path.isfile(_find_bin("mkvmerge")) else None
                        if mkvmerge_bin:
                            p = await asyncio.create_subprocess_exec(
                                mkvmerge_bin, "-o", remuxed_mkv,
                                injected_hevc, "-D", tmp_output,
                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                            stdout_out, stderr_out = await p.communicate()
                            if p.returncode > 1:
                                raise RuntimeError(f"mkvmerge failed (exit {p.returncode}): {stderr_out.decode(errors='replace')[-200:]}")
                        else:
                            log.warning(f"[{job.id}] mkvmerge not found, using ffmpeg for DV mux (may lose DV metadata)")
                            fps = frame_rate_str
                            p = await asyncio.create_subprocess_exec(
                                FFMPEG, "-y", "-fflags", "+genpts",
                                "-f", "hevc", "-r", fps, "-i", injected_hevc,
                                "-i", tmp_output,
                                "-map", "0:v:0", "-map", "1:a?", "-map", "1:s?",
                                "-c", "copy",
                                "-color_primaries", "bt2020", "-color_trc", "smpte2084",
                                "-colorspace", "bt2020nc",
                                remuxed_mkv,
                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                            _, stderr_out = await p.communicate()
                            if p.returncode != 0:
                                raise RuntimeError(f"ffmpeg remux failed: {stderr_out.decode(errors='replace')[-200:]}")

                        if os.path.exists(injected_hevc):
                            os.remove(injected_hevc)

                        # Patch dvvC compatibility_id (mkvmerge always writes 1, we need 4 for P8.4)
                        patch_dvvc_compat_id(remuxed_mkv, 4)

                        os.remove(tmp_output)
                        shutil.move(remuxed_mkv, tmp_output)
                        new_bytes = os.path.getsize(tmp_output)
                        saved_pct = int((orig_bytes - new_bytes) * 100 / orig_bytes) if orig_bytes > 0 else 0
                        log.info(f"[{job.id}] {dv_label} complete — final size {human_size(new_bytes)}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"{dv_label} complete — {human_size(new_bytes)}")

                    except Exception as e:
                        log.error(f"[{job.id}] {dv_label} failed: {e}")
                        encode_queue.ffmpeg_logs.get(job.id, []).append(f"DV upgrade failed: {e}")
                        for f in dovi_cleanup:
                            if os.path.exists(f):
                                os.remove(f)
                        if os.path.exists(tmp_output):
                            os.remove(tmp_output)
                        encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"{dv_label} failed: {e}")
                        _finish_job(JobStatus.FAILED, f"{dv_label} failed")
                        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                        continue
                    finally:
                        for f in dovi_cleanup:
                            if os.path.exists(f):
                                os.remove(f)

                # Move to final location
                try:
                    if os.path.exists(output_file):
                        os.remove(output_file)
                    shutil.move(tmp_output, output_file)
                except Exception as e:
                    if os.path.exists(tmp_output):
                        os.remove(tmp_output)
                    log.error(f"[{job.id}] Failed to move output: {e}")
                    encode_queue.ffmpeg_logs.setdefault(job.id, []).append(f"Failed to move output: {e}")
                    _finish_job(JobStatus.FAILED, "Failed to move output")
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                    continue

                job.status = JobStatus.DONE
                action = "kept original"
                if settings.get("delete_original", False):
                    if app_settings.get("test_mode"):
                        log.info(f"[{job.id}] Test mode (5 min) — not deleting original")
                        encode_queue.ffmpeg_logs.setdefault(job.id, []).append("Test mode (5 min) — not deleting original")
                    else:
                        try:
                            os.remove(info["path"])
                            action = "deleted original"
                            log.info(f"[{job.id}] Deleted original: {info['path']}")
                        except Exception as e:
                            action = "failed to delete original"
                            log.error(f"[{job.id}] Failed to delete original {info['path']}: {e}")
                else:
                    log.info(f"[{job.id}] Kept original: {info['path']}")

                log.info(f"[{job.id}] Output saved: {output_file} (saved {saved_pct}%)")
                # Write manifest entry for this encode
                write_recode_manifest_entry(info["path"], output_file)
                job.result = {
                    "output_path": output_file,
                    "orig_size": human_size(orig_bytes),
                    "new_size": human_size(new_bytes),
                    "orig_bytes": orig_bytes,
                    "new_bytes": new_bytes,
                    "saved_pct": saved_pct,
                    "action": action,
                    "larger": new_bytes >= orig_bytes,
                }

        _finish_job()
        encode_queue._save_state(force=True)
        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})

        # Trigger Plex library rescan after encode completes
        if job.status == JobStatus.DONE and PLEX_TOKEN:
            section_key = get_library_section_for_path(info["path"])
            if section_key:
                trigger_plex_rescan(section_key)


# =============================================================================
# FastAPI App
# =============================================================================

app = FastAPI(title="Plex Re-Encoder")

# First-run detection — set to True if settings file doesn't exist OR has no setup_complete flag
FIRST_RUN = not app_settings.get("setup_complete", False)


def _get_cpu_name():
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return ""

def _get_os_name():
    try:
        with open("/etc/os-release") as f:
            pretty = ""
            for line in f:
                if line.startswith("PRETTY_NAME="):
                    pretty = line.strip().split("=", 1)[1].strip('"')
                    return pretty
    except Exception:
        pass
    return ""

@app.get("/api/setup/status")
async def setup_status():
    """Check if first-run setup is needed."""
    has_gpu = False
    gpu_info = []
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            has_gpu = True
            for line in result.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3:
                    gpu_info.append({"index": int(parts[0]), "name": parts[1], "vram_mb": int(parts[2])})
    except Exception:
        pass

    has_plex = False
    plex_token_found = PLEX_TOKEN is not None
    plex_prefs_path = ""
    for pp in [
        "/var/lib/plexmediaserver/Library/Application Support/Plex Media Server/Preferences.xml",
        "/var/lib/plex/Library/Application Support/Plex Media Server/Preferences.xml",
    ]:
        if os.path.exists(pp):
            has_plex = True
            plex_prefs_path = pp
            break

    ffmpeg_path = _find_bin("ffmpeg")
    ffmpeg_found = os.path.isfile(ffmpeg_path) if ffmpeg_path else False
    has_nvenc = False
    has_libplacebo = False
    if ffmpeg_found:
        try:
            r = subprocess.run([ffmpeg_path, "-hide_banner", "-encoders"], capture_output=True, text=True, timeout=5)
            has_nvenc = "hevc_nvenc" in r.stdout
            r2 = subprocess.run([ffmpeg_path, "-hide_banner", "-filters"], capture_output=True, text=True, timeout=5)
            has_libplacebo = "libplacebo" in r2.stdout
        except Exception:
            pass

    # Detect GPU hardware — try nvidia-smi name first, fall back to lspci
    lspci_gpu = ""
    has_nvidia_hw = False
    # If nvidia-smi works, use its GPU name (always correct)
    if has_gpu and gpu_info:
        has_nvidia_hw = True
        lspci_gpu = gpu_info[0].get("name", "")
    if not has_nvidia_hw:
        # Fall back to lspci with -nn for numeric IDs + try -v for better names
        try:
            r = subprocess.run(["lspci", "-nn"], capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                for line in r.stdout.splitlines():
                    if "NVIDIA" in line and ("VGA" in line or "3D" in line or "Display" in line):
                        lspci_gpu = line.split(": ", 1)[-1] if ": " in line else line
                        has_nvidia_hw = True
                        break
            # Try to get a better name with lspci -v if we only got a device ID
            if has_nvidia_hw and "Device" in lspci_gpu and "[" in lspci_gpu:
                try:
                    r2 = subprocess.run(["lspci", "-vmm"], capture_output=True, text=True, timeout=5)
                    if r2.returncode == 0:
                        in_nvidia = False
                        for line in r2.stdout.splitlines():
                            if line.startswith("Vendor:") and "NVIDIA" in line:
                                in_nvidia = True
                            elif line.startswith("Device:") and in_nvidia:
                                dev_name = line.split(":", 1)[-1].strip()
                                if dev_name and "Device" not in dev_name:
                                    lspci_gpu = f"NVIDIA {dev_name}"
                                break
                            elif line.strip() == "":
                                in_nvidia = False
                except Exception:
                    pass
            # Last resort: try to update PCI IDs database
            if "Device" in lspci_gpu:
                try:
                    subprocess.run(["update-pciids"], capture_output=True, timeout=30)
                    r3 = subprocess.run(["lspci"], capture_output=True, text=True, timeout=5)
                    if r3.returncode == 0:
                        for line in r3.stdout.splitlines():
                            if "NVIDIA" in line and ("VGA" in line or "3D" in line or "Display" in line):
                                new_name = line.split(": ", 1)[-1] if ": " in line else line
                                if "Device" not in new_name:
                                    lspci_gpu = new_name
                                break
                except Exception:
                    pass
        except Exception:
            pass

    # Get tool versions
    def _get_version(bin_name, args=None, parse=None):
        try:
            path = _find_bin(bin_name)
            if not path or not os.path.isfile(path):
                return ""
            cmd = [path] + (args or ["--version"])
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            out = (r.stdout + r.stderr).strip()
            if parse:
                return parse(out)
            return out.split("\n")[0][:100]
        except Exception:
            return ""

    ffmpeg_version = ""
    if ffmpeg_found:
        ffmpeg_version = _get_version("ffmpeg", ["-version"], lambda o: o.split("\n")[0].replace("ffmpeg version ", "").split(" ")[0] if o else "")
    dovi_version = _get_version("dovi_tool", ["--version"], lambda o: o.replace("dovi_tool ", "").split("\n")[0] if o else "")
    mkvmerge_version = _get_version("mkvmerge", ["--version"], lambda o: o.split("(")[0].replace("mkvmerge v", "v").strip() if o else "")
    mediainfo_version = _get_version("mediainfo", ["--Version"], lambda o: o.replace("MediaInfoLib - v", "v").split("\n")[-1].strip() if o else "")
    nvidia_driver_ver = ""
    if has_gpu:
        try:
            r = subprocess.run(["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"], capture_output=True, text=True, timeout=5)
            nvidia_driver_ver = r.stdout.strip().split("\n")[0] if r.returncode == 0 else ""
        except Exception:
            pass

    return {
        "first_run": FIRST_RUN,
        "version": VERSION,
        "has_gpu": has_gpu,
        "gpu_info": gpu_info,
        "gpu_count": len(gpu_info),
        "has_nvidia_hw": has_nvidia_hw,
        "lspci_gpu": lspci_gpu,
        "has_plex": has_plex,
        "plex_token_found": plex_token_found,
        "plex_prefs_path": plex_prefs_path,
        "ffmpeg_path": ffmpeg_path if ffmpeg_found else "",
        "has_nvenc": has_nvenc,
        "has_libplacebo": has_libplacebo,
        "vulkan_available": _has_libplacebo,  # actual functional test from startup
        "vulkan_version": _get_vulkan_version(),
        "has_dovi_tool": os.path.isfile(_find_bin("dovi_tool")),
        "has_mkvmerge": os.path.isfile(_find_bin("mkvmerge")),
        "hostname": HOSTNAME,
        "cpu_name": _get_cpu_name(),
        "cpu_cores": psutil.cpu_count(logical=True) or 1,
        "ram_gb": round(psutil.virtual_memory().total / (1024**3), 1),
        "rrp_version": _get_version("recode-remote", ["--version"], lambda o: o.replace("recode-remote ", "").split("\n")[0].strip() if o else ""),
        "os_name": _get_os_name(),
        "kernel": subprocess.run(["uname", "-r"], capture_output=True, text=True, timeout=5).stdout.strip() if shutil.which("uname") else "",
        "ffmpeg_version": ffmpeg_version,
        "dovi_version": dovi_version,
        "mkvmerge_version": mkvmerge_version,
        "mediainfo_version": mediainfo_version,
        "nvidia_driver_version": nvidia_driver_ver,
    }


@app.post("/api/setup/complete")
async def setup_complete(settings: dict):
    """Save initial settings and mark first-run as complete, then restart service."""
    global FIRST_RUN
    merged = APP_DEFAULTS.copy()
    merged.update(settings)
    merged["setup_complete"] = True
    save_settings(merged)
    app_settings.update(merged)
    FIRST_RUN = False
    # Restart the service, then remove sudoers on next startup
    merged["_remove_sudoers_on_start"] = True
    save_settings(merged)
    async def _delayed_restart():
        await asyncio.sleep(1)
        # Create flag file — systemd path watcher triggers restart
        flag = os.path.join(BASE_DIR, ".restart-flag")
        try:
            with open(flag, "w") as f:
                f.write("restart")
        except Exception:
            pass
        # If path watcher doesn't trigger within 5 seconds, force exit
        await asyncio.sleep(5)
        os._exit(0)
    asyncio.create_task(_delayed_restart())
    return {"ok": True}


# Background tool install tracking
_install_tasks: dict[str, dict] = {}  # tool_name -> {"status": "running"|"done"|"error"|"cancelled", "log": str}
_install_proc: asyncio.subprocess.Process = None  # current running install process
_SUDO = "/usr/bin/sudo" if os.path.exists("/usr/bin/sudo") else "/usr/local/bin/sudo" if os.path.exists("/usr/local/bin/sudo") else "sudo"
_INSTALL_ENV = {**os.environ, "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME": "/tmp", "DEBIAN_FRONTEND": "noninteractive"}


async def _run_sudo(*cmd):
    """Run a command with sudo, full PATH, and noninteractive. Stores proc for cancellation."""
    global _install_proc
    proc = await asyncio.create_subprocess_exec(
        _SUDO, "-n", *cmd,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        env=_INSTALL_ENV)
    _install_proc = proc
    return proc


@app.get("/api/setup/install-status")
async def install_status():
    """Check status of background tool installations."""
    return {"tasks": _install_tasks}


@app.post("/api/setup/cancel-install")
async def cancel_install():
    """Cancel a running tool installation."""
    global _install_proc
    running = [t for t, s in _install_tasks.items() if s.get("status") == "running"]
    if not running:
        return {"ok": False, "error": "No install running"}
    tool = running[0]
    # Kill the process
    if _install_proc and _install_proc.returncode is None:
        try:
            _install_proc.kill()
            await _install_proc.wait()
        except Exception:
            pass
    _install_proc = None
    _install_tasks[tool]["status"] = "cancelled"
    _install_tasks[tool]["log"] += "\n*** Cancelled by user ***\n"
    # Clean up build directory
    build_dir = "/tmp/ffmpeg-build"
    if os.path.isdir(build_dir):
        shutil.rmtree(build_dir, ignore_errors=True)
    return {"ok": True, "tool": tool}


@app.post("/api/setup/install-tool")
async def install_tool(tool: str):
    """Install a tool to the app bin directory. Only one install at a time."""
    global FFMPEG, FFPROBE, DOVI_TOOL

    # Block if any install is already running
    running = [t for t, s in _install_tasks.items() if s.get("status") == "running"]
    if running:
        return {"ok": False, "error": f"Another install is in progress: {running[0]}"}

    _install_tasks[tool] = {"status": "running", "log": "Starting...\n"}

    async def _do_install():
        try:
            if tool == "dovi_tool":
                _install_tasks[tool]["log"] += "Downloading dovi_tool from GitHub...\n"
                import platform
                arch = platform.machine()
                arch_map = {"x86_64": "x86_64-unknown-linux-musl", "aarch64": "aarch64-unknown-linux-musl"}
                dovi_arch = arch_map.get(arch)
                if not dovi_arch:
                    raise RuntimeError(f"Unsupported architecture: {arch}")
                # Get latest release URL
                proc = await asyncio.create_subprocess_exec(
                    "curl", "-sL", "https://api.github.com/repos/quietvoid/dovi_tool/releases/latest",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, _ = await proc.communicate()
                import re as _re
                urls = _re.findall(r'"browser_download_url":\s*"([^"]*' + dovi_arch + r'[^"]*)"', stdout.decode())
                urls = [u for u in urls if not u.endswith(".sha256")]
                if not urls:
                    raise RuntimeError("Could not find dovi_tool release for this architecture")
                url = urls[0]
                _install_tasks[tool]["log"] += f"Downloading {url}\n"
                tmp = f"/tmp/dovi_tool_dl_{os.getpid()}"
                os.makedirs(tmp, exist_ok=True)
                proc = await asyncio.create_subprocess_exec(
                    "curl", "-sL", url, "-o", f"{tmp}/dovi_tool.tar.gz",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                await proc.communicate()
                proc = await asyncio.create_subprocess_exec(
                    "tar", "-xzf", f"{tmp}/dovi_tool.tar.gz", "-C", tmp,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                await proc.communicate()
                # Find binary
                for root, dirs, files in os.walk(tmp):
                    if "dovi_tool" in files:
                        src = os.path.join(root, "dovi_tool")
                        dst = os.path.join(BIN_DIR, "dovi_tool")
                        shutil.move(src, dst)
                        os.chmod(dst, 0o755)
                        DOVI_TOOL = dst
                        _install_tasks[tool]["log"] += f"Installed to {dst}\n"
                        break
                else:
                    raise RuntimeError("Binary not found in archive")
                shutil.rmtree(tmp, ignore_errors=True)

            elif tool == "mkvmerge":
                _install_tasks[tool]["log"] += "Installing mkvtoolnix...\n"
                # Try package manager first (works on Debian/Ubuntu, some RHEL)
                pkg_cmd = None
                pkg_ok = False
                if shutil.which("apt-get"):
                    pkg_cmd = ["apt-get", "install", "-y", "mkvtoolnix"]
                elif shutil.which("pacman"):
                    pkg_cmd = ["pacman", "-S", "--noconfirm", "mkvtoolnix-cli"]
                elif shutil.which("dnf"):
                    _install_tasks[tool]["log"] += "Trying package manager...\n"
                    p = await _run_sudo("dnf", "install", "-y", "mkvtoolnix")
                    async for line in p.stdout:
                        text = line.decode(errors="replace").rstrip()
                        if text:
                            _install_tasks[tool]["log"] += text + "\n"
                    await p.wait()
                    if p.returncode == 0:
                        pkg_ok = True
                    else:
                        _install_tasks[tool]["log"] += "Not in repos — downloading from AppImage...\n"

                if pkg_cmd and not pkg_ok:
                    proc = await _run_sudo(*pkg_cmd)
                    async for line in proc.stdout:
                        text = line.decode(errors="replace").rstrip()
                        if text:
                            _install_tasks[tool]["log"] += text + "\n"
                    await proc.wait()
                    pkg_ok = proc.returncode == 0

                # Fallback: download from AppImage (works on all distros)
                if not pkg_ok:
                    _install_tasks[tool]["log"] += "Downloading MKVToolNix AppImage...\n"
                    appimage_url = "https://mkvtoolnix.download/appimage/MKVToolNix_GUI-97.0-x86_64.AppImage"
                    tmp = f"/tmp/mkvtoolnix_dl_{os.getpid()}"
                    os.makedirs(tmp, exist_ok=True)
                    proc = await asyncio.create_subprocess_exec(
                        "curl", "-sL", appimage_url, "-o", f"{tmp}/mkvtoolnix.AppImage",
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    await proc.communicate()
                    os.chmod(f"{tmp}/mkvtoolnix.AppImage", 0o755)
                    _install_tasks[tool]["log"] += "Extracting binaries...\n"
                    proc = await asyncio.create_subprocess_exec(
                        f"{tmp}/mkvtoolnix.AppImage", "--appimage-extract",
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                        cwd=tmp)
                    await proc.communicate()
                    sq = os.path.join(tmp, "squashfs-root", "usr", "bin", "mkvmerge")
                    if os.path.isfile(sq):
                        lib_dir = os.path.join(BASE_DIR, "lib", "mkvtoolnix")
                        os.makedirs(lib_dir, exist_ok=True)
                        for binary in ["mkvmerge", "mkvextract", "mkvpropedit"]:
                            src = os.path.join(tmp, "squashfs-root", "usr", "bin", binary)
                            if os.path.isfile(src):
                                shutil.copy2(src, os.path.join(lib_dir, binary))
                                os.chmod(os.path.join(lib_dir, binary), 0o755)
                        # Copy shared libs
                        sq_lib = os.path.join(tmp, "squashfs-root", "usr", "lib")
                        if os.path.isdir(sq_lib):
                            for f in os.listdir(sq_lib):
                                if f.endswith(".so") or ".so." in f:
                                    shutil.copy2(os.path.join(sq_lib, f), os.path.join(lib_dir, f))
                        # Create wrapper scripts
                        for binary in ["mkvmerge", "mkvextract", "mkvpropedit"]:
                            wrapper = os.path.join(BIN_DIR, binary)
                            with open(wrapper, "w") as wf:
                                wf.write(f'#!/bin/bash\nSCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"\n')
                                wf.write(f'LIB_DIR="${{SCRIPT_DIR}}/../lib/mkvtoolnix"\n')
                                wf.write(f'LD_LIBRARY_PATH="${{LIB_DIR}}:${{LD_LIBRARY_PATH}}" exec "${{LIB_DIR}}/{binary}" "$@"\n')
                            os.chmod(wrapper, 0o755)
                        _install_tasks[tool]["log"] += "mkvtoolnix installed from AppImage\n"
                        pkg_ok = True
                    else:
                        raise RuntimeError("Failed to extract mkvmerge from AppImage")
                    shutil.rmtree(tmp, ignore_errors=True)
                if not pkg_ok:
                    raise RuntimeError("mkvtoolnix install failed — try installing manually")
                # Symlink system-installed binaries to app bin dir (if not already there from AppImage)
                if not os.path.exists(os.path.join(BIN_DIR, "mkvmerge")):
                    sys_bin = shutil.which("mkvmerge")
                    if sys_bin:
                        for binary in ["mkvmerge", "mkvextract"]:
                            sb = shutil.which(binary)
                            if sb:
                                dst = os.path.join(BIN_DIR, binary)
                                if not os.path.exists(dst):
                                    os.symlink(sb, dst)
                        _install_tasks[tool]["log"] += f"Linked to {BIN_DIR}\n"

            elif tool == "mediainfo":
                _install_tasks[tool]["log"] += "Installing mediainfo via package manager...\n"
                # Enable EPEL on RHEL-based distros
                if shutil.which("dnf") or shutil.which("yum"):
                    p = await _run_sudo("dnf", "install", "-y", "epel-release")
                    async for line in p.stdout:
                        text = line.decode(errors="replace").rstrip()
                        if text:
                            _install_tasks[tool]["log"] += text + "\n"
                    await p.wait()
                pkg_cmd = None
                if shutil.which("apt-get"):
                    pkg_cmd = ["apt-get", "install", "-y", "mediainfo"]
                elif shutil.which("dnf"):
                    pkg_cmd = ["dnf", "install", "-y", "mediainfo"]
                elif shutil.which("pacman"):
                    pkg_cmd = ["pacman", "-S", "--noconfirm", "mediainfo"]
                if not pkg_cmd:
                    raise RuntimeError("No supported package manager found")
                proc = await _run_sudo(*pkg_cmd)
                async for line in proc.stdout:
                    text = line.decode(errors="replace").rstrip()
                    if text:
                        _install_tasks[tool]["log"] += text + "\n"
                await proc.wait()
                if proc.returncode != 0:
                    raise RuntimeError("Package install failed")
                sys_bin = shutil.which("mediainfo")
                if sys_bin:
                    dst = os.path.join(BIN_DIR, "mediainfo")
                    if not os.path.exists(dst):
                        os.symlink(sys_bin, dst)

            elif tool == "nvidia-drivers":
                _install_tasks[tool]["log"] += "Installing NVIDIA drivers...\n"
                _install_tasks[tool]["log"] += "WARNING: A reboot will be required after installation.\n\n"

                # Read full os-release
                distro_id = ""
                distro_id_like = ""
                distro_version = ""
                try:
                    with open("/etc/os-release") as f:
                        for line in f:
                            k, _, v = line.strip().partition("=")
                            v = v.strip('"')
                            if k == "ID": distro_id = v.lower()
                            elif k == "ID_LIKE": distro_id_like = v.lower()
                            elif k == "VERSION_ID": distro_version = v
                except Exception:
                    pass
                uname_r = subprocess.run(["uname", "-r"], capture_output=True, text=True, timeout=5).stdout.strip()
                _install_tasks[tool]["log"] += f"Distro: {distro_id} {distro_version}\n"
                _install_tasks[tool]["log"] += f"Kernel: {uname_r}\n"

                # Secure Boot warning
                try:
                    sb = subprocess.run(["mokutil", "--sb-state"], capture_output=True, text=True, timeout=5)
                    if "enabled" in sb.stdout.lower():
                        _install_tasks[tool]["log"] += "\n⚠ Secure Boot is ENABLED — driver module may not load.\n"
                        _install_tasks[tool]["log"] += "Run: mokutil --disable-validation (then reboot)\n\n"
                except Exception:
                    pass

                # Helper
                async def _drv_run(cmd_list, label):
                    _install_tasks[tool]["log"] += f"{label}...\n"
                    p = await _run_sudo(*cmd_list)
                    async for line in p.stdout:
                        text = line.decode(errors="replace").rstrip()
                        if text:
                            _install_tasks[tool]["log"] += text + "\n"
                    await p.wait()
                    return p.returncode

                # Determine EL major version for RHEL-based distros
                el_ver_num = 0
                try:
                    r = subprocess.run(["rpm", "-E", "%rhel"], capture_output=True, text=True, timeout=5)
                    if r.returncode == 0 and r.stdout.strip().isdigit():
                        el_ver_num = int(r.stdout.strip())
                except Exception:
                    pass

                pkg_cmd = None

                # ============================================================
                # UBUNTU / POP!_OS / LINUX MINT (ubuntu-drivers available)
                # ============================================================
                if shutil.which("ubuntu-drivers"):
                    _install_tasks[tool]["log"] += "Ubuntu-based — using ubuntu-drivers...\n"
                    await _drv_run(["apt-get", "install", "-y",
                        f"linux-headers-{uname_r}", "build-essential", "dkms"],
                        "Installing kernel headers")
                    proc = await _run_sudo("ubuntu-drivers", "list")
                    stdout, _ = await proc.communicate()
                    _install_tasks[tool]["log"] += f"Available: {stdout.decode(errors='replace').strip()}\n"
                    pkg_cmd = ["ubuntu-drivers", "autoinstall"]

                # ============================================================
                # DEBIAN (apt-get, no ubuntu-drivers)
                # ============================================================
                elif distro_id == "debian" and shutil.which("apt-get"):
                    _install_tasks[tool]["log"] += "Debian — enabling non-free repos...\n"
                    await _drv_run(["bash", "-c",
                        'if [ -f /etc/apt/sources.list.d/debian.sources ]; then '
                        "  sed -i 's/^Components:.*/Components: main contrib non-free non-free-firmware/' /etc/apt/sources.list.d/debian.sources; "
                        'elif [ -f /etc/apt/sources.list ]; then '
                        r"  sed -i '/^deb /{s/ main.*/ main contrib non-free non-free-firmware/}' /etc/apt/sources.list; "
                        r"  sed -i '/^deb-src /{s/ main.*/ main contrib non-free non-free-firmware/}' /etc/apt/sources.list; "
                        'fi && apt-get update -qq'
                    ], "Updating sources")
                    rc = await _drv_run(["apt-get", "install", "-y", f"linux-headers-{uname_r}"],
                        "Installing kernel headers")
                    if rc != 0:
                        _install_tasks[tool]["log"] += "Headers unavailable — installing latest kernel...\n"
                        await _drv_run(["apt-get", "install", "-y",
                            "linux-image-amd64", "linux-headers-amd64"],
                            "Installing latest kernel + headers")
                        _install_tasks[tool]["log"] += "⚠ Reboot twice: once for new kernel, once after driver build.\n"
                    await _drv_run(["apt-get", "install", "-y", "build-essential", "dkms", "gcc", "make"],
                        "Installing build tools")
                    pkg_cmd = ["apt-get", "install", "-y", "nvidia-driver", "firmware-misc-nonfree"]

                # ============================================================
                # FEDORA (RPM Fusion + akmod-nvidia)
                # ============================================================
                elif distro_id == "fedora":
                    _install_tasks[tool]["log"] += "Fedora — using RPM Fusion...\n"
                    await _drv_run(["dnf", "install", "-y",
                        "kernel-devel", "kernel-headers", "gcc", "make", "dkms",
                        "libglvnd-glx", "libglvnd-opengl", "libglvnd-devel"],
                        "Installing kernel headers and build tools")
                    fedora_ver = distro_version or ""
                    for repo in [
                        f"https://mirrors.rpmfusion.org/free/fedora/rpmfusion-free-release-{fedora_ver}.noarch.rpm",
                        f"https://mirrors.rpmfusion.org/nonfree/fedora/rpmfusion-nonfree-release-{fedora_ver}.noarch.rpm",
                    ]:
                        await _drv_run(["dnf", "install", "-y", repo], "Adding RPM Fusion")
                    pkg_cmd = ["dnf", "install", "-y", "akmod-nvidia", "xorg-x11-drv-nvidia-cuda"]

                # ============================================================
                # ALMA LINUX 10+ (nvidia-driver in distro repos)
                # ============================================================
                elif (distro_id in ("almalinux", "rocky")) and el_ver_num >= 10:
                    _install_tasks[tool]["log"] += f"{distro_id.title()} {el_ver_num} — adding NVIDIA repo...\n"
                    await _drv_run(["dnf", "install", "-y",
                        "kernel-devel", "kernel-headers", "gcc", "make", "dkms",
                        "libglvnd-devel", "elfutils-libelf-devel"],
                        "Installing kernel headers and build tools")
                    await _drv_run(["dnf", "config-manager", "--add-repo",
                        f"https://developer.download.nvidia.com/compute/cuda/repos/rhel{el_ver_num}/x86_64/cuda-rhel{el_ver_num}.repo"],
                        "Adding NVIDIA CUDA repo")
                    pkg_cmd = ["dnf", "install", "-y", "nvidia-driver", "nvidia-driver-cuda"]

                # ============================================================
                # RHEL / ALMA / ROCKY 8-9 (NVIDIA CUDA repo + module install)
                # ============================================================
                elif el_ver_num in (8, 9) and shutil.which("dnf"):
                    el_ver = f"rhel{el_ver_num}"
                    _install_tasks[tool]["log"] += f"RHEL {el_ver_num} — adding NVIDIA CUDA repo...\n"
                    await _drv_run(["dnf", "install", "-y",
                        "kernel-devel", "kernel-headers", "gcc", "make", "dkms",
                        "libglvnd-devel", "elfutils-libelf-devel"],
                        "Installing kernel headers and build tools")
                    rc = await _drv_run(["dnf", "install", "-y",
                        f"https://developer.download.nvidia.com/compute/cuda/repos/{el_ver}/x86_64/cuda-repo-{el_ver}-12-4-local-12.4.0_550.54.14-1.x86_64.rpm"],
                        "Adding NVIDIA local repo")
                    if rc != 0:
                        await _drv_run(["dnf", "config-manager", "--add-repo",
                            f"https://developer.download.nvidia.com/compute/cuda/repos/{el_ver}/x86_64/cuda-{el_ver}.repo"],
                            "Trying NVIDIA network repo")
                    pkg_cmd = ["dnf", "module", "install", "-y", "nvidia-driver:latest-dkms"]

                # ============================================================
                # OPENSUSE / SLES (zypper + NVIDIA repo)
                # ============================================================
                elif shutil.which("zypper"):
                    suse_ver = "tumbleweed" if distro_id == "opensuse-tumbleweed" else f"leap/{distro_version or '15.5'}"
                    _install_tasks[tool]["log"] += f"openSUSE/SLES ({suse_ver})...\n"
                    await _drv_run(["zypper", "install", "-y", "kernel-devel", "gcc", "make", "dkms"],
                        "Installing kernel headers and build tools")
                    await _drv_run(["zypper", "addrepo", "--refresh",
                        f"https://download.nvidia.com/opensuse/{suse_ver}", "NVIDIA"],
                        "Adding NVIDIA repo")
                    pkg_cmd = ["zypper", "install", "-y", "--auto-agree-with-licenses",
                               "nvidia-driver", "nvidia-driver-G06-kmp-default"]

                # ============================================================
                # ARCH / MANJARO / ENDEAVOUROS (pacman)
                # ============================================================
                elif shutil.which("pacman"):
                    _install_tasks[tool]["log"] += "Arch-based — using pacman...\n"
                    await _drv_run(["pacman", "-S", "--noconfirm", "--needed",
                        "linux-headers", "base-devel", "dkms"],
                        "Installing kernel headers and build tools")
                    pkg_cmd = ["pacman", "-S", "--noconfirm", "nvidia-dkms", "nvidia-utils", "nvidia-settings"]

                # ============================================================
                # GENERIC DNF FALLBACK (unknown RHEL-like)
                # ============================================================
                elif shutil.which("dnf"):
                    _install_tasks[tool]["log"] += f"Unknown dnf-based distro ({distro_id}) — trying NVIDIA repo...\n"
                    await _drv_run(["dnf", "install", "-y",
                        "kernel-devel", "kernel-headers", "gcc", "make", "dkms"],
                        "Installing kernel headers and build tools")
                    # Try distro package first, fall back to CUDA repo
                    rc = await _drv_run(["dnf", "install", "-y", "nvidia-driver"],
                        "Trying distro nvidia-driver package")
                    if rc == 0:
                        pkg_cmd = None  # already installed
                        _install_tasks[tool]["log"] += "nvidia-driver installed from distro repos.\n"
                    else:
                        el = el_ver_num or 9
                        await _drv_run(["dnf", "config-manager", "--add-repo",
                            f"https://developer.download.nvidia.com/compute/cuda/repos/rhel{el}/x86_64/cuda-rhel{el}.repo"],
                            "Adding NVIDIA CUDA repo")
                        pkg_cmd = ["dnf", "install", "-y", "nvidia-driver"]

                # ============================================================
                # GENERIC YUM FALLBACK
                # ============================================================
                elif shutil.which("yum"):
                    await _drv_run(["yum", "install", "-y",
                        "kernel-devel", "kernel-headers", "gcc", "make"],
                        "Installing kernel headers and build tools")
                    pkg_cmd = ["yum", "install", "-y", "nvidia-driver"]

                if pkg_cmd is None and not any(k in _install_tasks[tool]["log"] for k in ["installed from distro", "installed successfully"]):
                    raise RuntimeError(f"No supported driver install method for: {distro_id} {distro_version}")
                if pkg_cmd:
                    _install_tasks[tool]["log"] += f"Running: {' '.join(pkg_cmd)}\n"
                    proc = await _run_sudo(*pkg_cmd)
                    async for line in proc.stdout:
                        text = line.decode(errors="replace").rstrip()
                        if text:
                            _install_tasks[tool]["log"] += text + "\n"
                            if len(_install_tasks[tool]["log"]) > 5000:
                                _install_tasks[tool]["log"] = "...\n" + _install_tasks[tool]["log"][-4500:]
                    await proc.wait()
                    if proc.returncode != 0:
                        raise RuntimeError("Driver installation failed")
                _install_tasks[tool]["log"] += "\nNVIDIA drivers installed successfully.\n"
                _install_tasks[tool]["log"] += "*** A REBOOT IS REQUIRED for the drivers to take effect. ***\n"
                _install_tasks[tool]["log"] += "After reboot, refresh this page to verify GPU detection.\n"

            elif tool == "reboot":
                _install_tasks[tool]["log"] += "Rebooting server in 5 seconds...\n"
                await asyncio.sleep(5)
                proc = await _run_sudo("reboot")
                await proc.communicate()
                return  # Server going down

            elif tool == "vulkan":
                _install_tasks[tool]["log"] += "Installing Vulkan runtime...\n"
                # Detect package manager
                for pkg_mgr, pkg_name in [("dnf", "vulkan-loader"), ("yum", "vulkan-loader"), ("apt-get", "libvulkan1"), ("zypper", "vulkan-loader"), ("pacman", "vulkan-loader")]:
                    if shutil.which(pkg_mgr):
                        _install_tasks[tool]["log"] += f"Using {pkg_mgr} to install {pkg_name}...\n"
                        if pkg_mgr == "apt-get":
                            proc = await asyncio.create_subprocess_exec("apt-get", "update", "-qq", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                            await proc.communicate()
                        install_cmd = [pkg_mgr, "install", "-y", pkg_name]
                        if pkg_mgr == "pacman":
                            install_cmd = ["pacman", "-S", "--noconfirm", pkg_name]
                        proc = await asyncio.create_subprocess_exec(*install_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        stdout, stderr = await proc.communicate()
                        _install_tasks[tool]["log"] += stdout.decode() + stderr.decode()
                        if proc.returncode == 0:
                            _install_tasks[tool]["log"] += f"\n{pkg_name} installed successfully.\n"
                            _install_tasks[tool]["status"] = "done"
                        else:
                            _install_tasks[tool]["log"] += f"\nInstallation failed (exit {proc.returncode}).\n"
                            _install_tasks[tool]["status"] = "error"
                        return
                _install_tasks[tool]["log"] += "No supported package manager found.\n"
                _install_tasks[tool]["status"] = "error"
                return

            elif tool == "restart-service":
                _install_tasks[tool]["log"] += "Restarting Recode service...\n"
                # Create flag file — systemd path watcher triggers restart
                flag = os.path.join(BASE_DIR, ".restart-flag")
                with open(flag, "w") as f:
                    f.write("restart")
                _install_tasks[tool]["status"] = "done"
                _install_tasks[tool]["log"] += "Restart flag created — systemd will restart the service.\n"
                return

            elif tool == "ffmpeg-static":
                _install_tasks[tool]["log"] += "Installing static ffmpeg (CPU encoding)...\n"
                static_dir = os.path.join(BASE_DIR, "bin", "static")
                if os.path.isfile(os.path.join(static_dir, "ffmpeg")):
                    # Use bundled static binary
                    for binary in ["ffmpeg", "ffprobe"]:
                        src = os.path.join(static_dir, binary)
                        dst = os.path.join(BIN_DIR, binary)
                        if os.path.exists(dst) or os.path.islink(dst):
                            os.remove(dst)
                        os.symlink(src, dst)
                        _install_tasks[tool]["log"] += f"Linked {binary} → {dst}\n"
                else:
                    # Download if not bundled
                    _install_tasks[tool]["log"] += "Downloading static ffmpeg from johnvansickle.com...\n"
                    import tempfile
                    tmp = tempfile.mkdtemp(prefix="ffmpeg_static_")
                    url = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
                    proc = await asyncio.create_subprocess_exec(
                        "curl", "-sL", url, "-o", f"{tmp}/ffmpeg.tar.xz",
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
                    await proc.communicate()
                    _install_tasks[tool]["log"] += "Extracting...\n"
                    proc = await asyncio.create_subprocess_exec(
                        "tar", "-xf", f"{tmp}/ffmpeg.tar.xz", "-C", tmp,
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
                    await proc.communicate()
                    # Find extracted ffmpeg binary
                    extracted = None
                    for d in os.listdir(tmp):
                        p = os.path.join(tmp, d, "ffmpeg")
                        if os.path.isfile(p):
                            extracted = os.path.join(tmp, d)
                            break
                    if not extracted:
                        shutil.rmtree(tmp, ignore_errors=True)
                        raise RuntimeError("Failed to extract static ffmpeg")
                    os.makedirs(os.path.join(BIN_DIR, "static"), exist_ok=True)
                    for binary in ["ffmpeg", "ffprobe"]:
                        src = os.path.join(extracted, binary)
                        static_dst = os.path.join(BIN_DIR, "static", binary)
                        shutil.copy2(src, static_dst)
                        os.chmod(static_dst, 0o755)
                        dst = os.path.join(BIN_DIR, binary)
                        if os.path.exists(dst) or os.path.islink(dst):
                            os.remove(dst)
                        os.symlink(static_dst, dst)
                        _install_tasks[tool]["log"] += f"Installed {binary} → {dst}\n"
                    shutil.rmtree(tmp, ignore_errors=True)
                FFMPEG = _find_bin("ffmpeg")
                FFPROBE = _find_bin("ffprobe")
                _install_tasks[tool]["log"] += f"ffmpeg ready: {FFMPEG}\n"
                try:
                    r = subprocess.run([FFMPEG, "-version"], capture_output=True, text=True, timeout=5)
                    _install_tasks[tool]["log"] += r.stdout.split("\n")[0] + "\n"
                except Exception:
                    pass
                _install_tasks[tool]["log"] += "CPU encoding ready. Build ffmpeg with GPU support for hardware acceleration.\n"

            elif tool == "ffmpeg":
                _install_tasks[tool]["log"] += "Building ffmpeg from source (this takes 15-30 minutes)...\n"
                build_script = os.path.join(BASE_DIR, "build-ffmpeg.sh")
                if not os.path.isfile(build_script):
                    raise RuntimeError("build-ffmpeg.sh not found")
                bash_bin = "/usr/bin/bash" if os.path.exists("/usr/bin/bash") else "/bin/bash"
                proc = await _run_sudo(bash_bin, build_script)
                # Read output in chunks to avoid blocking on partial lines
                while True:
                    try:
                        chunk = await asyncio.wait_for(proc.stdout.read(4096), timeout=300)
                    except asyncio.TimeoutError:
                        # Build still running but no output for 5 min — check if process alive
                        if proc.returncode is not None:
                            break
                        _install_tasks[tool]["log"] += "[still building...]\n"
                        continue
                    if not chunk:
                        break
                    text = chunk.decode(errors="replace")
                    _install_tasks[tool]["log"] += text
                    # Keep last 10000 chars to avoid memory bloat
                    if len(_install_tasks[tool]["log"]) > 10000:
                        _install_tasks[tool]["log"] = "...\n" + _install_tasks[tool]["log"][-9000:]
                await proc.wait()
                if proc.returncode != 0:
                    raise RuntimeError("ffmpeg build failed")
                # Copy built binaries to app bin dir
                for binary in ["ffmpeg", "ffprobe"]:
                    sys_bin = f"/usr/local/bin/{binary}"
                    if os.path.isfile(sys_bin):
                        dst = os.path.join(BIN_DIR, binary)
                        try:
                            p = await _run_sudo("cp", "-f", sys_bin, dst)
                            await p.communicate()
                            p = await _run_sudo("chmod", "755", dst)
                            await p.communicate()
                        except Exception as e:
                            _install_tasks[tool]["log"] += f"Warning: failed to copy {binary}: {e}\n"
                        _install_tasks[tool]["log"] += f"Installed {binary} → {dst}\n"
                FFMPEG = _find_bin("ffmpeg")
                FFPROBE = _find_bin("ffprobe")
                _install_tasks[tool]["log"] += f"ffmpeg path: {FFMPEG}\n"
                # Verify features
                try:
                    r = subprocess.run([FFMPEG, "-hide_banner", "-encoders"], capture_output=True, text=True, timeout=5)
                    has_nv = "hevc_nvenc" in r.stdout
                    r2 = subprocess.run([FFMPEG, "-hide_banner", "-filters"], capture_output=True, text=True, timeout=5)
                    has_lp = "libplacebo" in r2.stdout
                    _install_tasks[tool]["log"] += f"NVENC: {'YES' if has_nv else 'NO'}\n"
                    _install_tasks[tool]["log"] += f"libplacebo: {'YES' if has_lp else 'NO'}\n"
                except Exception as e:
                    _install_tasks[tool]["log"] += f"Feature check failed: {e}\n"
            else:
                raise RuntimeError(f"Unknown tool: {tool}")

            _install_proc = None
            _install_tasks[tool]["status"] = "done"
            _install_tasks[tool]["log"] += "Done!\n"
        except Exception as e:
            _install_proc = None
            if _install_tasks[tool].get("status") == "cancelled":
                return  # Already handled by cancel endpoint
            _install_tasks[tool]["status"] = "error"
            _install_tasks[tool]["log"] += f"Error: {e}\n"

    asyncio.create_task(_do_install())
    return {"ok": True}


async def job_watchdog():
    """Background task that cleans up zombie active jobs every 10 seconds.
    A job is zombie if it's ENCODING (not paused), past the 30s startup grace period,
    and has no live ffmpeg process."""
    while True:
        await asyncio.sleep(10)
        now = time.time()
        stale = []
        for jid, j in list(encode_queue.active_jobs.items()):
            if j.paused:
                continue  # paused jobs are fine
            if j.status == JobStatus.ENCODING:
                # Remote listener jobs have no local proc — skip watchdog for them
                if j.settings.get("_remote_server_idx", -1) >= 0:
                    continue
                proc = encode_queue.ffmpeg_procs.get(jid)
                if proc is None:
                    # No proc yet — job is still setting up (GPU check, building cmd)
                    # Give it 120s before considering it stale
                    if j.started_at and (now - j.started_at) < 120:
                        continue
                    stale.append(jid)
                elif proc.returncode is not None:
                    # Proc finished but job still active — could be DV post-processing
                    # Check how long since the proc exited using the proc_ended_at tracker
                    ended_at = encode_queue._proc_ended_at.get(jid)
                    if ended_at is None:
                        # First time we see it's dead — record the time
                        encode_queue._proc_ended_at[jid] = now
                        continue
                    elif (now - ended_at) < 1800:
                        # Give DV pipeline up to 30 minutes after ffmpeg exits (large remuxes need more time)
                        continue
                    stale.append(jid)
                # else: proc is alive and running, that's fine
            elif j.status not in (JobStatus.ENCODING, JobStatus.QUEUED):
                stale.append(jid)
        for jid in stale:
            job = encode_queue.active_jobs.pop(jid)
            log.warning(f"[{jid}] Watchdog: cleaning up zombie job for {job.file_info.get('filename', '?')}")
            if not any(h["id"] == jid for h in encode_queue.history):
                encode_queue.history.append({
                    "id": jid, "file_info": job.file_info, "settings": job.settings,
                    "status": JobStatus.FAILED, "error": job.error or "Encode process died unexpectedly",
                    "started_at": job.started_at, "finished_at": job.finished_at or now,
                    "result": job.result or {}, "log": encode_queue.ffmpeg_logs.get(jid, [])[-100:],
                })
            encode_queue.ffmpeg_procs.pop(jid, None)
            encode_queue.ffmpeg_logs.pop(jid, None)
            encode_queue._proc_ended_at.pop(jid, None)
            encode_queue.job_gpus.pop(jid, None)
        if stale:
            encode_queue.running = len(encode_queue.active_jobs) > 0
            encode_queue._save_state()
            cleanup_tmp_dir()
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})


def cleanup_tmp_dir():
    """Remove orphan temp files not associated with any active encode."""
    tmp_dir = app_settings.get("tmp_dir", "/var/lib/plex/tmp")
    if not os.path.isdir(tmp_dir):
        return
    # Collect temp file paths that active jobs are writing to
    # Protect ALL active jobs (including post-processing like DV RPU injection)
    active_tmp_files = set()
    for jid, job in encode_queue.active_jobs.items():
        info = job.file_info
        settings = job.settings
        p = Path(info.get("path", ""))
        encode_tag = build_encode_tag(settings.get("video_codec", "hevc"), info, settings.get("dv_mode", "skip"), settings.get("resize", "original"))
        active_tmp_files.add(f"{p.stem}{encode_tag}.mkv")
        # Protect DV pipeline files
        active_tmp_files.add(f"{jid}_source.hevc")
        active_tmp_files.add(f"{jid}_encoded.hevc")
        active_tmp_files.add(f"{jid}_rpu.bin")
        active_tmp_files.add(f"{jid}_injected.hevc")
        active_tmp_files.add(f"{jid}_dv.mkv")
        active_tmp_files.add(f"{jid}_dv_temp.mp4")
        active_tmp_files.add(f"{jid}_dv_gen.json")
    removed = 0
    for fname in os.listdir(tmp_dir):
        if fname not in active_tmp_files:
            fpath = os.path.join(tmp_dir, fname)
            try:
                os.remove(fpath)
                removed += 1
            except OSError:
                pass
    if removed:
        log.info(f"Cleaned up {removed} orphan temp file(s) from {tmp_dir}")


# GPU server process management
_ffmpeg_server_proc: asyncio.subprocess.Process = None

async def start_ffmpeg_server():
    """Start the RRP GPU server if enabled in settings."""
    global _ffmpeg_server_proc
    await stop_ffmpeg_server()
    if not app_settings.get("ffmpeg_server_enabled"):
        return
    port = app_settings.get("ffmpeg_server_port", 9878)
    secret = app_settings.get("ffmpeg_server_secret", "")
    rrp_bin = os.path.join(BIN_DIR, "recode-remote")
    if os.path.isfile(rrp_bin):
        rrp_log = open(os.path.join(BASE_DIR, "rrp-server.log"), "a")
        rrp_tmp = os.path.join(app_settings["tmp_dir"], "rrp")
        os.makedirs(rrp_tmp, exist_ok=True)
        _ffmpeg_server_proc = await asyncio.create_subprocess_exec(
            rrp_bin, "server", "--port", str(port), "--secret", secret,
            "--ffmpeg", os.path.join(BIN_DIR, "ffmpeg"),
            "--tmp-dir", rrp_tmp,
            stdout=rrp_log,
            stderr=rrp_log,
        )
        log.info(f"RRP server started on port {port} (PID {_ffmpeg_server_proc.pid})")
    else:
        log.warning("recode-remote binary not found")

async def stop_ffmpeg_server():
    """Stop the GPU server if running."""
    global _ffmpeg_server_proc
    if _ffmpeg_server_proc and _ffmpeg_server_proc.returncode is None:
        try:
            _ffmpeg_server_proc.terminate()
            await asyncio.wait_for(_ffmpeg_server_proc.wait(), timeout=5)
        except Exception:
            try:
                _ffmpeg_server_proc.kill()
            except Exception:
                pass
        log.info("GPU server stopped")
    _ffmpeg_server_proc = None

_remote_client_proc = None

async def start_remote_client_listener():
    """Start the reverse-connect listener for incoming GPU servers."""
    global _remote_client_proc
    await stop_remote_client_listener()
    if not app_settings.get("remote_client_enabled"):
        return
    port = app_settings.get("remote_client_port", 9879)
    secret = app_settings.get("remote_client_secret", "")
    if not secret:
        log.warning("Remote client listener: no secret configured")
        return
    rrp_bin = os.path.join(BIN_DIR, "recode-remote")
    if os.path.isfile(rrp_bin):
        rrp_tmp = os.path.join(app_settings["tmp_dir"], "rrp")
        try:
            os.makedirs(rrp_tmp, exist_ok=True)
        except PermissionError:
            log.warning(f"Cannot create {rrp_tmp} — remote listener disabled")
            return
        status_file = os.path.join(rrp_tmp, "listener-status.json")
        try:
            with open(status_file, "w") as f:
                f.write('{"enabled":true,"running":false,"gpus":[]}')
        except PermissionError:
            pass
        rrp_log = open(os.path.join(BASE_DIR, "rrp-client-listener.log"), "a")
        listen_env = {k: v for k, v in os.environ.items() if k != "LD_LIBRARY_PATH"}
        _remote_client_proc = await asyncio.create_subprocess_exec(
            rrp_bin, "listen", "--port", str(port), "--secret", secret,
            "--status-file", status_file,
            stdout=rrp_log, stderr=rrp_log,
            env=listen_env,
        )
        log.info(f"Remote client listener started on port {port} (PID {_remote_client_proc.pid})")
    else:
        log.warning("recode-remote binary not found for listener")

async def stop_remote_client_listener():
    """Stop the reverse-connect listener."""
    global _remote_client_proc
    if _remote_client_proc and _remote_client_proc.returncode is None:
        try:
            _remote_client_proc.terminate()
            await asyncio.wait_for(_remote_client_proc.wait(), timeout=5)
        except Exception:
            try: _remote_client_proc.kill()
            except Exception: pass
        log.info("Remote client listener stopped")
    _remote_client_proc = None

# Persistent connect processes — one per configured Remote Client
_remote_connect_procs: dict = {}  # key: index -> subprocess
_prev_remote_cfg = None  # track previous config to avoid unnecessary restarts

async def start_remote_connectors():
    """Start recode-remote connect for each configured remote client."""
    global _prev_remote_cfg
    _prev_remote_cfg = app_settings.get("remote_gpu_servers", [])
    await stop_remote_connectors()
    rrp_bin = os.path.join(BIN_DIR, "recode-remote")
    if not os.path.isfile(rrp_bin):
        return
    servers = app_settings.get("remote_gpu_servers", [])
    if not servers:
        return
    ffmpeg_bin = os.path.join(BIN_DIR, "ffmpeg")
    rrp_tmp = os.path.join(app_settings["tmp_dir"], "rrp")
    try:
        os.makedirs(rrp_tmp, exist_ok=True)
    except PermissionError:
        log.warning(f"Cannot create {rrp_tmp} — remote connectors disabled")
        return
    hostname = os.uname().nodename
    for i, srv in enumerate(servers):
        if srv.get("enabled", True) is False:
            continue
        addr = (srv.get("address") or "").strip()
        secret = (srv.get("secret") or "").strip()
        name = srv.get("name") or hostname
        max_jobs = srv.get("max_jobs", 1)
        if not addr or not secret:
            continue
        # Append default port if not specified
        if ":" not in addr.rsplit(".", 1)[-1]:
            addr = f"{addr}:9879"
        status_file = os.path.join(rrp_tmp, f"connect-status-{i}.json")
        rrp_log = open(os.path.join(BASE_DIR, f"rrp-connect-{i}.log"), "a")
        # Clear LD_LIBRARY_PATH to avoid PyInstaller bundle overriding system NVIDIA libs
        connect_env = {k: v for k, v in os.environ.items() if k != "LD_LIBRARY_PATH"}
        proc = await asyncio.create_subprocess_exec(
            rrp_bin, "connect",
            "--address", addr,
            "--secret", secret,
            "--name", name,
            "--ffmpeg", ffmpeg_bin,
            "--tmp-dir", rrp_tmp,
            "--max-jobs", str(max_jobs),
            "--status-file", status_file,
            stdout=rrp_log, stderr=rrp_log,
            env=connect_env,
        )
        _remote_connect_procs[i] = proc
        log.info(f"Remote connector {i} started: {name} -> {addr} (PID {proc.pid})")

async def stop_remote_connectors():
    """Stop all remote connect processes."""
    global _remote_connect_procs
    for i, proc in list(_remote_connect_procs.items()):
        if proc and proc.returncode is None:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try: proc.kill()
                except Exception: pass
    _remote_connect_procs.clear()
    log.info("Remote connectors stopped")

@app.on_event("startup")
async def startup():
    # Run GPU capability scan in background thread (non-blocking)
    import concurrent.futures
    _gpu_scan_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    asyncio.get_event_loop().run_in_executor(_gpu_scan_executor, _run_startup_gpu_scan)

    # Note: staged .new binaries are swapped by systemd ExecStartPre, not here
    # (Swapping inside the running process corrupts PyInstaller binaries)

    # Remove sudoers file if flagged from setup completion
    if app_settings.pop("_remove_sudoers_on_start", False):
        sudoers_file = "/etc/sudoers.d/recode"
        try:
            if os.path.exists(sudoers_file):
                os.remove(sudoers_file)
                log.info("Removed sudoers file (post-setup cleanup)")
        except PermissionError:
            log.warning("Could not remove sudoers file — remove manually: sudo rm /etc/sudoers.d/recode")
        save_settings(app_settings)

    # Kill any orphan ffmpeg processes from previous server instance
    try:
        tmp_dir = app_settings.get("tmp_dir", "/tmp/recode")
        for pattern in [f"ffmpeg.*{tmp_dir}"]:
            result = await asyncio.create_subprocess_exec(
                "pgrep", "-f", pattern,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            stdout, _ = await result.communicate()
            if stdout.strip():
                pids = stdout.decode().strip().split('\n')
                my_pid = str(os.getpid())
                for pid in pids:
                    pid = pid.strip()
                    if pid and pid != my_pid:
                        try:
                            os.kill(int(pid), 9)
                            log.info(f"Killed orphan process PID {pid} ({pattern})")
                        except (ProcessLookupError, ValueError):
                            pass
    except Exception:
        pass
    # Clean up any jobs that were active when the service was last stopped
    encode_queue.cleanup_stale_active()
    # Remove orphan temp files from previous runs
    cleanup_tmp_dir()
    # Spawn 10 encode workers — concurrency is controlled by max_concurrent_encodes setting
    for i in range(10):
        encode_queue.worker_tasks.append(asyncio.create_task(_encode_worker_safe(i)))
    asyncio.create_task(stats_collector())
    asyncio.create_task(job_watchdog())
    global watch_task
    watch_task = asyncio.create_task(folder_watcher())
    # Clear cached online status for remote servers (will be refreshed by status check)
    for srv in app_settings.get("remote_gpu_servers", []):
        srv.pop("_online", None)
    # Start reverse-connect listener if enabled
    await start_remote_client_listener()
    # Start remote client connectors
    await start_remote_connectors()
    # Run initial remote server status check
    asyncio.create_task(remote_gpu_status())


@app.get("/")
async def index():
    if FIRST_RUN:
        return FileResponse(os.path.join(BASE_DIR, "static", "setup.html"))
    return FileResponse(os.path.join(BASE_DIR, "static", "index.html"))

@app.get("/setup")
async def setup_page():
    return FileResponse(os.path.join(BASE_DIR, "static", "setup.html"))


@app.get("/api/browse")
async def browse_directory(path: str = "/mnt"):
    """List directories for folder browser."""
    target = Path(path).resolve()
    if not is_path_allowed(str(target)):
        return JSONResponse({"error": f"Browsing is restricted to: {', '.join(app_settings.get('allowed_paths', ['/mnt']))}"}, status_code=403)
    if not target.exists():
        return JSONResponse({"error": "Path not found"}, status_code=400)
    if not target.is_dir():
        return JSONResponse({"error": "Not a directory"}, status_code=400)

    dirs = []
    files_count = 0
    try:
        for entry in sorted(target.iterdir()):
            if entry.name.startswith('.'):
                continue
            if entry.is_dir():
                # Count video files in this dir (non-recursive, for preview)
                vcount = 0
                try:
                    for child in entry.iterdir():
                        if child.is_file() and child.suffix.lower() in VIDEO_EXTENSIONS:
                            vcount += 1
                except PermissionError:
                    pass
                dirs.append({"name": entry.name, "path": str(entry), "video_count": vcount})
            elif entry.is_file() and entry.suffix.lower() in VIDEO_EXTENSIONS:
                files_count += 1
    except PermissionError:
        return JSONResponse({"error": "Permission denied"}, status_code=403)

    parent = str(target.parent) if str(target) != "/mnt" else None
    return {"path": str(target), "parent": parent, "dirs": dirs, "video_files_here": files_count}


@app.get("/api/presets")
async def get_presets():
    return {"presets": PRESETS, "auto_presets": AUTO_PRESETS}


@app.get("/api/plex/libraries")
async def get_plex_libraries():
    """Fetch Plex library sections with their folder paths."""
    if not PLEX_TOKEN:
        return {"libraries": [], "error": "Plex token not found"}
    try:
        r = http_requests.get(f"{PLEX_URL}/library/sections", headers=plex_headers(), timeout=10)
        r.raise_for_status()
        sections = r.json().get("MediaContainer", {}).get("Directory", [])
        libraries = []
        for section in sections:
            locations = [loc["path"] for loc in section.get("Location", [])]
            libraries.append({
                "key": section["key"],
                "title": section.get("title", "Unknown"),
                "type": section.get("type", ""),
                "locations": locations,
            })
        return {"libraries": libraries}
    except Exception as e:
        log.error(f"Failed to fetch Plex libraries: {e}")
        return {"libraries": [], "error": str(e)}


@app.get("/api/update/check")
async def update_check():
    """Check GitHub for a newer version."""
    try:
        r = http_requests.get(
            "https://api.github.com/repos/tarquin-code/plex-recencoder/releases/latest",
            timeout=10, headers={"Accept": "application/vnd.github.v3+json"})
        if r.status_code != 200:
            return {"update_available": False, "error": f"GitHub API returned {r.status_code}"}
        data = r.json()
        latest = data.get("tag_name", "").lstrip("v")
        current = VERSION
        # Simple version comparison
        def ver_tuple(v):
            try:
                return tuple(int(x) for x in v.split("."))
            except Exception:
                return (0,)
        is_newer = ver_tuple(latest) > ver_tuple(current)
        asset_url = ""
        asset_size = 0
        for asset in data.get("assets", []):
            if asset["name"].endswith(".tar.gz"):
                asset_url = asset["browser_download_url"]
                asset_size = asset["size"]
                break
        return {
            "update_available": is_newer,
            "current_version": current,
            "latest_version": latest,
            "release_name": data.get("name", ""),
            "release_notes": data.get("body", ""),
            "published_at": data.get("published_at", ""),
            "download_url": asset_url,
            "download_size": asset_size,
        }
    except Exception as e:
        return {"update_available": False, "error": str(e)}


@app.post("/api/update/apply")
async def update_apply():
    """Download and apply the latest update from GitHub."""
    # Check for update first
    check = await update_check()
    if not check.get("update_available"):
        return {"ok": False, "error": "No update available"}
    url = check.get("download_url")
    if not url:
        return {"ok": False, "error": "No download URL found"}

    _install_tasks["update"] = {"status": "running", "log": f"Updating to v{check['latest_version']}...\n"}

    async def _do_update():
        try:
            tmp_dir = f"/tmp/recode_update_{os.getpid()}"
            os.makedirs(tmp_dir, exist_ok=True)
            tarball = os.path.join(tmp_dir, "update.tar.gz")

            # Download
            _install_tasks["update"]["log"] += f"Downloading {url}...\n"
            proc = await asyncio.create_subprocess_exec(
                "curl", "-sL", url, "-o", tarball,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"Download failed: {stdout.decode(errors='replace')[-200:]}")
            _install_tasks["update"]["log"] += f"Downloaded ({os.path.getsize(tarball) // 1024 // 1024} MB)\n"

            # Extract
            _install_tasks["update"]["log"] += "Extracting...\n"
            proc = await asyncio.create_subprocess_exec(
                "tar", "-xzf", tarball, "-C", tmp_dir,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            await proc.communicate()

            # Find extracted directory
            extracted = None
            for d in os.listdir(tmp_dir):
                if os.path.isdir(os.path.join(tmp_dir, d)) and d != "__MACOSX":
                    extracted = os.path.join(tmp_dir, d)
                    break
            if not extracted or not (
                os.path.isfile(os.path.join(extracted, "recode_server.py")) or
                os.path.isfile(os.path.join(extracted, "bin", "recode"))
            ):
                raise RuntimeError("Invalid update package — no recode binary or recode_server.py found")

            # Backup current
            backup_dir = os.path.join(BASE_DIR, "backups", f"pre-update-{VERSION}")
            os.makedirs(backup_dir, exist_ok=True)
            for f in ["recode_server.py", "build-ffmpeg.sh", "requirements.txt", "README.md", "LICENSE"]:
                src = os.path.join(BASE_DIR, f)
                if os.path.exists(src):
                    shutil.copy2(src, backup_dir)
            # Backup compiled binary if it exists
            recode_bin = os.path.join(BASE_DIR, "bin", "recode")
            if os.path.isfile(recode_bin):
                bin_bak = os.path.join(backup_dir, "bin")
                os.makedirs(bin_bak, exist_ok=True)
                shutil.copy2(recode_bin, bin_bak)
            static_bak = os.path.join(backup_dir, "static")
            os.makedirs(static_bak, exist_ok=True)
            for f in os.listdir(os.path.join(BASE_DIR, "static")):
                shutil.copy2(os.path.join(BASE_DIR, "static", f), static_bak)
            _install_tasks["update"]["log"] += f"Backed up current version to {backup_dir}\n"

            # Apply update — use sudo cp to handle root-owned files
            _install_tasks["update"]["log"] += "Applying update...\n"
            apply_script = f"""
set -e
# Core files
for f in recode_server.py build-ffmpeg.sh requirements.txt README.md LICENSE install.sh; do
    [ -f "{extracted}/$f" ] && cp -f "{extracted}/$f" "{BASE_DIR}/$f"
done
# Static files
[ -d "{extracted}/static" ] && cp -f {extracted}/static/* {BASE_DIR}/static/ 2>/dev/null || true
# Bin files (may be root-owned)
if [ -d "{extracted}/bin" ]; then
    mkdir -p {BASE_DIR}/bin
    cp -af {extracted}/bin/* {BASE_DIR}/bin/
    find {BASE_DIR}/bin -type f -exec chmod 755 {{}} \\;
fi
# Lib files
[ -d "{extracted}/lib" ] && cp -af {extracted}/lib/* {BASE_DIR}/lib/ 2>/dev/null || true
# If compiled binary exists, update systemd service to use it
if [ -x "{BASE_DIR}/bin/recode" ]; then
    SERVICE_FILE="/etc/systemd/system/recode.service"
    if [ -f "$SERVICE_FILE" ] && grep -q "recode_server.py" "$SERVICE_FILE"; then
        sed -i "s|ExecStart=.*|ExecStart={BASE_DIR}/bin/recode|" "$SERVICE_FILE"
        systemctl daemon-reload
        echo "Updated systemd service to use compiled binary"
    fi
fi
# Fix ownership
chown -R {os.getuid()}:{os.getgid()} {BASE_DIR}/ 2>/dev/null || true
echo "Files copied successfully"
"""
            proc = await _run_sudo("bash", "-c", apply_script)
            async for line in proc.stdout:
                text = line.decode(errors="replace").rstrip()
                if text:
                    _install_tasks["update"]["log"] += text + "\n"
            await proc.wait()
            if proc.returncode != 0:
                raise RuntimeError("Failed to copy update files")

            # Cleanup
            shutil.rmtree(tmp_dir, ignore_errors=True)

            _install_tasks["update"]["log"] += f"Update applied! Restart the service to activate the new version.\n"
            _install_tasks["update"]["status"] = "done"
        except Exception as e:
            _install_tasks["update"]["status"] = "error"
            _install_tasks["update"]["log"] += f"Update failed: {e}\n"

    asyncio.create_task(_do_update())
    return {"ok": True}


@app.get("/api/system/check")
async def system_check():
    results = {}

    for tool in ("ffmpeg", "ffprobe", "mediainfo"):
        found = _find_bin(tool)
        results[tool] = os.path.isfile(found) if found else False
    results["ffmpeg_path"] = FFMPEG
    try:
        _lp = subprocess.run([FFMPEG, "-hide_banner", "-filters"], capture_output=True, text=True, timeout=5)
        results["ffmpeg_libplacebo"] = "libplacebo" in _lp.stdout
    except Exception:
        results["ffmpeg_libplacebo"] = False
    # Vulkan check
    results["vulkan_available"] = _has_libplacebo
    results["vulkan_version"] = _get_vulkan_version()

    # Check GPU
    try:
        proc = await asyncio.create_subprocess_exec(
            "nvidia-smi", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        results["gpu"] = proc.returncode == 0
        results["gpu_count"] = GPU_COUNT
        results["version"] = VERSION
        results["max_gpu_encodes"] = sum(encode_queue.gpu_max_encodes(i) for i in range(GPU_COUNT)) if GPU_COUNT > 0 else 1
        if results["gpu"]:
            lines = stdout.decode().strip().split("\n")
            for line in lines:
                if "NVIDIA" in line and ("RTX" in line or "GTX" in line or "Tesla" in line or "Quadro" in line or "A100" in line or "A10" in line):
                    results["gpu_name"] = line.strip()
                    break
    except Exception:
        results["gpu"] = False

    # GPU info for settings UI
    results["gpu_info"] = [{"index": g, "name": per_gpu_info.get(g, {}).get("name", f"GPU {g}"), "vram_mb": per_gpu_info.get(g, {}).get("mem_total", 0), "max_jobs": encode_queue.gpu_max_encodes(g), "capabilities": _gpu_capabilities.get(g, {})} for g in range(GPU_COUNT)]

    # Check hevc_nvenc and h264_nvenc
    try:
        proc = await asyncio.create_subprocess_exec(
            FFMPEG, "-hide_banner", "-encoders",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        enc_list = stdout.decode()
        results["hevc_nvenc"] = "hevc_nvenc" in enc_list
        results["h264_nvenc"] = "h264_nvenc" in enc_list
    except Exception:
        results["hevc_nvenc"] = False
        results["h264_nvenc"] = False

    results["cpu_count"] = os.cpu_count() or 1

    # Check if staged .new files exist (waiting for restart to swap)
    restart_needed = False
    # Check binaries
    for binary in ("recode-remote", "recode"):
        staged = os.path.join(BIN_DIR, f"{binary}.new")
        if os.path.isfile(staged):
            restart_needed = True
            break
    # Check server code and static files
    if not restart_needed:
        for path in (os.path.join(BASE_DIR, "recode_server.py.new"),
                     os.path.join(BASE_DIR, "static", "index.html.new")):
            if os.path.isfile(path):
                restart_needed = True
                break
    results["restart_needed"] = restart_needed

    return results


@app.post("/api/scan")
async def scan_directory(req: ScanRequest):
    path = req.path
    if not is_path_allowed(path):
        return JSONResponse({"error": f"Scanning is restricted to: {', '.join(app_settings.get('allowed_paths', ['/mnt']))}"}, status_code=403)
    if not os.path.isdir(path):
        return JSONResponse({"error": f"Directory not found: {path}"}, status_code=400)

    # Reset cancel flag
    scan_cancel_event.clear()

    # Open cache DB
    try:
        cache_conn = get_cache_db()
    except Exception:
        cache_conn = None

    # Load existing cache for this directory tree
    cached_rows = {}
    if cache_conn:
        try:
            for row in cache_conn.execute("SELECT * FROM file_cache WHERE path LIKE ?",
                                          (path.replace("%", "%%") + "%",)):
                cached_rows[row["path"]] = row
        except Exception:
            cached_rows = {}

    # Collect all video files
    video_files = []
    for root, _, files in os.walk(path):
        if scan_cancel_event.is_set():
            break
        for f in sorted(files):
            ext = os.path.splitext(f)[1].lower()
            if ext in VIDEO_EXTENSIONS:
                video_files.append(os.path.join(root, f))

    video_files.sort()
    current_paths = set(video_files)
    total = len(video_files)

    # Classify files: cached (unchanged) vs. needs probing
    files_from_cache = []
    files_to_probe = []
    for filepath in video_files:
        cached = cached_rows.get(filepath)
        if cached:
            try:
                st = os.stat(filepath)
                if st.st_mtime == cached["mtime"] and st.st_size == cached["size_bytes"]:
                    files_from_cache.append(filepath)
                    continue
            except OSError:
                pass
        files_to_probe.append(filepath)

    await manager.broadcast({
        "type": "scan_progress",
        "data": {"total": total, "scanned": 0, "status": "scanning",
                 "cached": len(files_from_cache), "probing": len(files_to_probe)}
    })

    results = []
    scanned_count = 0
    sem = asyncio.Semaphore(16)

    def info_to_dict(info):
        suggestion = compute_suggestion(info)
        # Save to cache
        if cache_conn:
            save_to_cache(cache_conn, info, suggestion)
        return {
            "path": info.path, "filename": info.filename, "dirname": info.dirname,
            "size_bytes": info.size_bytes, "size_human": info.size_human,
            "codec": info.codec, "width": info.width, "height": info.height,
            "resolution_label": info.resolution_label, "pix_fmt": info.pix_fmt,
            "hdr_type": info.hdr_type, "is_hdr": info.is_hdr,
            "color_transfer": info.color_transfer, "color_primaries": info.color_primaries,
            "duration_secs": info.duration_secs, "audio_streams": info.audio_streams,
            "sub_streams": info.sub_streams,
            "is_hevc": info.is_hevc, "has_dovi": info.has_dovi, "dovi_profile": info.dovi_profile, "hdr10_metadata": info.hdr10_metadata,
            "output_exists": info.output_exists,
            "suggestion": suggestion,
        }

    # Batch broadcast helper
    broadcast_batch = []
    BATCH_SIZE = 10

    async def flush_batch():
        nonlocal broadcast_batch
        if broadcast_batch:
            await manager.broadcast({
                "type": "scan_results_batch",
                "data": {"files": broadcast_batch, "total": total, "scanned": scanned_count}
            })
            broadcast_batch = []

    # Phase 1: Send cached results immediately (very fast)
    for filepath in files_from_cache:
        if scan_cancel_event.is_set():
            break
        cached = cached_rows.get(filepath)
        if cached:
            d = cache_row_to_dict(cached)
            results.append(d)
            broadcast_batch.append(d)
            scanned_count += 1
            if len(broadcast_batch) >= BATCH_SIZE:
                await flush_batch()
                await manager.broadcast({
                    "type": "scan_progress",
                    "data": {"total": total, "scanned": scanned_count, "status": "scanning",
                             "current_file": "(cached)"}
                })

    await flush_batch()
    if files_from_cache:
        await manager.broadcast({
            "type": "scan_progress",
            "data": {"total": total, "scanned": scanned_count, "status": "scanning",
                     "current_file": f"{len(files_from_cache)} cached, probing {len(files_to_probe)} new/changed"}
        })

    # Phase 2: Probe new/changed files
    async def probe_one(filepath, idx):
        nonlocal scanned_count
        if scan_cancel_event.is_set():
            return
        async with sem:
            if scan_cancel_event.is_set():
                return
            info = await get_file_info(filepath)
            if info:
                d = info_to_dict(info)
                results.append(d)
                broadcast_batch.append(d)
            scanned_count += 1
            if len(broadcast_batch) >= BATCH_SIZE or scanned_count == total:
                await flush_batch()
                await manager.broadcast({
                    "type": "scan_progress",
                    "data": {"total": total, "scanned": scanned_count, "status": "scanning",
                             "current_file": os.path.basename(filepath)}
                })

    if files_to_probe:
        tasks = [probe_one(f, i) for i, f in enumerate(files_to_probe)]
        await asyncio.gather(*tasks)
        await flush_batch()

    # Clean up stale cache entries (files that no longer exist)
    if cache_conn:
        try:
            stale = set(cached_rows.keys()) - current_paths
            if stale:
                cache_conn.executemany("DELETE FROM file_cache WHERE path = ?",
                                       [(p,) for p in stale])
                cache_conn.commit()
            cache_conn.close()
        except Exception:
            pass

    cancelled = scan_cancel_event.is_set()
    results.sort(key=lambda x: x["path"])

    status = "cancelled" if cancelled else "done"
    await manager.broadcast({
        "type": "scan_progress",
        "data": {"total": total, "scanned": scanned_count, "status": status,
                 "cached": len(files_from_cache), "probing": len(files_to_probe)}
    })

    return {"files": results, "total": len(results), "cancelled": cancelled}


@app.post("/api/scan/cancel")
async def cancel_scan():
    scan_cancel_event.set()
    return {"ok": True}


@app.post("/api/scan/clear-cache")
async def clear_scan_cache():
    """Clear the scan cache table to force full re-probing."""
    try:
        conn = get_cache_db()
        try:
            conn.execute("DELETE FROM file_cache")
            conn.commit()
        finally:
            conn.close()
        return {"ok": True, "message": "Scan cache cleared"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/estimate")
async def estimate_size(req: EstimateRequest):
    """Instantly estimate output file size based on codec, bitrate and resolution."""
    path = req.path
    if not os.path.isfile(path):
        return JSONResponse({"error": "File not found"}, status_code=400)

    info = await get_file_info(path)
    if not info:
        return JSONResponse({"error": "Could not probe file"}, status_code=400)

    duration = info.duration_secs
    if duration <= 0:
        return JSONResponse({"error": "Could not determine file duration"}, status_code=400)

    orig_bytes = info.size_bytes
    # Calculate audio size from stream bitrates
    audio_size = sum(a.get("size_bytes", 0) for a in info.audio_streams)
    video_size = max(orig_bytes - audio_size, int(orig_bytes * 0.8))
    video_bitrate_bps = (video_size * 8) / duration

    pixels = info.width * info.height
    bpp = (video_size * 8) / duration / pixels if pixels > 0 else 0

    # Resolve preset to get target CQ
    preset_name = req.preset
    if preset_name in ("auto",) or preset_name in PRESETS:
        resolved = resolve_preset(preset_name, info.width, info.height)
        cq = resolved["cq"]
        maxbitrate_str = resolved["maxbitrate"]
    else:
        cq = req.cq
        maxbitrate_str = req.maxbitrate

    # Parse maxbitrate (e.g. "20M" -> 20_000_000)
    maxbr = maxbitrate_str.upper().replace("M", "000000").replace("K", "000")
    try:
        max_bitrate_bps = int(maxbr) * 8 if "M" not in maxbitrate_str.upper() and "K" not in maxbitrate_str.upper() else int(float(maxbitrate_str.upper().replace("M","").replace("K","")) * (1_000_000 if "M" in maxbitrate_str.upper() else 1_000))
    except (ValueError, TypeError):
        max_bitrate_bps = 20_000_000

    # Codec efficiency factor (how much more efficient H.265 is vs source codec)
    codec_factor = {
        "mpeg2video": 3.5, "mpeg4": 2.8, "vc1": 2.5, "msmpeg4v3": 3.0,
        "wmv3": 2.5, "vp8": 2.2, "h264": 1.6, "vp9": 1.1, "hevc": 1.0,
        "av1": 0.85,
    }.get(info.codec, 1.5)

    # CQ adjustment: lower CQ = higher quality = larger file
    # CQ 20 is reference, each +1 CQ is roughly -6% size, each -1 is +6%
    cq_ref = 22
    cq_multiplier = 1.0 * (0.94 ** (cq - cq_ref))

    # Reference H.265 bpp for good quality at this CQ
    h265_ref_bpp = 0.07 * cq_multiplier
    if info.is_hdr:
        h265_ref_bpp *= 1.25  # HDR needs ~25% more bits

    # Estimate the new video bitrate
    # If current bpp / codec_factor is already low, H.265 won't compress much further
    current_h265_equivalent_bpp = bpp / codec_factor
    estimated_bpp = max(h265_ref_bpp, current_h265_equivalent_bpp * 0.85)
    # Don't exceed current bpp (re-encode shouldn't make it bigger typically)
    estimated_bpp = min(estimated_bpp, bpp)

    estimated_video_bps = estimated_bpp * pixels
    # Also cap at maxbitrate
    estimated_video_bps = min(estimated_video_bps, max_bitrate_bps)

    estimated_video_bytes = int(estimated_video_bps * duration / 8)
    # Audio stays roughly same size (copied or re-encoded at similar bitrate)
    estimated_total = estimated_video_bytes + audio_size

    orig_video_bps = video_bitrate_bps
    new_video_bps = estimated_video_bps

    saved_pct = int((orig_bytes - estimated_total) * 100 / orig_bytes) if orig_bytes > 0 else 0

    return {
        "path": path,
        "filename": info.filename,
        "original_size": human_size(orig_bytes),
        "original_bytes": orig_bytes,
        "estimated_size": human_size(estimated_total),
        "estimated_bytes": estimated_total,
        "saved_pct": saved_pct,
        "original_bitrate_mbps": round(orig_video_bps / 1_000_000, 2),
        "estimated_bitrate_mbps": round(new_video_bps / 1_000_000, 2),
        "will_be_larger": estimated_total >= orig_bytes,
    }


class BatchEstimateRequest(BaseModel):
    files: list[dict]  # list of file_info dicts from scan results
    preset: str = "auto"
    cq: int = 24
    maxbitrate: str = "20M"
    speed: str = "p5"
    use_cpu: bool = False
    dv_mode: str = "skip"


def _estimate_from_info(info: dict, preset: str, cq: int, maxbitrate_str: str) -> dict:
    """Pure math estimate from file info dict — no ffprobe needed."""
    duration = info.get("duration_secs", 0)
    if duration <= 0:
        return {"path": info.get("path", ""), "error": "No duration"}
    orig_bytes = info.get("size_bytes", 0)
    audio_size = sum(a.get("size_bytes", 0) for a in info.get("audio_streams", []))
    video_size = max(orig_bytes - audio_size, int(orig_bytes * 0.8))
    pixels = info.get("width", 0) * info.get("height", 0)
    bpp = (video_size * 8) / duration / pixels if pixels > 0 else 0

    if preset in ("auto",) or preset in PRESETS:
        resolved = resolve_preset(preset, info.get("width", 0), info.get("height", 0))
        cq = resolved["cq"]
        maxbitrate_str = resolved["maxbitrate"]

    try:
        max_bitrate_bps = int(float(maxbitrate_str.upper().replace("M", "").replace("K", "")) * (1_000_000 if "M" in maxbitrate_str.upper() else 1_000))
    except (ValueError, TypeError):
        max_bitrate_bps = 20_000_000

    codec_factor = {
        "mpeg2video": 3.5, "mpeg4": 2.8, "vc1": 2.5, "msmpeg4v3": 3.0,
        "wmv3": 2.5, "vp8": 2.2, "h264": 1.6, "vp9": 1.1, "hevc": 1.0,
        "av1": 0.85,
    }.get(info.get("codec", ""), 1.5)

    cq_ref = 22
    cq_multiplier = 1.0 * (0.94 ** (cq - cq_ref))
    h265_ref_bpp = 0.07 * cq_multiplier
    if info.get("is_hdr", False):
        h265_ref_bpp *= 1.25

    current_h265_equivalent_bpp = bpp / codec_factor
    estimated_bpp = max(h265_ref_bpp, current_h265_equivalent_bpp * 0.85)
    estimated_bpp = min(estimated_bpp, bpp)
    estimated_video_bps = estimated_bpp * pixels
    estimated_video_bps = min(estimated_video_bps, max_bitrate_bps)
    estimated_video_bytes = int(estimated_video_bps * duration / 8)
    estimated_total = estimated_video_bytes + audio_size
    saved_pct = int((orig_bytes - estimated_total) * 100 / orig_bytes) if orig_bytes > 0 else 0

    return {
        "path": info.get("path", ""),
        "filename": info.get("filename", ""),
        "original_size": human_size(orig_bytes),
        "original_bytes": orig_bytes,
        "estimated_size": human_size(estimated_total),
        "estimated_bytes": estimated_total,
        "saved_pct": saved_pct,
        "original_bitrate_mbps": round((video_size * 8) / duration / 1_000_000, 2),
        "estimated_bitrate_mbps": round(estimated_video_bps / 1_000_000, 2),
        "will_be_larger": estimated_total >= orig_bytes,
    }


@app.post("/api/estimate/batch")
async def estimate_batch(req: BatchEstimateRequest):
    """Batch estimate for multiple files using pre-scanned file info — no ffprobe."""
    results = {}
    for info in req.files:
        path = info.get("path", "")
        results[path] = _estimate_from_info(info, req.preset, req.cq, req.maxbitrate)
    return {"results": results}


@app.get("/api/check-permissions")
async def check_permissions(path: str = ""):
    """Check write permissions for tmp dir and output directory."""
    tmp_dir = app_settings.get("tmp_dir", "/tmp/recode")
    issues = []
    for check_dir, label in [(tmp_dir, "Temp directory"), (path, "Output directory")]:
        if not check_dir:
            continue
        try:
            os.makedirs(check_dir, exist_ok=True)
            test_file = os.path.join(check_dir, ".recode_perm_test")
            with open(test_file, "w") as tf:
                tf.write("test")
            os.remove(test_file)
        except PermissionError:
            issues.append({"dir": check_dir, "label": label, "error": "Permission denied"})
        except Exception as e:
            issues.append({"dir": check_dir, "label": label, "error": str(e)})
    return {"ok": len(issues) == 0, "issues": issues}


@app.post("/api/queue/add")
async def queue_add(req: QueueAddRequest):
    base_settings = {
        "preset": req.preset, "cq": req.cq, "maxbitrate": req.maxbitrate,
        "speed": req.speed, "encoder": req.encoder, "use_cpu": req.use_cpu, "gpu_id": req.gpu_id, "gpu_target": req.gpu_target, "video_codec": req.video_codec, "dv_mode": req.dv_mode, "resize": req.resize,
        "skip_4k": req.skip_4k, "hdr_only": req.hdr_only,
        "delete_original": req.delete_original, "discard_larger": req.discard_larger,
        "english_only": req.english_only, "audio_codec": req.audio_codec,
        "audio_bitrate": req.audio_bitrate, "tmp_dir": req.tmp_dir,
    }

    # Convert audio_config Pydantic models to dicts
    audio_cfg_dict = {}
    for fpath, streams in req.audio_config.items():
        audio_cfg_dict[fpath] = [s.model_dump() for s in streams]

    # Subtitle config (simple dicts, no Pydantic)
    sub_cfg_dict = dict(req.subtitle_config)

    # Use pre-built file info from scan results when available, fall back to probing
    added = []
    skipped_dupes = 0
    seen_paths = set()  # Deduplicate within the same batch
    for fpath in req.files:
        if fpath in seen_paths:
            skipped_dupes += 1
            continue
        seen_paths.add(fpath)
        if fpath in req.file_info:
            info_dict = req.file_info[fpath]
        else:
            info = await get_file_info(fpath)
            if not info:
                continue
            info_dict = {
                "path": info.path, "filename": info.filename, "dirname": info.dirname,
                "size_bytes": info.size_bytes, "size_human": info.size_human,
                "codec": info.codec, "width": info.width, "height": info.height,
                "resolution_label": info.resolution_label, "pix_fmt": info.pix_fmt,
                "hdr_type": info.hdr_type, "is_hdr": info.is_hdr,
                "color_transfer": info.color_transfer, "color_primaries": info.color_primaries,
                "duration_secs": info.duration_secs, "audio_streams": info.audio_streams,
                "sub_streams": info.sub_streams,
                "is_hevc": info.is_hevc, "has_dovi": info.has_dovi, "dovi_profile": info.dovi_profile, "hdr10_metadata": info.hdr10_metadata,
                "output_exists": info.output_exists,
                "suggestion": compute_suggestion(info),
            }
        # Attach per-file audio and subtitle config if provided
        file_settings = base_settings.copy()
        if fpath in audio_cfg_dict:
            file_settings["audio_config"] = audio_cfg_dict[fpath]
        if fpath in sub_cfg_dict:
            file_settings["subtitle_config"] = sub_cfg_dict[fpath]
        # Pre-resolve preset for display
        p_name = file_settings.get("preset", "auto")
        w = info_dict.get("width", 0)
        h = info_dict.get("height", 0)
        if p_name in ("auto",) or p_name in PRESETS:
            pre_r = resolve_preset(p_name, w, h)
            file_settings["resolved_cq"] = pre_r["cq"]
            file_settings["resolved_maxbitrate"] = pre_r["maxbitrate"]
            file_settings["resolved_speed"] = pre_r["speed"]
        else:
            file_settings["resolved_cq"] = file_settings.get("cq", 24)
            file_settings["resolved_maxbitrate"] = file_settings.get("maxbitrate", "20M")
            file_settings["resolved_speed"] = file_settings.get("speed", "p5")
        job = encode_queue.add(info_dict, file_settings)
        if job:
            added.append(job.id)
        else:
            skipped_dupes += 1

    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    result = {"added": len(added), "skipped": skipped_dupes, "job_ids": added}
    return result


@app.delete("/api/queue/{job_id}")
async def queue_remove(job_id: str):
    ok = encode_queue.remove(job_id)
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"removed": ok}


@app.delete("/api/queue")
async def queue_remove_all():
    count = encode_queue.remove_all()
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"removed": count}


@app.post("/api/queue/reorder")
async def queue_reorder(req: ReorderRequest):
    encode_queue.reorder(req.job_ids)
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}


@app.post("/api/queue/cancel")
async def queue_cancel(job_id: str = None):
    await encode_queue.cancel_job(job_id)
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}


@app.get("/api/queue/state")
async def queue_state():
    return encode_queue.get_state()


@app.post("/api/queue/clear-history")
async def clear_history(filter: str = "all"):
    if filter == "all":
        encode_queue.history.clear()
    else:
        encode_queue.history = [j for j in encode_queue.history if j.get("status") != filter]
    encode_queue._save_state()
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}


@app.post("/api/queue/retry")
async def queue_retry(job_id: str):
    """Re-queue a failed/cancelled/skipped job from history using its original settings."""
    for i, job in enumerate(encode_queue.history):
        if job.get("id") == job_id:
            fi = job.get("file_info", {})
            if not os.path.exists(fi.get("path", "")):
                return {"ok": False, "error": "Source file no longer exists"}
            # Strip remote assignment and retry flags so auto-scheduling re-evaluates
            _retry_strip = {"use_cpu", "_oom_retries", "_remote_server_idx", "_remote_gpu_name", "_remote_encoder_type"}
            s = {k: v for k, v in job.get("settings", {}).items() if k not in _retry_strip}
            if s.get("encoder") == "remote":
                s["encoder"] = "gpu"
            if s.get("gpu_target", "auto") == "remote":
                s["gpu_target"] = "auto"
            new_job = encode_queue.add(fi, s)
            if not new_job:
                return {"ok": False, "error": "File is already queued or encoding"}
            encode_queue.history.pop(i)
            encode_queue._save_state()
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            return {"ok": True, "new_job_id": new_job.id}
    return {"ok": False, "error": "Job not found in history"}


@app.delete("/api/queue/history/{job_id}")
async def delete_history_item(job_id: str):
    """Remove a single job from history."""
    for i, job in enumerate(encode_queue.history):
        if job.get("id") == job_id:
            encode_queue.history.pop(i)
            encode_queue._save_state()
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
            return {"ok": True}
    return {"ok": False, "error": "Job not found in history"}


@app.post("/api/queue/pause")
async def queue_pause(job_id: str):
    await encode_queue.pause_job(job_id)
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}


@app.post("/api/queue/resume")
async def queue_resume(job_id: str):
    await encode_queue.resume_job(job_id)
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}


@app.post("/api/queue/start")
async def queue_start():
    """Enable queue processing — workers will pick up queued jobs."""
    encode_queue.queue_enabled = True
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}


@app.post("/api/queue/stop")
async def queue_stop():
    """Disable queue processing — no new jobs will start, active encodes continue."""
    encode_queue.queue_enabled = False
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}


@app.post("/api/queue/stop-now")
async def queue_stop_now():
    """Disable queue processing and cancel all active encodes."""
    encode_queue.queue_enabled = False
    await encode_queue.cancel_job()  # cancel all active
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}

@app.post("/api/queue/cancel-active")
async def queue_cancel_active():
    """Cancel all active encodes without stopping the queue."""
    await encode_queue.cancel_job()  # cancel all active
    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
    return {"ok": True}

@app.post("/api/backfill-manifests")
async def backfill_manifests(req: dict = None):
    """Scan allowed paths for existing encoded outputs and populate .recode.json manifests."""
    search_paths = app_settings.get("library_paths", []) + app_settings.get("allowed_paths", ["/mnt"])
    search_paths = [p for p in search_paths if p and os.path.isdir(p)]
    if not search_paths:
        return {"ok": False, "error": "No library or allowed paths configured"}
    updated = 0
    dirs_scanned = 0
    for base_path in search_paths:
        for root, dirs, files in os.walk(base_path):
            # Skip hidden dirs
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            filenames = set(files)
            manifest = read_recode_manifest(root)
            changed = False
            for fname in files:
                if not is_encoded_output(fname):
                    continue
                # This is an encoded output — find its source
                stem = os.path.splitext(fname)[0]
                # Strip the encode tag to recover original stem
                m = ENCODE_TAG_RE.search(stem)
                if not m:
                    continue
                orig_stem = stem[:m.start()]
                # Find the source file (any video extension)
                source_name = None
                for ext in ('.mkv', '.mp4', '.avi', '.ts', '.m2ts', '.wmv', '.flv', '.mov'):
                    candidate = orig_stem + ext
                    if candidate in filenames and candidate != fname:
                        source_name = candidate
                        break
                if source_name and source_name not in manifest:
                    manifest[source_name] = {
                        "output": fname,
                        "encoded_at": "backfill",
                        "version": "backfill",
                    }
                    changed = True
                    updated += 1
            if changed:
                mpath = os.path.join(root, MANIFEST_NAME)
                try:
                    with open(mpath, "w") as f:
                        json.dump(manifest, f, indent=2)
                except Exception:
                    pass
            dirs_scanned += 1
    log.info(f"Manifest backfill: {updated} entries added across {dirs_scanned} directories")
    return {"ok": True, "updated": updated, "dirs_scanned": dirs_scanned}

@app.post("/api/delete-file")
async def delete_file(req: dict):
    """Delete a file from disk. Used by scan results to remove unwanted files."""
    path = req.get("path", "")
    if not path or not os.path.isfile(path):
        return {"ok": False, "error": "File not found"}
    # Safety: only allow deleting files under configured library or allowed paths
    safe_paths = app_settings.get("library_paths", []) + app_settings.get("allowed_paths", ["/mnt"])
    if not any(path.startswith(sp.rstrip("/")) for sp in safe_paths if sp):
        return {"ok": False, "error": "File is not under a configured library or allowed path"}
    try:
        os.remove(path)
        log.info(f"Deleted file: {path}")
        return {"ok": True}
    except Exception as e:
        log.error(f"Failed to delete file {path}: {e}")
        return {"ok": False, "error": str(e)}


# =============================================================================
# System Transcoding Detection
# =============================================================================

@app.get("/api/system/stats")
async def get_system_stats():
    """Return CPU/GPU usage history for graphs."""
    # CPU info
    cpu_name = ""
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu_name = line.split(":", 1)[1].strip()
                    break
    except Exception:
        pass
    cpu_cores = psutil.cpu_count(logical=True) or 1
    mem = psutil.virtual_memory()

    result = {
        "cpu": list(stats_history["cpu"]),
        "cpu_name": cpu_name,
        "cpu_cores": cpu_cores,
        "mem_used": round(mem.used / (1024**3), 1),
        "mem_total": round(mem.total / (1024**3), 1),
        "gpu": list(stats_history["gpu"]),
        "gpu_mem": list(stats_history["gpu_mem"]),
        "gpu_temp": list(stats_history["gpu_temp"]),
        "gpu_count": GPU_COUNT,
    }
    # Per-GPU breakdown (always include for GPU info labels)
    result["per_gpu"] = {}
    for gi, data in per_gpu_stats.items():
        info = per_gpu_info.get(gi, {})
        mem_total = info.get("mem_total", 0)
        result["per_gpu"][str(gi)] = {
            "util": list(data["util"]),
            "temp": list(data["temp"]),
            "mem_pct": list(data.get("mem_pct", [])),
            "name": info.get("name", ""),
            "mem_used": info.get("mem_used", 0),
            "mem_total": mem_total,
            "can_4k": mem_total > 2048,
        }
    return result


@app.get("/api/system/transcodes")
async def get_system_transcodes():
    """Detect all ffmpeg/transcode processes running on the system."""
    processes = []
    our_pids = set()
    for proc in encode_queue.ffmpeg_procs.values():
        if proc and proc.returncode is None:
            our_pids.add(proc.pid)

    try:
        # Get all ffmpeg processes with CPU/MEM stats
        result = await asyncio.create_subprocess_exec(
            "ps", "aux",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await result.communicate()

        for line in stdout.decode().splitlines():
            parts = line.split(None, 10)
            if len(parts) < 11:
                continue
            user, pid_str, cpu, mem = parts[0], parts[1], parts[2], parts[3]
            cmd_full = parts[10]

            # Match ffmpeg and Plex Transcoder
            is_ffmpeg = "ffmpeg" in cmd_full and "-i " in cmd_full
            is_plex_transcoder = "Plex Transcoder" in cmd_full or "Plex New Transcoder" in cmd_full
            if not (is_ffmpeg or is_plex_transcoder):
                continue

            try:
                pid = int(pid_str)
            except ValueError:
                continue

            # Skip our own ffmpeg processes
            if pid in our_pids:
                continue

            # Read the actual cmdline from /proc for accurate argument parsing
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    cmdline_args = f.read().decode("utf-8", errors="replace").split("\0")
                    cmdline_args = [a for a in cmdline_args if a]  # remove empty
            except (FileNotFoundError, PermissionError):
                cmdline_args = None

            if cmdline_args:
                info = _parse_transcode_args(cmdline_args)
            else:
                info = _parse_transcode_cmd(cmd_full)

            info["pid"] = pid
            info["user"] = user
            info["cpu"] = float(cpu)
            info["mem"] = float(mem)
            info["source"] = "plex" if is_plex_transcoder else "ffmpeg"

            # Check if this is an RRP remote job
            # Try /proc/environ first, fall back to detecting /tmp/rrp/ in cmdline
            rrp_detected = False
            try:
                with open(f"/proc/{pid}/environ", "rb") as ef:
                    env_data = ef.read().decode("utf-8", errors="replace")
                    env_vars = dict(kv.split("=", 1) for kv in env_data.split("\0") if "=" in kv)
                    if "RRP_JOB_ID" in env_vars:
                        info["source"] = "remote"
                        info["rrp_job_id"] = env_vars.get("RRP_JOB_ID", "")
                        info["rrp_client"] = env_vars.get("RRP_CLIENT", "")
                        info["rrp_input"] = env_vars.get("RRP_INPUT", "")
                        rrp_detected = True
            except (FileNotFoundError, PermissionError):
                pass
            # Fallback: detect RRP jobs by /tmp/rrp/ path in args (works cross-user)
            if not rrp_detected and cmdline_args:
                for arg in cmdline_args:
                    if "/tmp/rrp/" in arg:
                        import re as _re
                        m = _re.search(r"/tmp/rrp/([^/]+)/", arg)
                        info["source"] = "remote"
                        info["rrp_job_id"] = m.group(1) if m else ""
                        # Try to read client/input from job marker file
                        if m:
                            marker = os.path.join("/tmp/rrp", m.group(1), ".rrp_info")
                            try:
                                with open(marker) as mf:
                                    import json as _json
                                    mdata = _json.load(mf)
                                    info["rrp_client"] = mdata.get("client", "")
                                    info["rrp_input"] = mdata.get("input", "")
                            except Exception:
                                info["rrp_client"] = ""
                                info["rrp_input"] = ""
                        break

            processes.append(info)

    except Exception as e:
        log.error(f"Error detecting transcodes: {e}")

    return {"transcodes": processes}


@app.post("/api/system/transcodes/{pid}/kill")
async def kill_transcode(pid: int):
    """Kill a transcoding process by PID."""
    # Safety: only kill ffmpeg or Plex Transcoder processes
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmdline = f.read().decode("utf-8", errors="replace")
        if "ffmpeg" not in cmdline and "Plex Transcoder" not in cmdline and "Plex New Transcoder" not in cmdline:
            return JSONResponse({"error": "Not a transcoding process"}, status_code=400)
    except FileNotFoundError:
        return JSONResponse({"error": "Process not found"}, status_code=404)
    except PermissionError:
        return JSONResponse({"error": "Permission denied reading process info"}, status_code=403)

    try:
        os.kill(pid, signal.SIGTERM)
        log.info(f"Killed transcode process PID {pid}")
        return {"ok": True, "pid": pid}
    except ProcessLookupError:
        return JSONResponse({"error": "Process not found"}, status_code=404)
    except PermissionError:
        return JSONResponse({"error": "Permission denied"}, status_code=403)


def _parse_transcode_args(tokens: list[str]) -> dict:
    """Extract info from a list of command-line arguments (from /proc/pid/cmdline)."""
    info = {"input": "", "output": "", "video_codec": "", "audio_codec": "", "command": ""}
    info["command"] = " ".join(tokens[:30]) + ("..." if len(tokens) > 30 else "")

    for i, tok in enumerate(tokens):
        if tok == "-i" and i + 1 < len(tokens):
            info["input"] = tokens[i + 1]
            break

    for i, tok in enumerate(tokens):
        if tok in ("-c:v", "-vcodec") and i + 1 < len(tokens):
            info["video_codec"] = tokens[i + 1]
            break

    for i, tok in enumerate(tokens):
        if tok in ("-c:a", "-acodec") and i + 1 < len(tokens):
            info["audio_codec"] = tokens[i + 1]
            break

    # Output is typically the last argument
    flag_args = {"-i", "-c:v", "-c:a", "-vcodec", "-acodec", "-b:v", "-b:a",
                 "-preset", "-cq", "-maxrate", "-bufsize", "-f", "-map",
                 "-filter:v", "-filter:a", "-ss", "-t", "-to"}
    skip_next = False
    non_flags = []
    for tok in tokens[1:]:
        if skip_next:
            skip_next = False
            continue
        if tok.startswith("-"):
            if tok in flag_args:
                skip_next = True
            continue
        non_flags.append(tok)
    if non_flags:
        info["output"] = non_flags[-1]

    return info


def _parse_transcode_cmd(cmd: str) -> dict:
    """Extract input file, codec, and other info from an ffmpeg/transcoder command line."""
    info = {"input": "", "output": "", "video_codec": "", "audio_codec": "", "command": ""}

    # Truncate command for display
    info["command"] = cmd[:200] + ("..." if len(cmd) > 200 else "")

    # Extract input file (-i)
    import shlex
    try:
        tokens = shlex.split(cmd)
    except ValueError:
        tokens = cmd.split()

    for i, tok in enumerate(tokens):
        if tok == "-i" and i + 1 < len(tokens):
            info["input"] = tokens[i + 1]
            break

    # Extract video codec (-c:v or -vcodec)
    for i, tok in enumerate(tokens):
        if tok in ("-c:v", "-vcodec") and i + 1 < len(tokens):
            info["video_codec"] = tokens[i + 1]
            break

    # Extract audio codec (-c:a or -acodec)
    for i, tok in enumerate(tokens):
        if tok in ("-c:a", "-acodec") and i + 1 < len(tokens):
            info["audio_codec"] = tokens[i + 1]
            break

    # Try to find output file (last non-flag argument)
    non_flags = []
    skip_next = False
    for tok in tokens[1:]:
        if skip_next:
            skip_next = False
            continue
        if tok.startswith("-"):
            if tok in ("-i", "-c:v", "-c:a", "-vcodec", "-acodec", "-b:v", "-b:a",
                        "-preset", "-cq", "-maxrate", "-bufsize", "-f", "-map",
                        "-filter:v", "-filter:a", "-ss", "-t", "-to"):
                skip_next = True
            continue
        non_flags.append(tok)
    if non_flags:
        info["output"] = non_flags[-1]

    return info


# =============================================================================
# Encode Statistics
# =============================================================================

@app.get("/api/logs")
async def get_logs(lines: int = 200):
    """Return recent log entries from server, listener, remote connectors, and ffmpeg."""
    result = {}
    # ffmpeg logs (active + recent history)
    ffmpeg_logs = []
    for jid, job_lines in list(encode_queue.ffmpeg_logs.items()):
        fi = encode_queue.jobs[jid].file_info if jid in encode_queue.jobs else {}
        fname = fi.get("filename", jid)
        for line in job_lines[-20:]:
            ffmpeg_logs.append(f"[{fname[:40]}] {line}")
    # Also include recent history job logs
    for h in encode_queue.history[-10:]:
        for line in (h.get("log") or [])[-10:]:
            fname = h.get("file_info", {}).get("filename", h.get("id", "?"))
            ffmpeg_logs.append(f"[{fname[:40]}] {line}")
    # Also pull ffmpeg-related lines from connector logs
    for i in range(10):
        cpath = os.path.join(BASE_DIR, f"rrp-connect-{i}.log")
        if not os.path.isfile(cpath):
            break
        try:
            with open(cpath) as f:
                for line in f.readlines()[-lines:]:
                    line = line.rstrip()
                    if any(kw in line for kw in ("ffmpeg ", "ffmpeg_stderr", "Probe ", "GPU connector", "Assigned GPU", "Compiled encoder", "capability", "exited", "error", "ERROR", "WARN")):
                        ffmpeg_logs.append(f"[Connector {i}] {line}")
        except Exception:
            pass
    result["ffmpeg"] = ffmpeg_logs[-lines:]
    # GC logs from connector logs + server queue dispatch logs
    gc_logs = []
    for i in range(10):
        cpath = os.path.join(BASE_DIR, f"rrp-connect-{i}.log")
        if not os.path.isfile(cpath):
            break
        try:
            with open(cpath) as f:
                for line in f.readlines()[-lines:]:
                    line = line.rstrip()
                    if "GC:" in line:
                        gc_logs.append(f"[Connector {i}] {line}")
        except Exception:
            pass
    # Queue dispatch/skip logs from server
    for entry in _log_buffer.buffer:
        msg = entry.get("msg", "")
        if "[queue]" in msg or "Dispatched:" in msg or "Skip:" in msg:
            gc_logs.append(msg)
    result["gc"] = gc_logs[-lines:]
    # Server logs (in-memory ring buffer)
    result["server"] = list(_log_buffer.buffer)[-lines:]
    # Listener log
    try:
        with open(os.path.join(BASE_DIR, "rrp-client-listener.log")) as f:
            raw = f.readlines()[-lines:]
        result["listener"] = [l.rstrip() for l in raw]
    except Exception:
        result["listener"] = []
    # Remote connector logs
    connectors = []
    for i in range(10):
        path = os.path.join(BASE_DIR, f"rrp-connect-{i}.log")
        if not os.path.isfile(path):
            break
        try:
            with open(path) as f:
                raw = f.readlines()[-lines:]
            connectors.append({"index": i, "lines": [l.rstrip() for l in raw]})
        except Exception:
            pass
    result["connectors"] = connectors
    return result

@app.get("/api/stats/history")
async def get_stats_history():
    """Return daily encode history for charts (last 30 days)."""
    conn = get_cache_db()
    try:
        rows = conn.execute("SELECT date, done, failed, orig_bytes, new_bytes, saved_bytes, encode_time FROM encode_daily ORDER BY date DESC LIMIT 30").fetchall()
        return {"days": [dict(r) for r in reversed(rows)]}
    except Exception:
        return {"days": []}
    finally:
        conn.close()

@app.get("/api/stats")
async def get_encode_stats():
    """Return aggregate encode statistics from persistent DB."""
    s = _get_encode_stats()
    if not s:
        s = {"done": 0, "failed": 0, "skipped": 0, "total_orig_bytes": 0,
             "total_new_bytes": 0, "total_saved_bytes": 0, "total_encode_time": 0,
             "savings_pct_sum": 0, "savings_pct_count": 0}

    total_files = s["done"] + s["failed"] + s["skipped"]
    avg_savings = round(s["savings_pct_sum"] / s["savings_pct_count"], 1) if s["savings_pct_count"] > 0 else 0
    t = s["total_encode_time"]

    return {
        "total_files": total_files,
        "done": s["done"],
        "failed": s["failed"],
        "skipped": s["skipped"],
        "total_orig_bytes": s["total_orig_bytes"],
        "total_orig_size": human_size(s["total_orig_bytes"]),
        "total_new_bytes": s["total_new_bytes"],
        "total_new_size": human_size(s["total_new_bytes"]),
        "total_saved_bytes": s["total_saved_bytes"],
        "total_saved_size": human_size(s["total_saved_bytes"]),
        "avg_savings_pct": avg_savings,
        "total_encode_time": t,
        "total_encode_time_human": f"{int(t // 3600)}h {int((t % 3600) // 60)}m",
    }


@app.post("/api/stats/reset")
async def reset_encode_stats():
    """Reset all persistent encode statistics."""
    conn = get_cache_db()
    try:
        conn.execute("DELETE FROM encode_stats")
        conn.execute("INSERT INTO encode_stats (id) VALUES (1)")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


# =============================================================================
# Log Viewer
# =============================================================================

@app.get("/api/queue/{job_id}/log")
async def get_job_log(job_id: str):
    """Get ffmpeg stderr log for an active or completed job."""
    # Check active jobs
    if job_id in encode_queue.ffmpeg_logs:
        return {"log": encode_queue.ffmpeg_logs[job_id]}
    # Check history
    for h in encode_queue.history:
        if h.get("id") == job_id:
            return {"log": h.get("log", [])}
    return {"log": []}


# =============================================================================
# Folder Watch
# =============================================================================

watch_task = None

async def folder_watcher():
    """Background task that monitors watched directories for new video files."""
    known_files: dict[str, set[str]] = {}  # path -> set of known files

    while True:
        if not app_settings.get("watch_enabled", False):
            await asyncio.sleep(10)
            continue

        watch_paths = app_settings.get("watch_paths", [])
        interval = app_settings.get("watch_interval", 300)
        log.debug(f"Folder watch: scanning {len(watch_paths)} paths, interval={interval}s")

        for watch_path in watch_paths:
            if not os.path.isdir(watch_path):
                log.warning(f"Folder watch: path does not exist: {watch_path}")
                continue

            current_files = set()
            for root, _, files in os.walk(watch_path):
                for f in files:
                    ext = os.path.splitext(f)[1].lower()
                    if ext in VIDEO_EXTENSIONS:
                        current_files.add(os.path.join(root, f))

            prev = known_files.get(watch_path, set())
            new_files = current_files - prev
            known_files[watch_path] = current_files
            log.info(f"Folder watch [{watch_path}]: {len(current_files)} files, {len(prev)} known, {len(new_files)} new, first_run={not bool(prev)}")

            if prev and new_files:  # Skip first run (don't queue existing files)
                for filepath in new_files:
                    # Skip if already in queue
                    in_queue = any(
                        encode_queue.jobs[jid].file_info.get("path") == filepath
                        for jid in encode_queue.queue_order
                        if jid in encode_queue.jobs
                    )
                    if in_queue:
                        continue

                    # Check for library profile
                    profile = None
                    for lpath, lprofile in app_settings.get("library_profiles", {}).items():
                        if filepath.startswith(lpath):
                            profile = lprofile
                            break

                    info = await get_file_info(filepath)
                    if not info:
                        continue

                    info_dict = {
                        "path": info.path, "filename": info.filename, "dirname": info.dirname,
                        "size_bytes": info.size_bytes, "size_human": info.size_human,
                        "codec": info.codec, "width": info.width, "height": info.height,
                        "resolution_label": info.resolution_label, "pix_fmt": info.pix_fmt,
                        "hdr_type": info.hdr_type, "is_hdr": info.is_hdr,
                        "color_transfer": info.color_transfer, "color_primaries": info.color_primaries,
                        "duration_secs": info.duration_secs, "audio_streams": info.audio_streams,
                        "sub_streams": info.sub_streams,
                        "is_hevc": info.is_hevc, "has_dovi": info.has_dovi, "dovi_profile": info.dovi_profile, "hdr10_metadata": info.hdr10_metadata,
                        "output_exists": info.output_exists,
                        "suggestion": compute_suggestion(info),
                    }

                    # Check for skip reasons
                    skip_reason = None
                    if info.is_hevc:
                        skip_reason = "Already HEVC"
                    elif is_encoded_output(filepath):
                        skip_reason = "Already encoded output"
                    elif info.output_exists:
                        skip_reason = "Encoded output already exists"

                    if skip_reason:
                        now = time.time()
                        encode_queue.history.append({
                            "id": f"fw-skip-{now:.4f}-{info.filename}",
                            "file_info": info_dict, "settings": {},
                            "status": JobStatus.SKIPPED,
                            "error": skip_reason, "started_at": now,
                            "finished_at": now, "result": {}, "log": [],
                        })
                        _record_encode_stat("skipped", {}, now, now)
                        encode_queue._save_state()
                        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})
                        log.info(f"Folder watch: skipped {info.filename} — {skip_reason}")
                        continue

                    # Use library profile (merged with defaults) or default profile
                    defaults = build_default_profile()
                    if profile:
                        merged = {**defaults, **profile}
                        merged["use_cpu"] = merged.get("encoder", "gpu") == "cpu" if "encoder" in profile else defaults["use_cpu"]
                    else:
                        merged = defaults
                    file_settings = merged
                    # Build per-stream audio config from profile language settings
                    fw_audio_codec = merged.get("audio_codec", "libopus")
                    fw_audio_bitrate = merged.get("audio_bitrate", "448k")
                    fw_audio_lang_mode = merged.get("audio_lang_mode", "all")
                    fw_audio_langs = set(l.strip().lower() for l in merged.get("audio_langs", "eng").split(",") if l.strip())
                    fw_audio_cfg = []
                    for ai, astream in enumerate(info.audio_streams):
                        lang = astream.get("language", "und").lower()
                        if fw_audio_lang_mode == "langs":
                            include = lang in fw_audio_langs or lang in ("und", "")
                        else:
                            include = True
                        fw_audio_cfg.append({
                            "index": ai,
                            "include": include,
                            "codec": fw_audio_codec,
                            "bitrate": fw_audio_bitrate,
                        })
                    file_settings["audio_config"] = fw_audio_cfg
                    # Build per-stream subtitle config from profile language settings
                    fw_sub_mode = merged.get("subtitle_mode", "all")
                    fw_sub_langs = set(l.strip().lower() for l in merged.get("subtitle_langs", "eng").split(",") if l.strip())
                    fw_sub_cfg = []
                    for si, sstream in enumerate(info.sub_streams):
                        lang = sstream.get("language", "und").lower()
                        if fw_sub_mode == "none":
                            include = False
                        elif fw_sub_mode == "langs":
                            include = lang in fw_sub_langs or lang in ("und", "")
                        else:
                            include = True
                        fw_sub_cfg.append({"index": si, "include": include})
                    file_settings["subtitle_config"] = fw_sub_cfg
                    # Pre-resolve preset for display
                    fw_preset = file_settings.get("preset", "auto")
                    if fw_preset in ("auto",) or fw_preset in PRESETS:
                        fw_r = resolve_preset(fw_preset, info.width, info.height)
                        file_settings["resolved_cq"] = fw_r["cq"]
                        file_settings["resolved_maxbitrate"] = fw_r["maxbitrate"]
                        file_settings["resolved_speed"] = fw_r["speed"]
                    else:
                        file_settings["resolved_cq"] = file_settings.get("cq", 24)
                        file_settings["resolved_maxbitrate"] = file_settings.get("maxbitrate", "20M")
                        file_settings["resolved_speed"] = file_settings.get("speed", "p5")
                    encode_queue.add(info_dict, file_settings)
                    log.info(f"Folder watch: queued {info.filename} | preset={fw_preset}, cq={file_settings['resolved_cq']}, maxbitrate={file_settings['resolved_maxbitrate']}, speed={file_settings['resolved_speed']}")

                if new_files:
                    await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})

        await asyncio.sleep(interval)


# =============================================================================
# Duplicate Detection
# =============================================================================

@app.get("/api/scan/duplicates")
async def detect_duplicates(path: str):
    """Find files that have both original and encoded versions."""
    if not is_path_allowed(path):
        return JSONResponse({"error": f"Restricted to: {', '.join(app_settings.get('allowed_paths', ['/mnt']))}"}, status_code=403)
    if not os.path.isdir(path):
        return JSONResponse({"error": "Directory not found"}, status_code=400)

    duplicates = []
    for root, _, files in os.walk(path):
        file_set = set(files)
        for f in files:
            if is_encoded_output(f):
                continue
            nameonly = os.path.splitext(f)[0]
            # Check for encoded versions (old _Streamer or new tagged format)
            encoded_name = None
            for other in file_set:
                if other != f and other.startswith(nameonly) and is_encoded_output(other):
                    encoded_name = other
                    break
            if encoded_name:
                orig_path = os.path.join(root, f)
                enc_path = os.path.join(root, encoded_name)
                try:
                    orig_size = os.path.getsize(orig_path)
                    enc_size = os.path.getsize(enc_path)
                    saved_pct = int((orig_size - enc_size) * 100 / orig_size) if orig_size > 0 else 0
                    duplicates.append({
                        "original": orig_path,
                        "encoded": enc_path,
                        "original_name": f,
                        "encoded_name": encoded_name,
                        "original_size": human_size(orig_size),
                        "encoded_size": human_size(enc_size),
                        "original_bytes": orig_size,
                        "encoded_bytes": enc_size,
                        "saved_pct": saved_pct,
                        "wasted_bytes": orig_size,
                        "wasted_size": human_size(orig_size),
                    })
                except OSError:
                    pass

    total_wasted = sum(d["original_bytes"] for d in duplicates)
    return {
        "duplicates": duplicates,
        "count": len(duplicates),
        "total_wasted_bytes": total_wasted,
        "total_wasted_size": human_size(total_wasted),
    }


@app.get("/api/scan/space-savings")
async def get_space_savings(path: str):
    """Calculate total potential space savings for a scanned directory."""
    if not is_path_allowed(path):
        return JSONResponse({"error": f"Restricted to: {', '.join(app_settings.get('allowed_paths', ['/mnt']))}"}, status_code=403)
    if not os.path.isdir(path):
        return JSONResponse({"error": "Directory not found"}, status_code=400)

    total_size = 0
    total_potential_savings = 0
    file_count = 0
    encodable_count = 0

    for root, _, files in os.walk(path):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext not in VIDEO_EXTENSIONS:
                continue
            if is_encoded_output(f):
                continue
            filepath = os.path.join(root, f)
            try:
                size = os.path.getsize(filepath)
                total_size += size
                file_count += 1
                # Quick check if already HEVC using cache
                try:
                    cache_conn = get_cache_db()
                    row = cache_conn.execute("SELECT codec, savings_pct, size_bytes FROM file_cache WHERE path = ?", (filepath,)).fetchone()
                    cache_conn.close()
                    if row:
                        if row["codec"] != "hevc" and row["savings_pct"] > 0:
                            total_potential_savings += int(row["size_bytes"] * row["savings_pct"] / 100)
                            encodable_count += 1
                        continue
                except Exception:
                    pass
            except OSError:
                pass

    return {
        "total_size": human_size(total_size),
        "total_size_bytes": total_size,
        "file_count": file_count,
        "encodable_count": encodable_count,
        "potential_savings": human_size(total_potential_savings),
        "potential_savings_bytes": total_potential_savings,
        "potential_savings_pct": round(total_potential_savings * 100 / total_size, 1) if total_size > 0 else 0,
    }


@app.delete("/api/scan/duplicates/cleanup")
async def cleanup_duplicate(path: str, keep: str = "encoded"):
    """Delete either the original or encoded file from a duplicate pair."""
    if keep not in ("original", "encoded"):
        return JSONResponse({"error": "keep must be 'original' or 'encoded'"}, status_code=400)
    if not os.path.exists(path):
        return JSONResponse({"error": "File not found"}, status_code=404)
    if not is_path_allowed(path):
        return JSONResponse({"error": f"File must be under: {', '.join(app_settings.get('allowed_paths', ['/mnt']))}"}, status_code=403)
    try:
        os.remove(path)
        return {"ok": True, "deleted": path}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# =============================================================================
# Settings Endpoints
# =============================================================================

@app.get("/api/remote-gpu/status")
async def remote_gpu_status():
    """Check online status of all configured remote clients (GPU→client connectors)."""
    servers = app_settings.get("remote_gpu_servers", [])
    results = []
    for i, srv in enumerate(servers):
        addr = (srv.get("address") or "").strip()
        name = srv.get("name") or addr or f"Client {i}"
        if not addr:
            results.append({"index": i, "name": name, "online": False, "error": "No address"})
            continue
        # Check actual connection status from connector's status file
        proc = _remote_connect_procs.get(i)
        proc_alive = proc is not None and proc.returncode is None
        connected = False
        connect_error = ""
        if proc_alive:
            status_file = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", f"connect-status-{i}.json")
            try:
                with open(status_file) as f:
                    st = json.load(f)
                connected = st.get("connected", False)
                connect_error = st.get("error", "")
                connect_status = st.get("status", "")
            except Exception:
                pass
        if connected:
            if i < len(servers):
                servers[i]["_online"] = True
            results.append({"index": i, "name": name, "online": True, "address": addr})
        elif srv.get("enabled", True) is False:
            results.append({"index": i, "name": name, "online": False, "error": "Disabled"})
        else:
            if i < len(servers):
                servers[i]["_online"] = False
            error_msg = connect_status or connect_error or ("Not running" if not proc_alive else "Not connected")
            results.append({"index": i, "name": name, "online": False, "error": error_msg})
    return {"servers": results}

@app.get("/api/ffmpeg-server/status")
async def ffmpeg_server_status():
    """Check if the local GPU server is running."""
    running = _ffmpeg_server_proc is not None and _ffmpeg_server_proc.returncode is None
    return {
        "enabled": app_settings.get("ffmpeg_server_enabled", False),
        "running": running,
        "port": app_settings.get("ffmpeg_server_port", 9878),
    }

@app.get("/api/remote-clients/status")
async def remote_client_status():
    """Return status of GPU servers connected via reverse-connect."""
    status_file = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-status.json")
    running = _remote_client_proc is not None and _remote_client_proc.returncode is None
    if running and os.path.isfile(status_file):
        try:
            with open(status_file) as f:
                data = json.load(f)
            data["port"] = app_settings.get("remote_client_port", 9879)
            return data
        except Exception:
            pass
    return {
        "enabled": app_settings.get("remote_client_enabled", False),
        "running": running,
        "port": app_settings.get("remote_client_port", 9879),
        "gpus": [],
    }

@app.get("/api/settings")
async def get_settings():
    """Return all app settings."""
    # Check if any GPU (local or remote) is still scanning capabilities
    _any_scanning = not _gpu_scan_complete
    if not _any_scanning:
        try:
            _lsf = os.path.join(app_settings.get("tmp_dir", "/tmp/recode"), "rrp", "listener-status.json")
            if os.path.isfile(_lsf):
                with open(_lsf) as _f:
                    for _g in json.load(_f).get("gpus", []):
                        # Connected but capabilities empty = still scanning
                        if not _g.get("gpu_capabilities") and _g.get("name"):
                            _any_scanning = True
                            break
        except Exception:
            pass
    return {"settings": app_settings, "plex_token_found": PLEX_TOKEN is not None, "gpu_scan_complete": not _any_scanning}

@app.post("/api/settings")
async def update_settings(new_settings: dict):
    """Update and persist app settings."""
    global _prev_remote_cfg
    # Snapshot config before applying changes (for change detection)
    _old_gpu_max_jobs = json.dumps(app_settings.get("gpu_max_jobs", {}), sort_keys=True)
    _old_disabled_gpus = sorted(app_settings.get("disabled_gpus", []))
    _old_listener_cfg = (app_settings.get("remote_client_enabled"), app_settings.get("remote_client_port"), app_settings.get("remote_client_secret"))
    # Accept all known keys plus dynamic ones like library_profiles
    known_keys = set(APP_DEFAULTS.keys())
    for k, v in new_settings.items():
        if k in known_keys:
            # Sanitize encode_suffix: only allow alphanumeric, hyphens, underscores
            if k == "encode_suffix":
                v = re.sub(r"[^A-Za-z0-9_-]", "", str(v).strip()) or "recode"
            app_settings[k] = v
    # Sync default profile
    WEBHOOK_DEFAULTS.clear()
    WEBHOOK_DEFAULTS.update(build_default_profile())
    save_settings(app_settings)

    # If gpu_max_jobs or disabled_gpus actually changed, requeue excess jobs
    _gpu_jobs_changed = json.dumps(app_settings.get("gpu_max_jobs", {}), sort_keys=True) != _old_gpu_max_jobs
    _disabled_changed = sorted(app_settings.get("disabled_gpus", [])) != _old_disabled_gpus
    if _gpu_jobs_changed or _disabled_changed:
        gpu_loads = encode_queue.get_gpu_loads()
        disabled = set(app_settings.get("disabled_gpus", []))
        to_requeue = []
        active = [(jid, j) for jid, j in encode_queue.active_jobs.items()
                  if not j.paused and j.settings.get("_remote_server_idx", -1) < 0]
        active.sort(key=lambda x: x[1].started_at or 0)
        # Find GPUs over their limit or disabled
        for jid, j in reversed(active):
            gid = encode_queue.job_gpus.get(jid, 0)
            if gid in disabled or gpu_loads.get(gid, 0) > encode_queue.gpu_max_encodes(gid):
                to_requeue.append((jid, j))
                gpu_loads[gid] = gpu_loads.get(gid, 0) - 1
        if to_requeue:
            for jid, job in to_requeue:
                gid = encode_queue.job_gpus.get(jid, 0)
                log.info(f"[{jid}] Re-queuing — GPU {gid} over limit or disabled")
                # Kill ffmpeg process
                proc = encode_queue.ffmpeg_procs.get(jid)
                if proc:
                    try:
                        if job.paused:
                            os.kill(proc.pid, signal.SIGCONT)
                        proc.kill()
                        await proc.wait()
                    except Exception:
                        pass
                # Clean up temp files
                info = job.file_info
                settings = job.settings
                tmp_dir = settings.get("tmp_dir", "/tmp/recode")
                p = Path(info.get("path", ""))
                encode_tag = build_encode_tag(settings.get("video_codec", "hevc"), info, settings.get("dv_mode", "skip"), settings.get("resize", "original"))
                for tmp_name in [f"{p.stem}{encode_tag}.mkv", f"{jid}_source.hevc", f"{jid}_encoded.hevc",
                                 f"{jid}_rpu.bin", f"{jid}_injected.hevc", f"{jid}_dv.mkv",
                                 f"{jid}_dv_temp.mp4", f"{jid}_dv_gen.json"]:
                    tmp_path = os.path.join(tmp_dir, tmp_name)
                    if os.path.exists(tmp_path):
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass
                # Move back to queued state
                job.status = JobStatus.QUEUED
                job.started_at = None
                job.finished_at = None
                job.progress = None
                job.error = None
                job.paused = False
                encode_queue.active_jobs.pop(jid, None)
                encode_queue.ffmpeg_procs.pop(jid, None)
                encode_queue.ffmpeg_logs.pop(jid, None)
                encode_queue._proc_ended_at.pop(jid, None)
                encode_queue.job_gpus.pop(jid, None)
                encode_queue.queue_order = [j for j in encode_queue.queue_order if j != jid]
                encode_queue.queue_order.insert(0, jid)  # front of queue
                encode_queue.jobs[jid] = job
            encode_queue.running = len(encode_queue.active_jobs) > 0
            encode_queue._save_state(force=True)
            await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})

    # Restart reverse-connect listener only if listener settings actually changed
    _new_listener_cfg = (app_settings.get("remote_client_enabled"), app_settings.get("remote_client_port"), app_settings.get("remote_client_secret"))
    if _new_listener_cfg != _old_listener_cfg:
        await start_remote_client_listener()

    # Restart remote client connectors only if config actually changed
    if "remote_gpu_servers" in new_settings:
        old_cfg = json.dumps(_prev_remote_cfg, sort_keys=True) if _prev_remote_cfg else ""
        new_cfg = json.dumps(new_settings["remote_gpu_servers"], sort_keys=True)
        if old_cfg != new_cfg:
            await start_remote_connectors()
            _prev_remote_cfg = new_settings["remote_gpu_servers"]

    return {"ok": True, "settings": app_settings}


# =============================================================================
# Plex Webhook Endpoint
# =============================================================================

@app.get("/api/webhook/settings")
async def get_webhook_settings():
    """Return current webhook default settings."""
    return {"settings": WEBHOOK_DEFAULTS, "plex_token_found": PLEX_TOKEN is not None}

async def _webhook_queue_file(file_path: str) -> Optional[dict]:
    """Probe, build settings, and queue a single file from a webhook. Returns result dict."""
    if not os.path.exists(file_path):
        log.warning(f"Webhook: file not found: {file_path}")
        return {"file": file_path, "status": "skipped", "reason": "file not found"}

    if not is_path_allowed(file_path):
        log.warning(f"Webhook: file outside allowed paths: {file_path}")
        return {"file": file_path, "status": "skipped", "reason": "file outside allowed paths"}

    # Check if already in queue or active
    in_queue = any(
        encode_queue.jobs[jid].file_info.get("path") == file_path
        for jid in list(encode_queue.queue_order) + list(encode_queue.active_jobs.keys())
        if jid in encode_queue.jobs
    )
    if in_queue:
        log.info(f"Webhook: {file_path} already in queue, skipping")
        return {"file": file_path, "status": "skipped", "reason": "already in queue"}

    # Check if encoded output already exists on disk
    p = Path(file_path)
    nameonly = p.stem
    dirname = str(p.parent)
    # Skip encoded outputs
    if is_encoded_output(os.path.basename(file_path)):
        return {"file": file_path, "status": "skipped", "reason": "is encoded output"}

    # Check if encoded output already exists on disk
    try:
        for f in Path(dirname).iterdir():
            if f.stem.startswith(nameonly) and f.stem != nameonly and is_encoded_output(f.name):
                log.info(f"Webhook: {file_path} already has encoded output ({f.name}), skipping")
                return {"file": file_path, "status": "skipped", "reason": "output already exists"}
    except OSError:
        pass

    log.info(f"Webhook: probing {file_path}")

    info = await get_file_info(file_path)
    if not info:
        log.error(f"Webhook: failed to probe {file_path}")
        return {"file": file_path, "status": "error", "reason": "probe failed"}

    info_dict = {
        "path": info.path, "filename": info.filename, "dirname": info.dirname,
        "size_bytes": info.size_bytes, "size_human": info.size_human,
        "codec": info.codec, "width": info.width, "height": info.height,
        "resolution_label": info.resolution_label, "pix_fmt": info.pix_fmt,
        "hdr_type": info.hdr_type, "is_hdr": info.is_hdr,
        "color_transfer": info.color_transfer, "color_primaries": info.color_primaries,
        "duration_secs": info.duration_secs, "audio_streams": info.audio_streams,
        "sub_streams": info.sub_streams,
        "is_hevc": info.is_hevc, "has_dovi": info.has_dovi, "dovi_profile": info.dovi_profile, "hdr10_metadata": info.hdr10_metadata,
        "output_exists": info.output_exists,
        "suggestion": compute_suggestion(info),
    }

    # Check for a library profile matching this file's path
    lib_profile = None
    for lpath, lprofile in app_settings.get("library_profiles", {}).items():
        if file_path.startswith(lpath):
            lib_profile = lprofile
            log.info(f"Webhook: using library profile for {lpath}")
            break

    defaults = build_default_profile()
    if lib_profile:
        merged = {**defaults, **lib_profile}
        merged["use_cpu"] = merged.get("encoder", "gpu") == "cpu" if "encoder" in lib_profile else defaults["use_cpu"]
    else:
        merged = defaults

    file_settings = {k: v for k, v in merged.items()}
    profile_audio_codec = merged.get("audio_codec", "libopus")
    profile_audio_bitrate = merged.get("audio_bitrate", "448k")
    audio_lang_mode = merged.get("audio_lang_mode", "all")
    audio_langs = set(l.strip().lower() for l in merged.get("audio_langs", "eng").split(",") if l.strip())
    subtitle_mode = merged.get("subtitle_mode", "all")
    subtitle_langs = set(l.strip().lower() for l in merged.get("subtitle_langs", "eng").split(",") if l.strip())

    # Build per-stream audio config using profile language settings
    audio_cfg = []
    for i, astream in enumerate(info.audio_streams):
        lang = astream.get("language", "und").lower()
        if audio_lang_mode == "langs":
            include = lang in audio_langs or lang in ("und", "")
        else:
            include = True
        audio_cfg.append({
            "index": i,
            "include": include,
            "codec": profile_audio_codec,
            "bitrate": profile_audio_bitrate,
        })
    file_settings["audio_config"] = audio_cfg

    # Build per-stream subtitle config using profile language settings
    sub_cfg = []
    for i, sstream in enumerate(info.sub_streams):
        lang = sstream.get("language", "und").lower()
        if subtitle_mode == "none":
            include = False
        elif subtitle_mode == "langs":
            include = lang in subtitle_langs or lang in ("und", "")
        else:
            include = True
        sub_cfg.append({"index": i, "include": include})
    file_settings["subtitle_config"] = sub_cfg

    # Pre-resolve preset for display
    p_name = file_settings.get("preset", "auto")
    if p_name in ("auto",) or p_name in PRESETS:
        pre_r = resolve_preset(p_name, info.width, info.height)
        file_settings["resolved_cq"] = pre_r["cq"]
        file_settings["resolved_maxbitrate"] = pre_r["maxbitrate"]
        file_settings["resolved_speed"] = pre_r["speed"]
    else:
        file_settings["resolved_cq"] = file_settings.get("cq", 24)
        file_settings["resolved_maxbitrate"] = file_settings.get("maxbitrate", "20M")
        file_settings["resolved_speed"] = file_settings.get("speed", "p5")

    job = encode_queue.add(info_dict, file_settings)
    if not job:
        log.info(f"Webhook: skipped duplicate for {info.filename}")
        return {"status": "skipped", "reason": "duplicate"}
    log.info(f"Webhook: queued job {job.id} for {info.filename} ({info.size_human}) | preset={p_name}, cq={file_settings['resolved_cq']}, maxbitrate={file_settings['resolved_maxbitrate']}, speed={file_settings['resolved_speed']}")
    return {"file": info.filename, "status": "queued", "job_id": job.id}


async def _webhook_resolve_files(metadata: dict, metadata_key: str, media_type: str) -> list[str]:
    """Resolve all file paths from a Plex webhook payload.

    Handles show/season-level events by fetching all episodes and finding recently updated ones.
    Handles episode/movie-level events by fetching media parts directly.
    """
    file_paths = []

    # Try to get Media directly from the payload
    media_list = metadata.get("Media", [])
    for media_entry in media_list:
        for part in media_entry.get("Part", []):
            fp = part.get("file", "")
            if fp:
                file_paths.append(fp)

    if file_paths:
        return file_paths

    if not PLEX_TOKEN or not metadata_key:
        return file_paths

    # Fetch metadata from Plex API
    # Strip /children suffix — we want the item itself, not its children listing
    fetch_key = metadata_key.rstrip("/")
    if fetch_key.endswith("/children"):
        fetch_key = fetch_key[:-len("/children")]
    try:
        r = http_requests.get(
            f"{PLEX_URL}{fetch_key}",
            headers=plex_headers(), timeout=10
        )
        r.raise_for_status()
        api_data = r.json()
        api_metadata = api_data.get("MediaContainer", {}).get("Metadata", [])
    except Exception as e:
        log.error(f"Webhook: failed to fetch {fetch_key}: {e}")
        return file_paths

    if not api_metadata:
        return file_paths

    item = api_metadata[0]
    item_type = item.get("type", media_type)
    log.info(f"Webhook: metadata resolved to type={item_type}, title={item.get('title')}, ratingKey={item.get('ratingKey')}")

    # For show/season-level events, get all episodes and find recently updated ones
    if item_type in ("show", "season"):
        # Use ratingKey (numeric ID) to build the allLeaves URL — the 'key' field
        # may contain '/children' which breaks the URL
        rating_key = item.get("ratingKey", "")
        leaves_key = f"/library/metadata/{rating_key}/allLeaves" if rating_key else f"{metadata_key}/allLeaves"
        log.info(f"Webhook: fetching episodes from {leaves_key} (type={item_type}, ratingKey={rating_key})")
        try:
            r = http_requests.get(
                f"{PLEX_URL}{leaves_key}",
                headers=plex_headers(),
                params={"X-Plex-Container-Start": "0", "X-Plex-Container-Size": "200"},
                timeout=15,
            )
            r.raise_for_status()
            episodes = r.json().get("MediaContainer", {}).get("Metadata", [])
            now = time.time()
            for ep in episodes:
                added_at = ep.get("addedAt", 0)
                updated_at = ep.get("updatedAt", 0)
                most_recent = max(added_at, updated_at)
                if now - most_recent > 900:  # 15 minutes
                    continue
                ep_title = ep.get("title", "?")
                for media_entry in ep.get("Media", []):
                    for part in media_entry.get("Part", []):
                        fp = part.get("file", "")
                        if fp:
                            log.info(f"Webhook: resolved episode file: {fp} ({ep_title})")
                            file_paths.append(fp)
            log.info(f"Webhook: {item_type} '{item.get('title')}' resolved to {len(file_paths)} recent episode files from {len(episodes)} total")
        except Exception as e:
            log.error(f"Webhook: failed to fetch episodes for {metadata_key}: {e}")
    else:
        # Episode or movie — get media directly
        for media_entry in item.get("Media", []):
            for part in media_entry.get("Part", []):
                fp = part.get("file", "")
                if fp:
                    file_paths.append(fp)
        if file_paths:
            log.info(f"Webhook: resolved {len(file_paths)} files from {item_type} metadata")

    return file_paths


@app.post("/api/plex-webhook")
async def plex_webhook(payload: str = Form(None)):
    """
    Receive Plex webhook events.
    Plex sends multipart/form-data with a 'payload' field containing JSON.
    Configure in Plex: Settings > Webhooks > Add Webhook > http://<server>:9877/api/plex-webhook

    Processes each webhook immediately. For show/season-level events, queries
    the Plex API for all episodes and finds recently updated ones.
    """
    global _webhook_processed

    if not payload:
        return JSONResponse({"error": "No payload"}, status_code=400)

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return JSONResponse({"error": "Invalid JSON payload"}, status_code=400)

    event = data.get("event", "")
    metadata = data.get("Metadata", {})
    title = metadata.get("title", "unknown")
    media_type = metadata.get("type", "")

    log.info(f"Plex webhook: event={event} type={media_type} title={title}")

    # Only act on new media
    if event not in ("media.add", "library.new"):
        return {"status": "ignored", "event": event}

    metadata_key = metadata.get("key", "")

    # Resolve all file paths from this event
    file_paths = await _webhook_resolve_files(metadata, metadata_key, media_type)

    if not file_paths:
        log.warning(f"Webhook: no files resolved for [{title}] (type={media_type}, key={metadata_key})")
        return {"status": "skipped", "reason": "no files resolved"}

    # Prune old dedup entries
    now = time.time()
    _webhook_processed = {fp: ts for fp, ts in _webhook_processed.items() if now - ts < WEBHOOK_DEDUP_SECS}

    # Process each file
    results = []
    for fp in file_paths:
        # Skip recently processed files (dedup rapid webhook events)
        if fp in _webhook_processed:
            log.info(f"Webhook: {fp} recently processed, skipping (dedup)")
            results.append({"file": fp, "status": "skipped", "reason": "recently processed"})
            continue

        result = await _webhook_queue_file(fp)
        if result:
            results.append(result)
            if result.get("status") == "queued":
                _webhook_processed[fp] = now
            log.info(f"Webhook: {fp} -> {result.get('status')}: {result.get('reason', 'ok')}")

    queued = [r for r in results if r.get("status") == "queued"]
    if queued:
        log.info(f"Webhook: queued {len(queued)} files from event '{title}'")
        await manager.broadcast({"type": "state_update", "data": encode_queue.get_state()})

    if len(results) == 1:
        return results[0]
    return {"status": "processed", "files": results, "queued": len(queued)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        # Send initial state
        await ws.send_json({"type": "state_update", "data": encode_queue.get_state()})
        while True:
            # Keep connection alive, handle client messages
            data = await ws.receive_text()
            # Client can send ping
            if data == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)


class SuppressPollingFilter(logging.Filter):
    """Suppress access log lines for high-frequency polling endpoints."""
    SUPPRESSED = ("/api/system/stats", "/api/system/transcodes")
    def filter(self, record):
        msg = record.getMessage()
        return not any(ep in msg for ep in self.SUPPRESSED)


if __name__ == "__main__":
    logging.getLogger("uvicorn.access").addFilter(SuppressPollingFilter())
    uvicorn.run(app, host="0.0.0.0", port=9877, log_level="info")
