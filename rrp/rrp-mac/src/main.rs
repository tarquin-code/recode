use eframe::egui;
use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tray_icon::{TrayIconBuilder, menu::{Menu, MenuEvent, MenuItem}};
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "macos")]
#[macro_use]
extern crate objc;

static QUIT_FLAG: AtomicBool = AtomicBool::new(false);
static SHOW_FLAG: AtomicBool = AtomicBool::new(false);

const TRAY_FRAME_SIZE: usize = 22 * 22 * 4;
const TRAY_FRAME_COUNT: usize = 36;
const TRAY_FRAMES: &[u8] = include_bytes!("tray_frames.bin");
const TRAY_STATIC: &[u8] = include_bytes!("tray_icon.bin");

// Global tray handle (main thread only, wrapped in UnsafeCell)
static mut TRAY: Option<tray_icon::TrayIcon> = None;

fn update_tray_icon(frame_data: &[u8]) {
    unsafe {
        if let Some(ref tray) = TRAY {
            if let Ok(icon) = tray_icon::Icon::from_rgba(frame_data.to_vec(), 22, 22) {
                let _ = tray.set_icon(Some(icon));
            }
        }
    }
}

fn update_dock_progress(pct: f64) {
    unsafe {
        use cocoa::base::id;
        let app = cocoa::appkit::NSApp();
        if app == cocoa::base::nil { return; }
        let dock_tile: id = msg_send![app, dockTile];
        if dock_tile == cocoa::base::nil { return; }
        if pct > 0.0 && pct < 100.0 {
            let badge = format!("{:.0}%", pct);
            let badge_cstr = std::ffi::CString::new(badge).unwrap();
            let ns_badge: id = msg_send![class!(NSString), stringWithUTF8String: badge_cstr.as_ptr()];
            let _: () = msg_send![dock_tile, setBadgeLabel: ns_badge];
        } else {
            let _: () = msg_send![dock_tile, setBadgeLabel: cocoa::base::nil];
        }
    }
}

fn update_tray_tooltip(text: &str) {
    unsafe {
        if let Some(ref tray) = TRAY {
            let _ = tray.set_tooltip(Some(text));
        }
    }
}

// Persistent tray menu items that can be updated in-place without rebuilding
static mut TRAY_MENU_ITEMS: Option<TrayMenuItems> = None;

struct TrayMenuItems {
    status_item: tray_icon::menu::MenuItem,
    job_title: tray_icon::menu::MenuItem,
    job_name: tray_icon::menu::MenuItem,
    job_encoder: tray_icon::menu::MenuItem,
    job_progress: tray_icon::menu::MenuItem,
    job_speed: tray_icon::menu::MenuItem,
    job_output: tray_icon::menu::MenuItem,
    job_client: tray_icon::menu::MenuItem,
}

fn build_tray_menu(encoding: bool) {
    use tray_icon::menu::{Menu, MenuItem, PredefinedMenuItem};
    unsafe {
        if let Some(ref tray) = TRAY {
            let menu = Menu::new();
            let status_item = MenuItem::new("Recode GPU Server", false, None);
            let job_title = MenuItem::new(if encoding { "Encoding..." } else { "No active encodes" }, false, None);

            let _ = menu.append(&status_item);
            let _ = menu.append(&job_title);

            // Only add job detail items when encoding
            let (job_name, job_progress, job_speed, job_output, job_encoder, job_client);
            if encoding {
                let _ = menu.append(&PredefinedMenuItem::separator());
                job_name = MenuItem::new("", false, None);
                job_progress = MenuItem::new("", false, None);
                job_speed = MenuItem::new("", false, None);
                job_output = MenuItem::new("", false, None);
                job_encoder = MenuItem::new("", false, None);
                job_client = MenuItem::new("", false, None);
                let _ = menu.append(&job_name);
                let _ = menu.append(&job_progress);
                let _ = menu.append(&job_speed);
                let _ = menu.append(&job_output);
                let _ = menu.append(&job_encoder);
                let _ = menu.append(&job_client);
            } else {
                job_name = MenuItem::new("", false, None);
                job_progress = MenuItem::new("", false, None);
                job_speed = MenuItem::new("", false, None);
                job_output = MenuItem::new("", false, None);
                job_encoder = MenuItem::new("", false, None);
                job_client = MenuItem::new("", false, None);
            }

            let _ = menu.append(&PredefinedMenuItem::separator());
            let _ = menu.append(&MenuItem::with_id("show", "Show Window", true, None));
            let _ = menu.append(&MenuItem::with_id("quit", "Quit", true, None));

            let _ = tray.set_menu(Some(Box::new(menu)));

            TRAY_MENU_ITEMS = Some(TrayMenuItems {
                status_item, job_title, job_name, job_encoder,
                job_progress, job_speed, job_output,
                job_client,
            });
        }
    }
}

/// Color a tray menu item by index using NSAttributedString.
/// If text is empty, re-uses the item's existing title.
fn color_tray_item(index: isize, _text: &str, r: f64, g: f64, b: f64, bold: bool, size: f64) {
    unsafe {
        use cocoa::base::{nil, id};

        let status_bar: id = msg_send![class!(NSStatusBar), systemStatusBar];
        let status_items: id = msg_send![status_bar, statusItems];
        let count: usize = msg_send![status_items, count];
        if count == 0 { return; }

        let status_item: id = msg_send![status_items, objectAtIndex: count - 1];
        let menu: id = msg_send![status_item, menu];
        if menu == nil { return; }
        let item_count: isize = msg_send![menu, numberOfItems];
        if index >= item_count { return; }

        let ns_item: id = msg_send![menu, itemAtIndex: index];

        // Get existing title text
        let existing_title: id = msg_send![ns_item, title];
        if existing_title == nil { return; }

        let color: id = msg_send![class!(NSColor), colorWithRed:r green:g blue:b alpha:1.0_f64];
        let font: id = if bold {
            msg_send![class!(NSFont), boldSystemFontOfSize: size]
        } else {
            msg_send![class!(NSFont), systemFontOfSize: size]
        };

        let fg_key: id = msg_send![class!(NSString), stringWithUTF8String: b"NSColor\0".as_ptr()];
        let font_key: id = msg_send![class!(NSString), stringWithUTF8String: b"NSFont\0".as_ptr()];
        let keys = [fg_key, font_key];
        let vals = [color, font];
        let attrs: id = msg_send![class!(NSDictionary), dictionaryWithObjects:vals.as_ptr() forKeys:keys.as_ptr() count:2_usize];

        let attr_str: id = msg_send![class!(NSAttributedString), alloc];
        let attr_str: id = msg_send![attr_str, initWithString:existing_title attributes:attrs];
        let _: () = msg_send![ns_item, setAttributedTitle: attr_str];
    }
}

fn update_tray_menu_items(jobs: &[JobLog], transcodes: &[Transcode], connected: bool, running: bool) {
    unsafe {
        let items = match TRAY_MENU_ITEMS.as_ref() { Some(i) => i, None => return };

        // Header: app name + connection status
        let dot = if !running { "○" } else if connected { "●" } else { "◐" };
        let state = if !running { "Stopped" } else if connected { "Connected" } else { "Connecting…" };
        items.status_item.set_text(&format!("{} Recode — {}", dot, state));

        if let Some(t) = transcodes.first() {
            let job = jobs.iter().find(|j| !j.finished);

            if let Some(j) = job {
                let pct = if j.progress_pct > 0.0 { format!("{:.1}%", j.progress_pct) } else { "…".into() };
                let speed = if j.last_speed.is_empty() { "…".into() } else { j.last_speed.clone() };

                // Title: "⟳ Encoding 42.1% at 1.95x"
                items.job_title.set_text(&format!("⟳ Encoding {} at {}", pct, speed));

                // Filename
                let fname = if t.input.len() > 55 { format!("{}…", &t.input[..52]) } else { t.input.clone() };
                items.job_name.set_text(&fname);

                // Progress bar: ████████░░░░░░░░░░░░ (40 blocks = 2.5% each)
                if j.progress_pct > 0.0 {
                    let filled = (j.progress_pct / 2.5) as usize;
                    let empty = 40 - filled.min(40);
                    items.job_progress.set_text(&format!("{}{}  {}", "█".repeat(filled), "░".repeat(empty), pct));
                } else {
                    items.job_progress.set_text("Preparing…");
                }

                // Time: "1:24:30 / 2:16:18  ·  Elapsed 0:45:12"
                let elapsed = j.started.elapsed().as_secs();
                let elapsed_str = format!("{}:{:02}:{:02}", elapsed / 3600, (elapsed % 3600) / 60, elapsed % 60);
                if j.last_time_secs > 0.0 {
                    let fmt_time = |secs: f64| -> String {
                        let h = (secs / 3600.0) as u64;
                        let m = ((secs % 3600.0) / 60.0) as u64;
                        let s = (secs % 60.0) as u64;
                        if h > 0 { format!("{}:{:02}:{:02}", h, m, s) } else { format!("{}:{:02}", m, s) }
                    };
                    let pos = fmt_time(j.last_time_secs);
                    let total = if j.duration_secs > 0.0 { fmt_time(j.duration_secs) } else { "?".into() };
                    items.job_speed.set_text(&format!("{}  ·  {} / {}", speed, pos, total));
                } else {
                    items.job_speed.set_text(&format!("{}  ·  Elapsed {}", speed, elapsed_str));
                }

                // Output size
                items.job_output.set_text(&if !j.output_size.is_empty() {
                    format!("Output: {}  ·  Elapsed {}", j.output_size, elapsed_str)
                } else {
                    format!("Elapsed {}", elapsed_str)
                });

                // Position (hidden — info merged into speed line)

                // Encoder
                let codec = if t.video_codec.contains("videotoolbox") { "VideoToolbox HEVC" }
                    else if t.video_codec.contains("nvenc") { "NVENC HEVC" }
                    else { &t.video_codec };
                items.job_encoder.set_text(&format!("{}  ·  CPU {:.0}%", codec, t.cpu));

                // Client + CPU
                let host = if !j.client.is_empty() {
                    format!("Client: {}", j.client.split(':').next().unwrap_or(&j.client))
                } else { String::new() };
                let cpu_info = format!("CPU {:.0}%  ·  PID {}", t.cpu, t.pid);
                if host.is_empty() {
                    items.job_client.set_text(&cpu_info);
                } else {
                    items.job_client.set_text(&format!("{}  ·  {}", host, cpu_info));
                }

            } else {
                items.job_title.set_text(&format!("{} encode{} active", transcodes.len(), if transcodes.len() > 1 { "s" } else { "" }));
                items.job_name.set_text(&t.input);
                items.job_progress.set_text("");
                items.job_speed.set_text("");
                items.job_output.set_text("");
                items.job_encoder.set_text("");
                items.job_client.set_text("");
            }
        } else {
            items.job_title.set_text("No active encodes");
            items.job_name.set_text("");
            items.job_progress.set_text("");
            items.job_speed.set_text("");
            items.job_output.set_text("");
            items.job_encoder.set_text("");
            items.job_client.set_text("");
        }

    }
}
const VERSION: &str = "2.22.1";

// ── Recode Design System (exact CSS values) ────────────────────────────────
const BG_PRIMARY: egui::Color32 = egui::Color32::from_rgb(13, 17, 23);
const BG_SECONDARY: egui::Color32 = egui::Color32::from_rgb(22, 27, 34);
const BG_CARD: egui::Color32 = egui::Color32::from_rgb(28, 35, 51);
const BG_HOVER: egui::Color32 = egui::Color32::from_rgb(35, 42, 55);
const BG_INPUT: egui::Color32 = egui::Color32::from_rgb(21, 26, 33);
const BORDER: egui::Color32 = egui::Color32::from_rgb(48, 54, 61);
const TEXT_PRIMARY: egui::Color32 = egui::Color32::from_rgb(230, 237, 243);
const TEXT_SECONDARY: egui::Color32 = egui::Color32::from_rgb(139, 148, 158);
const TEXT_MUTED: egui::Color32 = egui::Color32::from_rgb(72, 78, 86);
const ACCENT: egui::Color32 = egui::Color32::from_rgb(0, 180, 216);
const SUCCESS: egui::Color32 = egui::Color32::from_rgb(63, 185, 80);
const ERROR: egui::Color32 = egui::Color32::from_rgb(248, 81, 73);
const WARNING: egui::Color32 = egui::Color32::from_rgb(210, 153, 34);
const PURPLE: egui::Color32 = egui::Color32::from_rgb(188, 140, 255);

fn alpha(c: egui::Color32, a: u8) -> egui::Color32 {
    egui::Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), a)
}

// ── Data Types ─────────────────────────────────────────────────────────────
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct Settings {
    address: String, secret: String, name: String, max_jobs: u32,
    ffmpeg_path: String, tmp_dir: String, auto_start: bool,
}
impl Settings {
    fn load() -> Self {
        std::fs::read_to_string(Self::path()).ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(Settings {
                address: String::new(), secret: String::new(),
                name: "Mac GPU".into(), max_jobs: 2, ffmpeg_path: find("ffmpeg"),
                tmp_dir: "/tmp/recode/rrp".into(), auto_start: false,
            })
    }
    fn save(&self) {
        let p = Self::path();
        let _ = std::fs::create_dir_all(std::path::Path::new(&p).parent().unwrap_or(std::path::Path::new(".")));
        let _ = serde_json::to_string_pretty(self).map(|j| std::fs::write(&p, j));
    }
    fn path() -> String {
        dirs::config_dir().map(|d| d.join("recode/settings.json").to_string_lossy().into()).unwrap_or("settings.json".into())
    }
}

fn find(name: &str) -> String {
    // Check app bundle first (tools bundled inside .app/Contents/MacOS/)
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let bundled = dir.join(name);
            if bundled.exists() { return bundled.to_string_lossy().into(); }
        }
    }
    let h = dirs::home_dir().unwrap_or_default();
    for p in [h.join("Recode/bin").join(name), format!("/usr/local/bin/{name}").into(), format!("/opt/homebrew/bin/{name}").into()] {
        if p.exists() { return p.to_string_lossy().into(); }
    }
    name.into()
}

#[derive(Clone, Default)]
struct ConnStatus { connected: bool, active_jobs: u32, error: String }

#[derive(Clone, Default)]
struct Transcode { input: String, video_codec: String, cpu: f32, pid: u64, client: String }

#[derive(Clone)]
struct JobLog {
    id: String,
    input: String,
    client: String,
    started: Instant,
    last_progress: String,
    last_speed: String,
    last_frame: u64,
    last_time_secs: f64,
    duration_secs: f64,
    progress_pct: f32,
    output_size: String,
    finished: bool,
    exit_ok: bool,
}

const GRAPH_HISTORY: usize = 120; // ~2 minutes at 1s intervals

#[derive(Clone, Default)]
struct SystemStats {
    cpu_percent: f32,
    gpu_percent: f32,
    gpu_mem_percent: f32,
    gpu_temp: f32,
    cpu_history: Vec<f32>,
    gpu_history: Vec<f32>,
    gpu_mem_history: Vec<f32>,
    gpu_temp_history: Vec<f32>,
}

#[derive(PartialEq, Clone, Copy)]
enum Panel { Encoding, Logs, Help, Settings }

// ── Main App ───────────────────────────────────────────────────────────────
struct App {
    settings: Settings, draft: Settings, child: Option<Child>, running: bool,
    status: Arc<Mutex<ConnStatus>>, logs: Arc<Mutex<Vec<String>>>,
    panel: Panel, tick: Instant, gpu_name: String, encoders: Vec<String>,
    started: bool, dirty: bool, transcodes: Vec<Transcode>, visible: bool,
    job_logs: Vec<JobLog>, known_jobs: std::collections::HashSet<String>,
    sys_stats: SystemStats, stats_tick: Instant,
    tray_frame: usize, tray_tick: Instant, tray_was_encoding: bool,
    tray_menu_hash: u64,
    fuse_installed: bool, fuse_checked: bool,
    update_available: Arc<Mutex<Option<(String, String)>>>, update_checked: bool,
}

/// Check GitHub for a newer version. Returns (new_version, dmg_url) or None.
fn check_for_update() -> Option<(String, String)> {
    let output = Command::new("curl")
        .args(["-sL", "--max-time", "10",
               "https://api.github.com/repos/tarquin-code/recode/releases/latest"])
        .output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&text).ok()?;
    let tag = json["tag_name"].as_str()?.trim_start_matches('v').to_string();

    // Compare versions
    let current_parts: Vec<u32> = VERSION.split('.').filter_map(|s| s.parse().ok()).collect();
    let remote_parts: Vec<u32> = tag.split('.').filter_map(|s| s.parse().ok()).collect();
    let is_newer = remote_parts.iter().zip(current_parts.iter())
        .find(|(r, c)| r != c)
        .map(|(r, c)| r > c)
        .unwrap_or(remote_parts.len() > current_parts.len());

    if !is_newer { return None; }

    // Find DMG asset URL
    let dmg_url = json["assets"].as_array()?.iter()
        .filter_map(|a| a["browser_download_url"].as_str())
        .find(|u| u.ends_with(".dmg"))
        .map(|u| u.to_string())
        .unwrap_or_else(|| json["html_url"].as_str().unwrap_or("").to_string());

    Some((tag, dmg_url))
}

fn download_and_install_update(dmg_url: &str) {
    let dmg_path = "/tmp/Recode-GPU-Server-update.dmg";
    // Download DMG
    let _ = Command::new("curl").args(["-sL", "-o", dmg_path, dmg_url]).output();
    // Mount DMG
    let _ = Command::new("hdiutil").args(["attach", dmg_path, "-nobrowse", "-quiet"]).output();
    // Find mounted volume and copy app
    if let Ok(o) = Command::new("ls").arg("/Volumes/Recode GPU Server/").output() {
        let text = String::from_utf8_lossy(&o.stdout);
        if text.contains("Recode GPU Server.app") {
            // Close current app, copy new one, reopen
            let app_path = if let Ok(exe) = std::env::current_exe() {
                exe.parent()
                    .and_then(|p| p.parent())
                    .and_then(|p| p.parent())
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default()
            } else { String::new() };
            if !app_path.is_empty() && app_path.ends_with(".app") {
                // Use a script that waits for us to exit, copies, and relaunches
                let script = format!(
                    "sleep 2; rm -rf '{}'; cp -a '/Volumes/Recode GPU Server/Recode GPU Server.app' '{}'; hdiutil detach '/Volumes/Recode GPU Server' -quiet; rm -f {}; open '{}'",
                    app_path, app_path, dmg_path, app_path
                );
                let _ = Command::new("bash").args(["-c", &script]).spawn();
                std::process::exit(0);
            }
        }
    }
    // Cleanup on failure
    let _ = Command::new("hdiutil").args(["detach", "/Volumes/Recode GPU Server", "-quiet"]).output();
    let _ = std::fs::remove_file(dmg_path);
}

fn check_macfuse_installed() -> bool {
    // Check for macFUSE filesystem bundle
    if std::path::Path::new("/Library/Filesystems/macfuse.fs").exists() {
        return true;
    }
    // Check for kernel extension
    if let Ok(o) = Command::new("kextstat").output() {
        if String::from_utf8_lossy(&o.stdout).contains("macfuse") {
            return true;
        }
    }
    false
}

fn install_macfuse() {
    // Download macFUSE .pkg and run installer
    let url = "https://github.com/osxfuse/osxfuse/releases/latest";
    // Try to find the latest .pkg URL
    if let Ok(o) = Command::new("curl").args(["-sL", "-o", "/dev/null", "-w", "%{url_effective}",
        "https://github.com/osxfuse/osxfuse/releases/latest"]).output() {
        let _redirect_url = String::from_utf8_lossy(&o.stdout).to_string();
    }
    // Simpler: use brew if available, otherwise open download page
    if let Ok(o) = Command::new("which").arg("brew").output() {
        if o.status.success() {
            // brew install macfuse
            let _ = Command::new("open").args(["-a", "Terminal",
                "bash -c 'echo Installing macFUSE...; brew install --cask macfuse; echo Done. Please reboot.; read'"
            ]).output();
            return;
        }
    }
    // Fallback: open the macFUSE releases page
    let _ = Command::new("open").arg(url).output();
}

/// Aggressive cleanup of all stale Recode processes and mounts.
/// Called on app startup to ensure a clean slate.
fn startup_cleanup() {
    // Kill any leftover recode-remote connectors
    let _ = Command::new("pkill").args(["-9", "-f", "recode-remote connect"]).output();
    // Kill any orphan ffmpeg processes from our tmp dir
    if let Ok(o) = Command::new("pgrep").args(["-f", "ffmpeg.*recode/rrp"]).output() {
        for pid in String::from_utf8_lossy(&o.stdout).lines() {
            let pid = pid.trim();
            if !pid.is_empty() {
                let _ = Command::new("kill").args(["-9", pid]).output();
            }
        }
    }
    // Wait for processes to die
    std::thread::sleep(Duration::from_secs(1));
    // Unmount ALL stale macFUSE mounts under /tmp/recode
    if let Ok(output) = Command::new("mount").output() {
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            if line.contains("macfuse") && line.contains("/tmp/recode") {
                if let Some(mp) = line.split(" on ").nth(1).and_then(|s| s.split(' ').next()) {
                    let _ = Command::new("diskutil").args(["unmount", "force", mp]).output();
                }
            }
        }
    }
    // Also try umount -f as fallback
    if let Ok(entries) = std::fs::read_dir("/tmp/recode/rrp") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.len() == 8 && name.chars().all(|c| c.is_ascii_hexdigit()) && entry.path().is_dir() {
                let mnt = entry.path().join("mnt");
                if mnt.exists() {
                    let _ = Command::new("umount").args(["-f", &mnt.to_string_lossy()]).output();
                }
            }
        }
    }
    std::thread::sleep(Duration::from_millis(500));
    // Remove all stale job dirs
    if let Ok(entries) = std::fs::read_dir("/tmp/recode/rrp") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.len() == 8 && name.chars().all(|c| c.is_ascii_hexdigit()) && entry.path().is_dir() {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
    // Kill any stuck diskutil/umount processes
    let _ = Command::new("pkill").args(["-9", "-f", "diskutil unmount.*recode"]).output();
    let _ = Command::new("pkill").args(["-9", "-f", "umount.*recode"]).output();
}

impl App {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        startup_cleanup();
        let s = Settings::load();
        Self {
            draft: s.clone(), settings: s, child: None, running: false,
            status: Arc::new(Mutex::new(ConnStatus::default())),
            logs: Arc::new(Mutex::new(Vec::new())), panel: Panel::Encoding,
            tick: Instant::now(), gpu_name: gpu_name(), encoders: Vec::new(),
            started: false, dirty: false, transcodes: Vec::new(), visible: true,
            job_logs: Vec::new(), known_jobs: std::collections::HashSet::new(),
            sys_stats: SystemStats::default(), stats_tick: Instant::now(),
            tray_frame: 0, tray_tick: Instant::now(), tray_was_encoding: false,
            tray_menu_hash: 0,
            fuse_installed: false, fuse_checked: false,
            update_available: Arc::new(Mutex::new(None)), update_checked: false,
        }
    }
    fn start(&mut self) {
        if self.running || self.settings.address.is_empty() || self.settings.secret.is_empty() { return; }
        if !self.fuse_installed {
            self.fuse_installed = check_macfuse_installed(); // re-check in case just installed
            if !self.fuse_installed {
                self.log("Cannot start — macFUSE is not installed");
                return;
            }
        }
        // Quick cleanup before starting
        let _ = Command::new("pkill").args(["-9", "-f", "recode-remote connect"]).output();
        std::thread::sleep(Duration::from_millis(300));
        self.job_logs.clear();
        self.known_jobs.clear();
        self.transcodes.clear();
        let _ = std::fs::create_dir_all(&self.settings.tmp_dir);
        let sf = format!("{}/connect-status-0.json", self.settings.tmp_dir);
        let mut cmd = Command::new(find("recode-remote"));
        cmd.args(["connect", "-a", &self.settings.address, "-s", &self.settings.secret,
            "--name", &self.settings.name, "--ffmpeg", &self.settings.ffmpeg_path,
            "--tmp-dir", &self.settings.tmp_dir, "--max-jobs", &self.settings.max_jobs.to_string(),
            "--status-file", &sf]).stdout(Stdio::piped()).stderr(Stdio::piped());
        match cmd.spawn() {
            Ok(mut c) => {
                let logs = self.logs.clone();
                let tmp_dir_clone = self.settings.tmp_dir.clone();
                if let Some(se) = c.stderr.take() {
                    std::thread::spawn(move || {
                        let log_path = format!("{}/connector.log", tmp_dir_clone);
                        let mut log_file = std::fs::File::create(&log_path).ok();
                        for l in std::io::BufReader::new(se).lines().flatten() {
                            if let Some(ref mut f) = log_file {
                                use std::io::Write;
                                let _ = writeln!(f, "{}", l);
                            }
                            // Clean up tracing prefix: "2026-03-28T... INFO rrp_app::connect: msg" → "HH:MM:SS msg"
                            let clean = if l.len() > 30 && l.chars().nth(4) == Some('-') {
                                // Extract time (chars 11..19) and message (after last ": " or after level)
                                let time_part = if l.len() > 19 { &l[11..19] } else { "" };
                                let msg_part = if let Some(pos) = l.find("]: ").or_else(|| l.find(":: ")).or_else(|| l.find(": ")) {
                                    // Skip past module path to get the actual message
                                    let after = &l[pos + 2..];
                                    if let Some(p2) = after.find(": ") { after[p2+2..].trim() } else { after.trim() }
                                } else { &l };
                                format!("{} {}", time_part, msg_part)
                            } else { l };
                            let mut lg = logs.lock().unwrap();
                            lg.push(clean); if lg.len() > 2000 { lg.drain(0..500); }
                        }
                    });
                }
                self.child = Some(c); self.running = true;
                self.log(&format!("Started: {}", find("recode-remote")));
                self.log("Connecting to encoding server...");
            }
            Err(e) => self.log(&format!("Failed to start: {e}")),
        }
    }
    fn stop(&mut self) {
        if let Some(mut c) = self.child.take() { let _ = c.kill(); let _ = c.wait(); }
        // Kill all related processes
        let _ = Command::new("pkill").args(["-f", "recode-remote connect"]).output();
        // Kill any ffmpeg processes in our tmp dir
        if let Ok(o) = Command::new("pgrep").args(["-f", "ffmpeg.*recode/rrp"]).output() {
            for pid in String::from_utf8_lossy(&o.stdout).lines() {
                let _ = Command::new("kill").args(["-9", pid.trim()]).output();
            }
        }
        // Unmount stale FUSE
        if let Ok(output) = Command::new("mount").output() {
            for line in String::from_utf8_lossy(&output.stdout).lines() {
                if line.contains("macfuse") && line.contains("/tmp/recode") {
                    if let Some(mp) = line.split(" on ").nth(1).and_then(|s| s.split(' ').next()) {
                        let _ = Command::new("diskutil").args(["unmount", "force", mp]).output();
                    }
                }
            }
        }
        self.running = false; *self.status.lock().unwrap() = ConnStatus::default();
        self.transcodes.clear(); self.log("Disconnected");
    }
    fn log(&self, m: &str) { self.logs.lock().unwrap().push(format!("{} {}", ts(), m)); }
    fn poll(&mut self) {
        if let Some(ref mut c) = self.child {
            if let Ok(Some(status)) = c.try_wait() {
                self.log(&format!("Connector exited ({})", status));
                self.running = false; self.child = None;
                *self.status.lock().unwrap() = ConnStatus::default(); return;
            }
        }
        if let Ok(d) = std::fs::read_to_string(format!("{}/connect-status-0.json", self.settings.tmp_dir)) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&d) {
                let mut s = self.status.lock().unwrap();
                s.connected = v["connected"].as_bool().unwrap_or(false);
                s.active_jobs = v["active_jobs"].as_u64().unwrap_or(0) as u32;
                s.error = v["error"].as_str().unwrap_or("").into();
            }
        }
        self.transcodes = scan_transcodes();
        self.poll_jobs();
        if self.stats_tick.elapsed() >= Duration::from_secs(1) {
            self.stats_tick = Instant::now();
            self.poll_system_stats();
        }
        if self.encoders.is_empty() {
            if let Ok(o) = Command::new(&self.settings.ffmpeg_path).args(["-hide_banner", "-encoders"]).output() {
                let out = String::from_utf8_lossy(&o.stdout);
                for n in ["hevc_videotoolbox", "h264_videotoolbox", "libx265", "libx264"] {
                    if out.contains(n) { self.encoders.push(n.into()); }
                }
            }
        }
    }
}

fn poll_cpu_percent() -> f32 {
    // macOS: use `ps -A -o %cpu` and sum all
    if let Ok(o) = Command::new("ps").args(["-A", "-o", "%cpu"]).output() {
        let text = String::from_utf8_lossy(&o.stdout);
        let total: f32 = text.lines().skip(1).filter_map(|l| l.trim().parse::<f32>().ok()).sum();
        let ncpu = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8) as f32;
        return (total / ncpu).min(100.0);
    }
    0.0
}

fn poll_gpu_stats() -> (f32, f32, f32) {
    // macOS Apple Silicon: use powermetrics or ioreg for GPU %, but these need root.
    // Simpler: parse `sudo powermetrics` or estimate from VideoToolbox activity.
    // For now, use ioreg to get GPU busy % from AGX
    // Fallback: if ffmpeg is running with videotoolbox, estimate GPU at ~80%
    let mut gpu_pct = 0.0_f32;
    let mut mem_pct = 0.0_f32;
    let mut temp = 0.0_f32;

    // Try ioreg for GPU utilization (Apple Silicon)
    if let Ok(o) = Command::new("ioreg").args(["-r", "-c", "AGXAccelerator", "-d", "1"]).output() {
        let text = String::from_utf8_lossy(&o.stdout);
        for line in text.lines() {
            if line.contains("\"gpu-utilization\"") || line.contains("\"Device Utilization %\"") {
                if let Some(num) = line.split('=').nth(1).and_then(|s| s.trim().parse::<f32>().ok()) {
                    gpu_pct = num.min(100.0);
                }
            }
        }
    }

    // Try getting temperature from powermetrics-compatible sources
    if let Ok(o) = Command::new("ioreg").args(["-r", "-c", "AppleARMSOCDevice", "-d", "1"]).output() {
        let text = String::from_utf8_lossy(&o.stdout);
        for line in text.lines() {
            if line.contains("\"temperature\"") || line.contains("\"die-temp\"") {
                if let Some(num) = line.split('=').nth(1).and_then(|s| s.trim().parse::<f32>().ok()) {
                    if num > 0.0 && num < 150.0 { temp = num; }
                }
            }
        }
    }

    // Estimate memory usage from encoding activity
    if let Ok(o) = Command::new("ps").args(["auxww"]).output() {
        let text = String::from_utf8_lossy(&o.stdout);
        let vt_active = text.lines().any(|l| l.contains("ffmpeg") && l.contains("videotoolbox") && !l.contains("_captest_"));
        if vt_active && gpu_pct < 10.0 { gpu_pct = 65.0; } // Estimate if ioreg doesn't report
        if vt_active { mem_pct = 30.0; } // Estimate VT memory usage
    }

    (gpu_pct, mem_pct, temp)
}

impl App {
    fn poll_system_stats(&mut self) {
        let cpu = poll_cpu_percent();
        let (gpu, mem, temp) = poll_gpu_stats();
        let s = &mut self.sys_stats;
        s.cpu_percent = cpu;
        s.gpu_percent = gpu;
        s.gpu_mem_percent = mem;
        s.gpu_temp = temp;
        s.cpu_history.push(cpu);
        s.gpu_history.push(gpu);
        s.gpu_mem_history.push(mem);
        s.gpu_temp_history.push(temp);
        if s.cpu_history.len() > GRAPH_HISTORY { s.cpu_history.remove(0); }
        if s.gpu_history.len() > GRAPH_HISTORY { s.gpu_history.remove(0); }
        if s.gpu_mem_history.len() > GRAPH_HISTORY { s.gpu_mem_history.remove(0); }
        if s.gpu_temp_history.len() > GRAPH_HISTORY { s.gpu_temp_history.remove(0); }
    }
}

/// Draw a utilization graph matching the web UI style: filled area under line, grid lines, legend.
fn draw_graph(
    ui: &mut egui::Ui, height: f32,
    lines: &[(&[f32], egui::Color32, &str)], // (history, color, label)
) {
    let avail_width = ui.available_width();
    let (rect, _) = ui.allocate_exact_size(egui::vec2(avail_width, height), egui::Sense::hover());
    let painter = ui.painter_at(rect);

    // Background
    painter.rect_filled(rect, 4.0, BG_SECONDARY);

    // Grid lines at 25%, 50%, 75%
    // Subtle grid lines at 25%, 50%, 75%
    let grid_color = egui::Color32::from_rgb(28, 33, 40); // slightly lighter than BG_SECONDARY (22,27,34)
    let grid_stroke = egui::Stroke::new(0.5, grid_color);
    for pct in [25.0, 50.0, 75.0] {
        let y = rect.top() + (1.0 - pct / 100.0) * rect.height();
        painter.line_segment([egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)], grid_stroke);
    }

    // Y-axis labels
    let label_color = egui::Color32::from_rgba_premultiplied(255, 255, 255, 25);
    let small_font = egui::FontId::proportional(9.0);
    painter.text(egui::pos2(rect.left() + 2.0, rect.top() + 1.0), egui::Align2::LEFT_TOP, "100%", small_font.clone(), label_color);
    painter.text(egui::pos2(rect.left() + 2.0, rect.center().y - 4.0), egui::Align2::LEFT_TOP, "50%", small_font.clone(), label_color);
    painter.text(egui::pos2(rect.left() + 2.0, rect.bottom() - 11.0), egui::Align2::LEFT_TOP, "0%", small_font, label_color);

    // Draw each line series
    let max_pts = GRAPH_HISTORY;
    for &(history, color, _label) in lines {
        if history.len() < 2 { continue; }
        let slice = if history.len() > max_pts { &history[history.len() - max_pts..] } else { history };
        let count = slice.len();
        let start_x = rect.right() - ((count - 1) as f32 / (max_pts - 1) as f32) * rect.width();

        // Build points
        let mut points: Vec<egui::Pos2> = Vec::with_capacity(count);
        for (i, &v) in slice.iter().enumerate() {
            let x = start_x + (i as f32 / (max_pts - 1) as f32) * rect.width();
            let y = rect.bottom() - (v.min(100.0) / 100.0) * rect.height();
            points.push(egui::pos2(x, y));
        }

        // Stroke line
        let stroke = egui::Stroke::new(1.5, color);
        for w in points.windows(2) {
            painter.line_segment([w[0], w[1]], stroke);
        }

        // Fill under line
        let fill_color = egui::Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 20);
        if points.len() >= 2 {
            let mut mesh = egui::Mesh::default();
            let base_y = rect.bottom();
            for i in 0..points.len() - 1 {
                let tl = points[i];
                let tr = points[i + 1];
                let bl = egui::pos2(tl.x, base_y);
                let br = egui::pos2(tr.x, base_y);
                let idx = mesh.vertices.len() as u32;
                mesh.colored_vertex(tl, fill_color);
                mesh.colored_vertex(tr, fill_color);
                mesh.colored_vertex(br, fill_color);
                mesh.colored_vertex(bl, fill_color);
                mesh.add_triangle(idx, idx + 1, idx + 2);
                mesh.add_triangle(idx, idx + 2, idx + 3);
            }
            painter.add(egui::Shape::mesh(mesh));
        }
    }

    // Legend (top-right)
    let legend_font = egui::FontId::proportional(10.0);
    let mut legend_y = rect.top() + 3.0;
    let legend_x = rect.right() - 80.0;
    for &(history, color, label) in lines {
        let val = history.last().copied().unwrap_or(0.0);
        let suffix = if label.contains("Temp") { "°C" } else { "%" };
        let text = format!("{}: {:.0}{}", label, val, suffix);
        painter.text(egui::pos2(legend_x, legend_y), egui::Align2::LEFT_TOP, text, legend_font.clone(), color);
        legend_y += 13.0;
    }
}

fn human_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 { format!("{:.2} GB", bytes as f64 / 1_073_741_824.0) }
    else if bytes >= 1_048_576 { format!("{:.1} MB", bytes as f64 / 1_048_576.0) }
    else { format!("{} KB", bytes / 1024) }
}

impl App {
    fn poll_jobs(&mut self) {
        let tmp = &self.settings.tmp_dir;
        let entries = match std::fs::read_dir(tmp) { Ok(e) => e, Err(_) => return };
        let mut active_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            // Job dirs are hex IDs (8 chars)
            if name.len() != 8 || !name.chars().all(|c| c.is_ascii_hexdigit()) { continue; }
            let job_dir = entry.path();
            if !job_dir.is_dir() { continue; }
            active_ids.insert(name.clone());

            let is_new = !self.known_jobs.contains(&name);
            if is_new {
                self.known_jobs.insert(name.clone());
                // Read input filename
                let input = std::fs::read_to_string(job_dir.join("rrp_input.txt"))
                    .unwrap_or_default().trim().to_string();
                let input_name = std::path::Path::new(&input).file_name()
                    .map(|f| f.to_string_lossy().to_string()).unwrap_or(input.clone());
                let client = std::fs::read_to_string(job_dir.join("rrp_client.txt"))
                    .unwrap_or_default().trim().to_string();

                self.log(&format!("Job {} started: {}", &name[..6], if input_name.is_empty() { "(unknown)" } else { &input_name }));
                if !client.is_empty() {
                    self.log(&format!("  Client: {}", client));
                }

                self.job_logs.push(JobLog {
                    id: name.clone(), input: input_name, client,
                    started: Instant::now(), last_progress: String::new(),
                    last_speed: String::new(), last_frame: 0, last_time_secs: 0.0,
                    duration_secs: 0.0, progress_pct: 0.0, output_size: String::new(),
                    finished: false, exit_ok: false,
                });
            }

            // Update progress from ffmpeg_progress.txt
            if let Some(job) = self.job_logs.iter_mut().find(|j| j.id == name && !j.finished) {
                if let Ok(progress) = std::fs::read_to_string(job_dir.join("ffmpeg_progress.txt")) {
                    let mut time_str = String::new();
                    let mut speed_str = String::new();
                    let mut frame: u64 = 0;
                    let mut done = false;
                    for line in progress.lines() {
                        if let Some(v) = line.strip_prefix("out_time=") { time_str = v.trim().to_string(); }
                        if let Some(v) = line.strip_prefix("speed=") { speed_str = v.trim().to_string(); }
                        if let Some(v) = line.strip_prefix("frame=") { frame = v.trim().parse().unwrap_or(0); }
                        if line.starts_with("progress=end") { done = true; }
                    }

                    // Parse out_time to seconds (format: HH:MM:SS.microseconds)
                    if !time_str.is_empty() {
                        let parts: Vec<&str> = time_str.split(':').collect();
                        if parts.len() == 3 {
                            let h: f64 = parts[0].parse().unwrap_or(0.0);
                            let m: f64 = parts[1].parse().unwrap_or(0.0);
                            let s: f64 = parts[2].parse().unwrap_or(0.0);
                            job.last_time_secs = h * 3600.0 + m * 60.0 + s;
                        }
                    }
                    // Read duration from rrp_duration.txt (written by recode-remote after FUSE mount)
                    if job.duration_secs <= 0.0 {
                        if let Ok(s) = std::fs::read_to_string(job_dir.join("rrp_duration.txt")) {
                            if let Ok(d) = s.trim().parse::<f64>() {
                                if d > 0.0 { job.duration_secs = d; }
                            }
                        }
                    }
                    if job.duration_secs > 0.0 && job.last_time_secs > 0.0 {
                        job.progress_pct = ((job.last_time_secs / job.duration_secs) * 100.0).min(100.0) as f32;
                    }
                    // Update output size
                    let out_size = std::fs::metadata(job_dir.join("output.mkv"))
                        .map(|m| m.len()).unwrap_or(0);
                    if out_size > 0 { job.output_size = human_size(out_size); }

                    // Log progress updates at milestones
                    if frame > 0 && frame != job.last_frame {
                        let new_progress = format!("{} @ {}", time_str, speed_str);
                        if new_progress != job.last_progress {
                            // Only log every ~5000 frames to avoid spam
                            if frame - job.last_frame >= 5000 || done {
                                // Check output file size
                                let out_size = std::fs::metadata(job_dir.join("output.mkv"))
                                    .map(|m| m.len()).unwrap_or(0);
                                let size_str = if out_size > 0 { human_size(out_size) } else { String::new() };
                                let elapsed = job.started.elapsed().as_secs();
                                let elapsed_str = format!("{}:{:02}:{:02}", elapsed / 3600, (elapsed % 3600) / 60, elapsed % 60);
                                self.logs.lock().unwrap().push(format!("{} Job {} | frame {} | {} | speed {} | {}{}",
                                    ts(), &name[..6], frame,
                                    if time_str.is_empty() { "-".into() } else { time_str.clone() },
                                    if speed_str.is_empty() { "-".into() } else { speed_str.clone() },
                                    elapsed_str,
                                    if size_str.is_empty() { String::new() } else { format!(" | {}", size_str) },
                                ));
                                job.last_frame = frame;
                            }
                            job.last_progress = new_progress;
                            job.last_speed = speed_str;
                        }
                    }

                    if done && !job.finished {
                        let out_size = std::fs::metadata(job_dir.join("output.mkv"))
                            .map(|m| m.len()).unwrap_or(0);
                        let elapsed = job.started.elapsed().as_secs();
                        let elapsed_str = format!("{}:{:02}:{:02}", elapsed / 3600, (elapsed % 3600) / 60, elapsed % 60);
                        job.finished = true;
                        job.exit_ok = true;
                        job.output_size = human_size(out_size);
                        self.logs.lock().unwrap().push(format!("{} Job {} COMPLETE | {} | {} | sending to server...",
                            ts(), &name[..6], elapsed_str, human_size(out_size)));
                    }
                }

                // Check stderr for errors
                let stderr_path = job_dir.join("ffmpeg_stderr.log");
                if let Ok(meta) = std::fs::metadata(&stderr_path) {
                    if meta.len() > 0 && !job.finished {
                        if let Ok(content) = std::fs::read_to_string(&stderr_path) {
                            let last_line = content.lines().last().unwrap_or("");
                            if !last_line.is_empty() && (last_line.contains("Error") || last_line.contains("error") || last_line.contains("FAIL")) {
                                self.logs.lock().unwrap().push(format!("{} Job {} ERROR: {}",
                                    ts(), &name[..6], last_line.chars().take(200).collect::<String>()));
                            }
                        }
                    }
                }
            }
        }

        // Detect completed/removed jobs (dir gone = job sent back and cleaned up)
        for job in self.job_logs.iter_mut().filter(|j| !j.finished) {
            if !active_ids.contains(&job.id) {
                job.finished = true;
                job.exit_ok = true;
                let elapsed = job.started.elapsed().as_secs();
                let elapsed_str = format!("{}:{:02}:{:02}", elapsed / 3600, (elapsed % 3600) / 60, elapsed % 60);
                self.logs.lock().unwrap().push(format!("{} Job {} done — sent back to server ({})",
                    ts(), &job.id[..6], elapsed_str));
            }
        }

        // Keep only last 50 job logs
        if self.job_logs.len() > 50 {
            self.job_logs.drain(0..self.job_logs.len() - 50);
        }
    }
}

fn kill_pid(pid: u64) {
    if pid == 0 { return; }
    // Kill ffmpeg
    let _ = std::process::Command::new("kill").args(["-9", &pid.to_string()]).output();
    // Wait 3s then unmount any FUSE mounts left by this job (in background)
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(3));
        // Find and unmount any FUSE mounts under /tmp/recode
        if let Ok(output) = std::process::Command::new("mount").output() {
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                if line.contains("macfuse") && line.contains("/tmp/recode") {
                    if let Some(mount_point) = line.split(" on ").nth(1).and_then(|s| s.split(' ').next()) {
                        let _ = std::process::Command::new("diskutil").args(["unmount", "force", mount_point]).output();
                    }
                }
            }
        }
    });
}

fn gpu_name() -> String {
    Command::new("system_profiler").args(["SPDisplaysDataType"]).output().ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).lines()
            .find(|l| l.contains("Chipset Model:"))
            .map(|l| l.split(':').nth(1).unwrap_or("Apple GPU").trim().into()))
        .unwrap_or("Apple GPU".into())
}

fn scan_transcodes() -> Vec<Transcode> {
    let mut r = Vec::new();
    if let Ok(o) = Command::new("ps").args(["auxww"]).output() {
        for line in String::from_utf8_lossy(&o.stdout).lines() {
            let parts: Vec<&str> = line.split_whitespace().collect(); let p: Vec<&str> = if parts.len() >= 11 { let cmd_start = line.find(parts[10]).unwrap_or(0); let mut v: Vec<&str> = parts[..10].to_vec(); v.push(&line[cmd_start..]); v } else { continue; };
            if p.len() < 11 || !p[10].contains("ffmpeg") || !p[10].contains("-i ") { continue; }
            if p[10].contains("_captest_") || p[10].contains("-encoders") { continue; }
            let pid: u64 = p[1].parse().unwrap_or(0);
            let cpu: f32 = p[2].parse().unwrap_or(0.0);
            let codec: String = p[10].split("-c:v ").nth(1).and_then(|s| s.split(' ').next()).unwrap_or("").into();

            // Extract job dir from ffmpeg's -i path (e.g. /tmp/recode/rrp/72148362/mnt/input_0.mkv)
            let input_path = p[10].split("-i ").nth(1).and_then(|s| s.split(' ').next()).unwrap_or("").trim_matches('"');
            let job_dir = std::path::Path::new(input_path).parent().and_then(|p| p.parent());

            // Read original filename and client from job dir files
            let fname = job_dir.and_then(|d| std::fs::read_to_string(d.join("rrp_input.txt")).ok())
                .map(|s| { let s = s.trim().to_string(); std::path::Path::new(&s).file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or(s) })
                .unwrap_or_else(|| std::path::Path::new(input_path).file_name().map(|f| f.to_string_lossy().into()).unwrap_or(input_path.into()));

            let client = job_dir.and_then(|d| std::fs::read_to_string(d.join("rrp_client.txt")).ok())
                .map(|s| s.trim().to_string()).unwrap_or_default();
            r.push(Transcode { input: fname, video_codec: codec, cpu, pid, client });
        }
    }
    r
}

fn get_rrp_env(pid: u64, var: &str) -> Option<String> {
    // Read from /proc on Linux, or ps eww on macOS
    // On macOS, env vars with spaces need careful parsing
    let output = Command::new("ps").args(["eww", "-p", &pid.to_string()]).output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let needle = format!(" {}=", var);
    if let Some(pos) = text.find(&needle) {
        let val_start = pos + needle.len();
        // Value ends at next env var (space + uppercase + =) or end of string
        let rest = &text[val_start..];
        let val_end = rest.find(" RRP_").or_else(|| rest.find(" HOME="))
            .or_else(|| rest.find(" PATH=")).or_else(|| rest.find(" USER="))
            .or_else(|| rest.find("\n")).unwrap_or(rest.len());
        let val = rest[..val_end].trim();
        if !val.is_empty() {
            return Some(val.to_string());
        }
    }
    None
}

fn ts() -> String {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    format!("{:02}:{:02}:{:02}", (d/3600)%24, (d/60)%60, d%60)
}

// ── UI Drawing ─────────────────────────────────────────────────────────────
fn badge(ui: &mut egui::Ui, text: &str, color: egui::Color32) {
    let font = egui::FontId::proportional(10.0);
    let galley = ui.painter().layout_no_wrap(text.into(), font, color);
    let size = galley.size() + egui::vec2(12.0, 6.0);
    let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
    ui.painter().rect_filled(rect, 4.0, alpha(color, 50));
    ui.painter().galley(rect.min + egui::vec2(6.0, 3.0), galley, color);
}

fn status_dot(ui: &mut egui::Ui, color: egui::Color32, label: &str) {
    ui.horizontal(|ui| {
        ui.spacing_mut().item_spacing.x = 4.0;
        let (rect, _) = ui.allocate_exact_size(egui::vec2(8.0, 8.0), egui::Sense::hover());
        ui.painter().circle_filled(rect.center(), 4.0, color);
        // Glow effect
        ui.painter().circle_filled(rect.center(), 6.0, alpha(color, 20));
        ui.label(egui::RichText::new(label).color(TEXT_MUTED).size(10.0));
    });
}

fn section_label(ui: &mut egui::Ui, text: &str) {
    ui.add_space(4.0);
    ui.label(egui::RichText::new(text).color(TEXT_MUTED).size(9.0).strong());
    ui.add_space(2.0);
}

fn card_begin(ui: &mut egui::Ui) -> egui::InnerResponse<()> {
    egui::Frame::new()
        .fill(BG_CARD)
        .stroke(egui::Stroke::new(1.0, BORDER))
        .corner_radius(10.0)
        .inner_margin(egui::Margin::same(14))
        .outer_margin(egui::Margin::symmetric(0, 2))
        .show(ui, |_ui| {})
}

const BORDER_SUBTLE: egui::Color32 = egui::Color32::from_rgb(35, 40, 48);

macro_rules! card {
    ($ui:expr, $body:expr) => {
        egui::Frame::new()
            .fill(BG_CARD)
            .stroke(egui::Stroke::new(0.5, BORDER_SUBTLE))
            .corner_radius(10.0)
            .inner_margin(egui::Margin::same(14))
            .outer_margin(egui::Margin::symmetric(0, 2))
            .show($ui, $body);
    };
}

/// Draw the Recode logo (circular arrow matching the web favicon SVG) at the given position.
/// SVG: <path d="M21 12a9 9 0 1 1-6.219-8.56"/><polyline points="21 3 21 9 15 9"/>
fn paint_recode_logo(painter: &egui::Painter, center: egui::Pos2, radius: f32, stroke: egui::Stroke) {
    // Arc: from 0deg (3 o'clock) going clockwise ~305 degrees
    // Gap is roughly from -55deg to 0deg (top-right area)
    let steps = 80;
    let start = 0.0_f32; // radians, 0 = right
    let sweep = 305.0_f32.to_radians();
    for i in 0..steps {
        let t0 = i as f32 / steps as f32;
        let t1 = (i + 1) as f32 / steps as f32;
        let a0 = start - t0 * sweep; // negative = clockwise in screen coords
        let a1 = start - t1 * sweep;
        let p0 = center + egui::vec2(radius * a0.cos(), -radius * a0.sin());
        let p1 = center + egui::vec2(radius * a1.cos(), -radius * a1.sin());
        painter.line_segment([p0, p1], stroke);
    }
    // Arrowhead: SVG polyline (21,3)→(21,9)→(15,9) in viewBox 0 0 24 24
    // Map to our center/radius coordinate system: center=(12,12), radius maps to 9
    let s = radius / 9.0;
    let ox = center.x - 12.0 * s;
    let oy = center.y - 12.0 * s;
    let p = |x: f32, y: f32| egui::pos2(ox + x * s, oy + y * s);
    painter.line_segment([p(21.0, 3.0), p(21.0, 9.0)], stroke);
    painter.line_segment([p(21.0, 9.0), p(15.0, 9.0)], stroke);
}

fn icon_tab(ui: &mut egui::Ui, icon: &str, active: bool, tooltip: &str) -> bool {
    let size = egui::vec2(32.0, 28.0);
    let (rect, resp) = ui.allocate_exact_size(size, egui::Sense::click());
    let color = if active { ACCENT } else if resp.hovered() { TEXT_PRIMARY } else { TEXT_MUTED };

    // Draw icon character
    let font = egui::FontId::proportional(16.0);
    let galley = ui.painter().layout_no_wrap(icon.into(), font, color);
    let text_pos = rect.center() - galley.size() / 2.0;
    ui.painter().galley(text_pos, galley, color);

    // Active underline
    if active {
        ui.painter().line_segment(
            [rect.left_bottom() + egui::vec2(2.0, 0.0), rect.right_bottom() - egui::vec2(2.0, 0.0)],
            egui::Stroke::new(2.0, ACCENT),
        );
    }

    resp.clone().on_hover_text(tooltip);
    resp.clicked()
}

fn text_tab(ui: &mut egui::Ui, label: &str, active: bool, count: Option<usize>) -> bool {
    let (rect, resp) = ui.allocate_exact_size(egui::vec2(80.0, 28.0), egui::Sense::click());
    let color = if active { TEXT_PRIMARY } else if resp.hovered() { TEXT_SECONDARY } else { TEXT_MUTED };

    let font = egui::FontId::proportional(12.0);
    let galley = ui.painter().layout_no_wrap(label.into(), font, color);
    let text_x = rect.min.x + 8.0;
    let text_y = rect.center().y - galley.size().y / 2.0;
    ui.painter().galley(egui::pos2(text_x, text_y), galley.clone(), color);

    // Badge count
    if let Some(n) = count {
        if n > 0 {
            let badge_text = n.to_string();
            let bf = egui::FontId::proportional(9.0);
            let bg = ui.painter().layout_no_wrap(badge_text.clone(), bf.clone(), egui::Color32::WHITE);
            let bw = bg.size().x + 8.0;
            let bh = 14.0;
            let bx = text_x + galley.size().x + 6.0;  // Use galley width instead of rect
            let by = rect.center().y - bh / 2.0;
            let brect = egui::Rect::from_min_size(egui::pos2(bx, by), egui::vec2(bw, bh));
            ui.painter().rect_filled(brect, 7.0, WARNING);
            ui.painter().galley(brect.min + egui::vec2(4.0, 2.0), bg, egui::Color32::WHITE);
        }
    }

    // Active underline
    if active {
        ui.painter().line_segment(
            [rect.left_bottom(), rect.right_bottom()],
            egui::Stroke::new(2.0, ACCENT),
        );
    }

    resp.clicked()
}

// ── eframe::App ────────────────────────────────────────────────────────────
impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Hide window on close instead of quitting (keeps running in menu bar)
        if ctx.input(|i| i.viewport().close_requested()) {
            ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(false));
            self.visible = false;
        }
        // Tray "Show Window" clicked
        if SHOW_FLAG.swap(false, Ordering::Relaxed) {
            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
            ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
            self.visible = true;
        }
        // Startup checks (first frame only)
        if !self.fuse_checked {
            self.fuse_checked = true;
            self.fuse_installed = check_macfuse_installed();
            if !self.fuse_installed {
                self.log("WARNING: macFUSE is not installed — encoding will not work");
            }
        }
        if !self.update_checked {
            self.update_checked = true;
            let logs = self.logs.clone();
            let update_result = self.update_available.clone();
            std::thread::spawn(move || {
                if let Some((ver, url)) = check_for_update() {
                    logs.lock().unwrap().push(format!("{} Update available: v{}", ts(), ver));
                    *update_result.lock().unwrap() = Some((ver, url));
                }
            });
        }
        if self.settings.auto_start && !self.started { self.started = true; self.start(); }
        // Repaint frequently when encoding (for tray animation), otherwise every 1s
        let encoding = !self.transcodes.is_empty();
        ctx.request_repaint_after(if encoding { Duration::from_millis(50) } else { Duration::from_secs(1) });

        let poll_interval = if self.transcodes.is_empty() { 2 } else { 1 };
        if self.tick.elapsed() > Duration::from_secs(poll_interval) {
            self.poll(); self.tick = Instant::now();
        }

        // Tray icon animation
        if encoding && self.tray_tick.elapsed() >= Duration::from_millis(50) {
            self.tray_tick = Instant::now();
            let start = self.tray_frame * TRAY_FRAME_SIZE;
            let end = start + TRAY_FRAME_SIZE;
            if end <= TRAY_FRAMES.len() {
                update_tray_icon(&TRAY_FRAMES[start..end]);
            }
            self.tray_frame = (self.tray_frame + 1) % TRAY_FRAME_COUNT;
            // Update tooltip, dock progress, and menu with job info
            if let Some(j) = self.job_logs.iter().find(|j| !j.finished) {
                let pct = if j.progress_pct > 0.0 { format!("{:.0}%", j.progress_pct) } else { "...".into() };
                update_tray_tooltip(&format!("Encoding: {} — {} {}", j.input, pct, j.last_speed));
                update_dock_progress(j.progress_pct as f64);
            }
            // Update menu items in-place every frame (doesn't close the open menu)
            if self.tray_frame % 5 == 0 {
                let status = self.status.lock().unwrap().clone();
                update_tray_menu_items(&self.job_logs, &self.transcodes, status.connected, self.running);
            }
            if !self.tray_was_encoding {
                // Transition to encoding — rebuild menu with job detail rows
                build_tray_menu(true);
            }
            self.tray_was_encoding = true;
        } else if !encoding && self.tray_was_encoding {
            update_tray_icon(TRAY_STATIC);
            update_tray_tooltip("Recode GPU Server — Idle");
            update_dock_progress(0.0);
            // Transition to idle — rebuild menu without job detail rows
            build_tray_menu(false);
            let status = self.status.lock().unwrap().clone();
            update_tray_menu_items(&self.job_logs, &self.transcodes, status.connected, self.running);
            self.tray_was_encoding = false;
        }

        // ── Global Style ──
        let mut style = (*ctx.style()).clone();
        style.visuals.override_text_color = Some(TEXT_PRIMARY);
        style.visuals.window_fill = BG_PRIMARY;
        style.visuals.panel_fill = BG_PRIMARY;
        style.visuals.extreme_bg_color = BG_INPUT;
        style.visuals.widgets.inactive.bg_fill = BG_INPUT;
        style.visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, TEXT_SECONDARY);
        style.visuals.widgets.inactive.weak_bg_fill = BG_INPUT;
        style.visuals.widgets.inactive.bg_stroke = egui::Stroke::new(0.5, BORDER_SUBTLE);
        style.visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(0.5, BORDER_SUBTLE);
        style.visuals.widgets.hovered.bg_fill = BG_HOVER;
        style.visuals.widgets.hovered.weak_bg_fill = BG_HOVER;
        style.visuals.widgets.active.bg_fill = ACCENT;
        style.visuals.selection.bg_fill = alpha(ACCENT, 40);
        style.visuals.selection.stroke = egui::Stroke::new(1.0, ACCENT);
        style.visuals.window_stroke = egui::Stroke::new(0.5, BORDER_SUBTLE);
        style.spacing.item_spacing = egui::vec2(8.0, 6.0);
        style.visuals.widgets.noninteractive.corner_radius = egui::CornerRadius::same(6);
        style.visuals.widgets.inactive.corner_radius = egui::CornerRadius::same(6);
        style.visuals.widgets.hovered.corner_radius = egui::CornerRadius::same(6);
        style.visuals.widgets.active.corner_radius = egui::CornerRadius::same(6);
        ctx.set_style(style);

        let status = self.status.lock().unwrap().clone();

        // ═══ HEADER ═══
        egui::TopBottomPanel::top("header").frame(
            egui::Frame::new().fill(BG_SECONDARY)
                .stroke(egui::Stroke::new(0.5, BORDER_SUBTLE))
                .inner_margin(egui::Margin::symmetric(16, 10))
        ).show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.spacing_mut().item_spacing.x = 6.0;
                // Recode logo icon (circular arrow)
                let (logo_rect, _) = ui.allocate_exact_size(egui::vec2(18.0, 18.0), egui::Sense::hover());
                paint_recode_logo(ui.painter(), logo_rect.center(), 7.0, egui::Stroke::new(1.8, ACCENT));
                ui.label(egui::RichText::new("Recode GPU Server").color(egui::Color32::WHITE).size(14.0));
                ui.label(egui::RichText::new(format!("v{VERSION}")).color(TEXT_MUTED).size(10.0));

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.spacing_mut().item_spacing.x = 12.0;
                    let conn_color = if !self.running { TEXT_MUTED } else if status.connected { SUCCESS } else { WARNING };
                    let conn_label = if !self.running { "Off" } else if status.connected { "Connected" } else { "Connecting" };
                    status_dot(ui, conn_color, conn_label);
                    status_dot(ui, SUCCESS, "GPU");
                    let ffmpeg_ok = std::path::Path::new(&self.settings.ffmpeg_path).exists();
                    status_dot(ui, if ffmpeg_ok { SUCCESS } else { ERROR }, "ffmpeg");
                });
            });
        });

        // ═══ TAB BAR ═══
        egui::TopBottomPanel::top("tabs").frame(
            egui::Frame::new().fill(BG_PRIMARY)
                .stroke(egui::Stroke::new(0.5, BORDER_SUBTLE))
                .inner_margin(egui::Margin::symmetric(12, 0))
        ).show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.spacing_mut().item_spacing.x = 0.0;
                let enc_count = if self.transcodes.is_empty() { None } else { Some(self.transcodes.len()) };
                if text_tab(ui, "Encoding", self.panel == Panel::Encoding, enc_count) { self.panel = Panel::Encoding; }

                // Spacer
                let avail = ui.available_width() - 110.0;
                if avail > 0.0 { ui.add_space(avail); }

                // Icon tabs (right side)
                if icon_tab(ui, "\u{1F4C4}", self.panel == Panel::Logs, "Logs") { self.panel = Panel::Logs; }
                if icon_tab(ui, "?", self.panel == Panel::Help, "Help") { self.panel = Panel::Help; }
                if icon_tab(ui, "\u{2699}", self.panel == Panel::Settings, "Settings") { self.panel = Panel::Settings; }
            });
        });

        // ═══ BOTTOM BAR ═══
        egui::TopBottomPanel::bottom("bottom").frame(
            egui::Frame::new().fill(BG_SECONDARY)
                .stroke(egui::Stroke::new(0.5, BORDER_SUBTLE))
                .inner_margin(egui::Margin::symmetric(16, 8))
        ).show(ctx, |ui| {
            ui.horizontal(|ui| {
                if self.running {
                    if ui.add(egui::Button::new(egui::RichText::new("  Stop  ").color(egui::Color32::WHITE).size(11.0))
                        .fill(ERROR).corner_radius(6.0).min_size(egui::vec2(60.0, 28.0))).clicked() { self.stop(); }
                } else {
                    let ok = !self.settings.address.is_empty() && !self.settings.secret.is_empty();
                    if ui.add_enabled(ok, egui::Button::new(egui::RichText::new("  Start  ").color(egui::Color32::WHITE).size(11.0))
                        .fill(SUCCESS).corner_radius(6.0).min_size(egui::vec2(60.0, 28.0))).clicked() { self.start(); }
                    if !ok {
                        ui.label(egui::RichText::new("Configure address and secret in Settings").color(WARNING).size(10.0));
                    }
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(egui::RichText::new(&self.gpu_name).color(TEXT_MUTED).size(10.0));
                    if self.running && status.connected {
                        ui.label(egui::RichText::new(format!("\u{2022} {} active", status.active_jobs)).color(TEXT_MUTED).size(10.0));
                    }
                });
            });
        });

        // ═══ MAIN CONTENT ═══
        egui::CentralPanel::default().frame(
            egui::Frame::new().fill(BG_PRIMARY).inner_margin(egui::Margin::same(16))
        ).show(ctx, |ui| {
            // macFUSE warning banner
            if !self.fuse_installed {
                let banner = egui::Frame::new()
                    .fill(egui::Color32::from_rgb(60, 30, 10))
                    .stroke(egui::Stroke::new(1.0, WARNING))
                    .corner_radius(6.0)
                    .inner_margin(egui::Margin::symmetric(12, 8));
                banner.show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new("⚠").color(WARNING).size(16.0));
                        ui.vertical(|ui| {
                            ui.label(egui::RichText::new("macFUSE is not installed").color(WARNING).size(12.0).strong());
                            ui.label(egui::RichText::new("macFUSE is required for FUSE file streaming. Encoding will not work without it.").color(TEXT_SECONDARY).size(10.0));
                        });
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.add(egui::Button::new(egui::RichText::new("Install macFUSE").color(egui::Color32::WHITE).size(11.0))
                                .fill(WARNING).corner_radius(4.0)).clicked() {
                                install_macfuse();
                            }
                            if ui.add(egui::Button::new(egui::RichText::new("Re-check").color(TEXT_SECONDARY).size(10.0))
                                .corner_radius(4.0)).clicked() {
                                self.fuse_installed = check_macfuse_installed();
                            }
                        });
                    });
                });
                ui.add_space(8.0);
            }
            // Update available banner
            let update_info = self.update_available.lock().unwrap().clone();
            if let Some((ref ver, ref url)) = update_info {
                let banner = egui::Frame::new()
                    .fill(egui::Color32::from_rgb(15, 40, 60))
                    .stroke(egui::Stroke::new(1.0, ACCENT))
                    .corner_radius(6.0)
                    .inner_margin(egui::Margin::symmetric(12, 8));
                banner.show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new("⬆").color(ACCENT).size(16.0));
                        ui.vertical(|ui| {
                            ui.label(egui::RichText::new(format!("Update available: v{}", ver)).color(ACCENT).size(12.0).strong());
                            ui.label(egui::RichText::new(format!("Current: v{}", VERSION)).color(TEXT_MUTED).size(10.0));
                        });
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let url_clone = url.clone();
                            if ui.add(egui::Button::new(egui::RichText::new("Update Now").color(egui::Color32::WHITE).size(11.0))
                                .fill(ACCENT).corner_radius(4.0)).clicked() {
                                download_and_install_update(&url_clone);
                            }
                            if ui.add(egui::Button::new(egui::RichText::new("Later").color(TEXT_MUTED).size(10.0))
                                .corner_radius(4.0)).clicked() {
                                *self.update_available.lock().unwrap() = None;
                            }
                        });
                    });
                });
                ui.add_space(8.0);
            }
            egui::ScrollArea::vertical().show(ui, |ui| {
                match self.panel {
                    Panel::Encoding => self.ui_encoding(ui, &status),
                    Panel::Logs => self.ui_logs(ui),
                    Panel::Help => self.ui_help(ui),
                    Panel::Settings => self.ui_settings(ui),
                }
            });
        });
    }
}

// ── Panel Renderers ────────────────────────────────────────────────────────
impl App {
    fn ui_encoding(&mut self, ui: &mut egui::Ui, status: &ConnStatus) {
        // "Connected GPUs" heading
        ui.label(egui::RichText::new("Connected GPUs").color(TEXT_SECONDARY).size(11.0).strong());
        ui.add_space(2.0);

        // GPU card — matching web UI's Connected GPUs row
        let gpu_frame = egui::Frame::new()
            .fill(egui::Color32::from_rgb(21, 26, 33)) // bg-tertiary
            .stroke(egui::Stroke::new(0.5, BORDER_SUBTLE))
            .corner_radius(6.0)
            .inner_margin(egui::Margin::symmetric(10, 6))
            .outer_margin(egui::Margin::symmetric(0, 1));
        gpu_frame.show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.spacing_mut().item_spacing.x = 5.0;
                // Connection status dot
                let conn_color = if !self.running { TEXT_MUTED } else if status.connected { SUCCESS } else { WARNING };
                let (dot_rect, _) = ui.allocate_exact_size(egui::vec2(8.0, 8.0), egui::Sense::hover());
                ui.painter().circle_filled(dot_rect.center(), 4.0, conn_color);
                // GPU name
                ui.label(egui::RichText::new(&self.gpu_name).color(TEXT_PRIMARY).size(12.0).strong());
                // Server address
                if !self.settings.address.is_empty() {
                    ui.label(egui::RichText::new(&self.settings.address).color(TEXT_MUTED).size(11.0).font(egui::FontId::monospace(11.0)));
                }
                // Encoder badges: "H.265 videotoolbox, cpu" and "H.264 videotoolbox, cpu"
                let h265: Vec<_> = self.encoders.iter().filter(|e| e.contains("hevc") || e.contains("x265")).collect();
                let h264: Vec<_> = self.encoders.iter().filter(|e| e.contains("h264") || e.contains("x264")).collect();
                if !h265.is_empty() {
                    let names: Vec<_> = h265.iter().map(|e| {
                        e.replace("hevc_", "").replace("libx265", "cpu")
                    }).collect();
                    badge(ui, &format!("H.265 {}", names.join(", ")), ACCENT);
                }
                if !h264.is_empty() {
                    let names: Vec<_> = h264.iter().map(|e| {
                        e.replace("h264_", "").replace("libx264", "cpu")
                    }).collect();
                    badge(ui, &format!("H.264 {}", names.join(", ")), SUCCESS);
                }
                // Capability badges: "1080p 10bit HDR" and "4K 10bit HDR"
                // All pass on Apple Silicon VideoToolbox
                let cap_color = TEXT_MUTED;
                let ok_color = SUCCESS;
                // 1080p capabilities badge
                {
                    let font = egui::FontId::proportional(10.0);
                    let texts = [("1080p", true), (" 10bit", true), (" HDR", true)];
                    let mut total_w = 12.0_f32; // padding
                    let galleys: Vec<_> = texts.iter().map(|(t, _)| {
                        let g = ui.painter().layout_no_wrap((*t).into(), font.clone(), ok_color);
                        total_w += g.size().x;
                        g
                    }).collect();
                    let size = egui::vec2(total_w, 18.0);
                    let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
                    ui.painter().rect_filled(rect, 4.0, alpha(cap_color, 45));
                    let mut x = rect.left() + 6.0;
                    for (i, g) in galleys.into_iter().enumerate() {
                        let c = if texts[i].1 { ok_color } else { ERROR };
                        ui.painter().galley(egui::pos2(x, rect.top() + 3.0), g, c);
                        x += ui.painter().layout_no_wrap(texts[i].0.into(), font.clone(), c).size().x;
                    }
                }
                // 4K capabilities badge
                {
                    let font = egui::FontId::proportional(10.0);
                    let texts = [("4K", true), (" 10bit", true), (" HDR", true)];
                    let mut total_w = 12.0_f32;
                    let galleys: Vec<_> = texts.iter().map(|(t, _)| {
                        let g = ui.painter().layout_no_wrap((*t).into(), font.clone(), ok_color);
                        total_w += g.size().x;
                        g
                    }).collect();
                    let size = egui::vec2(total_w, 18.0);
                    let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
                    ui.painter().rect_filled(rect, 4.0, alpha(cap_color, 45));
                    let mut x = rect.left() + 6.0;
                    for (i, g) in galleys.into_iter().enumerate() {
                        let c = if texts[i].1 { ok_color } else { ERROR };
                        ui.painter().galley(egui::pos2(x, rect.top() + 3.0), g, c);
                        x += ui.painter().layout_no_wrap(texts[i].0.into(), font.clone(), c).size().x;
                    }
                }
                if self.encoders.is_empty() && self.running {
                    ui.label(egui::RichText::new("Detecting...").color(WARNING).size(10.0));
                }
                // Jobs count (right-aligned)
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let active = self.transcodes.len();
                    ui.label(egui::RichText::new(format!("{}/{} jobs", active, self.settings.max_jobs)).color(TEXT_MUTED).size(11.0));
                });
            });
        });

        // System graphs (matching web UI style)
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            // CPU graph (1/3 width)
            let cpu_width = (ui.available_width() - 8.0) * 0.33;
            ui.vertical(|ui| {
                ui.set_width(cpu_width);
                ui.label(egui::RichText::new("CPU").color(TEXT_MUTED).size(9.0));
                let cpu_color = egui::Color32::from_rgb(79, 195, 247); // light blue
                draw_graph(ui, 80.0, &[
                    (&self.sys_stats.cpu_history, cpu_color, "CPU"),
                ]);
            });
            ui.add_space(8.0);
            // GPU graph (2/3 width)
            ui.vertical(|ui| {
                ui.label(egui::RichText::new("GPU").color(TEXT_MUTED).size(9.0));
                let gpu_color = egui::Color32::from_rgb(102, 187, 106); // green
                let temp_color = egui::Color32::from_rgb(255, 112, 67); // orange
                if self.sys_stats.gpu_temp > 0.0 {
                    draw_graph(ui, 80.0, &[
                        (&self.sys_stats.gpu_history, gpu_color, "GPU"),
                        (&self.sys_stats.gpu_temp_history, temp_color, "Temp"),
                    ]);
                } else {
                    draw_graph(ui, 80.0, &[
                        (&self.sys_stats.gpu_history, gpu_color, "GPU"),
                    ]);
                }
            });
        });

        ui.add_space(6.0);

        // Active encodes
        if !self.transcodes.is_empty() {
            // Toolbar row matching web UI
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new(format!("{} active encode{}", self.transcodes.len(), if self.transcodes.len() > 1 { "s" } else { "" })).color(TEXT_SECONDARY).size(11.0));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if self.transcodes.len() > 1 {
                        if ui.add(egui::Button::new(egui::RichText::new("Cancel All").color(egui::Color32::WHITE).size(10.0))
                            .fill(ERROR).corner_radius(4.0).min_size(egui::vec2(65.0, 22.0))).clicked() {
                            for t in &self.transcodes { kill_pid(t.pid); }
                        }
                    }
                });
            });
            ui.add_space(4.0);

            for t in &self.transcodes {
                // Find matching job_log for extra detail
                let job_info = self.job_logs.iter().find(|j| !j.finished);

                // Progress card matching web UI's .progress-card
                let frame = egui::Frame::new()
                    .fill(BG_CARD)
                    .stroke(egui::Stroke::new(0.5, BORDER_SUBTLE))
                    .corner_radius(6.0)
                    .inner_margin(egui::Margin::symmetric(10, 8))
                    .outer_margin(egui::Margin::symmetric(0, 2));
                frame.show(ui, |ui| {
                    // Row 1: badges + filename + cancel
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 4.0;
                        badge(ui, "Recode", ACCENT);
                        if !t.video_codec.is_empty() {
                            let codec_label = if t.video_codec.contains("videotoolbox") { "VideoToolbox" }
                                else if t.video_codec.contains("nvenc") { "NVENC" }
                                else { &t.video_codec };
                            badge(ui, codec_label, SUCCESS);
                        }
                        // Filename (truncated, fills space)
                        let fname = if t.input.len() > 55 { format!("{}...", &t.input[..52]) } else { t.input.clone() };
                        ui.label(egui::RichText::new(fname).color(TEXT_PRIMARY).size(11.0));

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.spacing_mut().item_spacing.x = 4.0;
                            // Cancel button (x-circle icon — matches web UI: 12px icon, padding 4px/8px)
                            let btn_size = egui::vec2(28.0, 20.0);
                            let (btn_rect, btn_resp) = ui.allocate_exact_size(btn_size, egui::Sense::click());
                            let btn_color = if btn_resp.hovered() { ERROR } else { egui::Color32::from_rgb(180, 55, 50) };
                            ui.painter().rect_filled(btn_rect, 4.0, btn_color);
                            let c = btn_rect.center();
                            // SVG viewBox 24x24, rendered at 12px: circle r=10 → 5.0, lines 9↔15 → ±1.5
                            ui.painter().circle_stroke(c, 5.0, egui::Stroke::new(1.0, egui::Color32::WHITE));
                            let xs = egui::Stroke::new(1.2, egui::Color32::WHITE);
                            ui.painter().line_segment([egui::pos2(c.x - 1.8, c.y - 1.8), egui::pos2(c.x + 1.8, c.y + 1.8)], xs);
                            ui.painter().line_segment([egui::pos2(c.x + 1.8, c.y - 1.8), egui::pos2(c.x - 1.8, c.y + 1.8)], xs);
                            if btn_resp.clicked() {
                                kill_pid(t.pid);
                            }
                            // Speed badge (with gap from cancel button)
                            ui.add_space(6.0);
                            if let Some(j) = job_info {
                                if !j.last_speed.is_empty() {
                                    badge(ui, &j.last_speed, TEXT_SECONDARY);
                                }
                            }
                        });
                    });

                    // Row 2: detail badges (client, CPU, elapsed, size)
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 3.0;
                        ui.add_space(4.0);
                        if !t.client.is_empty() {
                            let host = t.client.split(':').next().unwrap_or(&t.client);
                            badge(ui, &format!("Client: {}", host), TEXT_SECONDARY);
                        }
                        badge(ui, &format!("CPU {:.0}%", t.cpu), TEXT_SECONDARY);
                        if let Some(j) = job_info {
                            let elapsed = j.started.elapsed().as_secs();
                            if elapsed > 0 {
                                badge(ui, &format!("{}:{:02}:{:02}", elapsed / 3600, (elapsed % 3600) / 60, elapsed % 60), TEXT_SECONDARY);
                            }
                            if !j.output_size.is_empty() {
                                badge(ui, &j.output_size, TEXT_SECONDARY);
                            }
                        }
                    });

                    // Row 3: progress bar
                    ui.add_space(4.0);
                    let bar_height = 16.0;
                    let (bar_rect, _) = ui.allocate_exact_size(egui::vec2(ui.available_width(), bar_height), egui::Sense::hover());
                    let painter = ui.painter_at(bar_rect);
                    // Bar background
                    painter.rect_filled(bar_rect, 8.0, egui::Color32::from_rgb(30, 37, 48));
                    // Bar fill from actual progress percentage
                    let pct = job_info.map(|j| j.progress_pct).unwrap_or(0.0);
                    let fill_frac = (pct / 100.0).clamp(0.0, 1.0);
                    if fill_frac > 0.0 {
                        let fill_rect = egui::Rect::from_min_size(bar_rect.min, egui::vec2(bar_rect.width() * fill_frac, bar_height));
                        painter.rect_filled(fill_rect, 8.0, ACCENT);
                    }
                    // Bar text: percentage + speed + out_time
                    let bar_text = if let Some(j) = job_info {
                        let time_str = if j.last_time_secs > 0.0 {
                            let h = (j.last_time_secs / 3600.0) as u64;
                            let m = ((j.last_time_secs % 3600.0) / 60.0) as u64;
                            let s = (j.last_time_secs % 60.0) as u64;
                            if h > 0 { format!("{h}:{m:02}:{s:02}") } else { format!("{m}:{s:02}") }
                        } else { String::new() };
                        if j.progress_pct > 0.0 {
                            format!("{:.1}%  ·  {}  ·  {}", j.progress_pct, time_str, j.last_speed)
                        } else if j.last_time_secs > 0.0 {
                            format!("{}  ·  {}  ·  {}", time_str, j.last_speed, j.output_size)
                        } else if j.last_frame > 0 {
                            format!("frame {}  ·  {}", j.last_frame, j.last_speed)
                        } else { "Encoding...".into() }
                    } else { "Encoding...".into() };
                    painter.text(bar_rect.center(), egui::Align2::CENTER_CENTER, bar_text, egui::FontId::proportional(9.0), egui::Color32::WHITE);
                });
            }
        } else {
            // Empty state
            ui.add_space(40.0);
            ui.vertical_centered(|ui| {
                // Recode icon
                ui.label(egui::RichText::new("\u{21BB}").color(TEXT_MUTED).size(36.0));
                ui.add_space(8.0);
                let (title, sub) = if !self.running {
                    ("Not running", "Click Start to connect to the encoding server.")
                } else if status.connected {
                    ("No active encodes", "Waiting for jobs from the server.")
                } else if !status.error.is_empty() {
                    ("Connection error", status.error.as_str())
                } else {
                    ("Connecting...", "Establishing connection to the server.")
                };
                ui.label(egui::RichText::new(title).color(TEXT_SECONDARY).size(15.0));
                ui.add_space(2.0);
                ui.label(egui::RichText::new(sub).color(TEXT_MUTED).size(12.0));
            });
        }
    }

    fn ui_logs(&mut self, ui: &mut egui::Ui) {
        // Active jobs summary
        let active: Vec<_> = self.job_logs.iter().filter(|j| !j.finished).collect();
        if !active.is_empty() {
            section_label(ui, "ACTIVE JOBS");
            for job in &active {
                card!(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new(&job.id[..6]).color(ACCENT).size(11.0).font(egui::FontId::monospace(11.0)));
                        ui.label(egui::RichText::new(&job.input).color(TEXT_PRIMARY).size(11.0));
                    });
                    ui.horizontal(|ui| {
                        if !job.client.is_empty() {
                            ui.label(egui::RichText::new(format!("Client: {}", job.client)).color(TEXT_MUTED).size(10.0));
                        }
                        if !job.last_speed.is_empty() {
                            ui.label(egui::RichText::new(format!("Speed: {}", job.last_speed)).color(TEXT_SECONDARY).size(10.0));
                        }
                        let elapsed = job.started.elapsed().as_secs();
                        ui.label(egui::RichText::new(format!("Elapsed: {}:{:02}:{:02}", elapsed / 3600, (elapsed % 3600) / 60, elapsed % 60)).color(TEXT_MUTED).size(10.0));
                    });
                });
            }
        }

        section_label(ui, "LOG");
        card!(ui, |ui| {
            ui.horizontal(|ui| {
                if ui.button(egui::RichText::new("Clear").color(TEXT_SECONDARY).size(10.0)).clicked() {
                    self.logs.lock().unwrap().clear();
                }
                ui.label(egui::RichText::new(format!("{} entries", self.logs.lock().unwrap().len())).color(TEXT_MUTED).size(10.0));
            });
            egui::ScrollArea::vertical().max_height(ui.available_height() - 20.0).stick_to_bottom(true).show(ui, |ui| {
                let logs = self.logs.lock().unwrap();
                if logs.is_empty() {
                    ui.label(egui::RichText::new("No logs yet. Start the connector to see output here.").color(TEXT_MUTED).size(11.0));
                }
                for line in logs.iter() {
                    let c = if line.contains("ERROR") || line.contains("error") || line.contains("fail") { ERROR }
                        else if line.contains("WARN") || line.contains("warn") { WARNING }
                        else if line.contains("COMPLETE") || line.contains("done") { SUCCESS }
                        else if line.contains("Job ") && line.contains("started") { ACCENT }
                        else if line.contains("Job ") && line.contains("frame") { TEXT_SECONDARY }
                        else { TEXT_MUTED };
                    ui.label(egui::RichText::new(line).color(c).size(10.0).font(egui::FontId::monospace(10.0)));
                }
            });
        });
    }

    fn ui_help(&self, ui: &mut egui::Ui) {
        section_label(ui, "ABOUT");
        card!(ui, |ui| {
            ui.horizontal(|ui| {
                let (logo_rect, _) = ui.allocate_exact_size(egui::vec2(20.0, 20.0), egui::Sense::hover());
                paint_recode_logo(ui.painter(), logo_rect.center(), 8.0, egui::Stroke::new(2.0, ACCENT));
                ui.label(egui::RichText::new("Recode GPU Server").color(egui::Color32::WHITE).size(16.0));
            });
            ui.label(egui::RichText::new(format!("Version {VERSION}")).color(TEXT_MUTED).size(11.0));
            ui.add_space(6.0);
            ui.label(egui::RichText::new(
                "Connects your Mac to a Recode encoding server as a remote GPU worker. \
                Your Mac's VideoToolbox hardware encoder is used for HEVC and H.264 encoding."
            ).color(TEXT_SECONDARY).size(12.0));
        });

        ui.add_space(4.0);
        section_label(ui, "QUICK START");
        card!(ui, |ui| {
            for (i, step) in ["Open Settings (\u{2699} icon)", "Enter your server address and secret", "Click Start", "Jobs appear in the Encoding tab"].iter().enumerate() {
                ui.horizontal(|ui| {
                    badge(ui, &format!("{}", i+1), ACCENT);
                    ui.label(egui::RichText::new(*step).color(TEXT_SECONDARY).size(12.0));
                });
            }
        });

        ui.add_space(4.0);
        section_label(ui, "HOW IT WORKS");
        card!(ui, |ui| {
            for line in [
                "Your Mac connects to the Recode server via RRP protocol",
                "Source files are streamed on-demand via FUSE mount",
                "Encoding uses Apple VideoToolbox hardware acceleration",
                "Encoded files are sent back to the server automatically",
            ] {
                ui.label(egui::RichText::new(format!("  \u{2022}  {line}")).color(TEXT_SECONDARY).size(12.0));
            }
        });

        ui.add_space(4.0);
        section_label(ui, "SYSTEM INFO");
        card!(ui, |ui| {
            ui.horizontal(|ui| { ui.label(egui::RichText::new("GPU:").color(TEXT_MUTED).size(11.0)); ui.label(egui::RichText::new(&self.gpu_name).color(TEXT_PRIMARY).size(11.0)); });
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Encoders:").color(TEXT_MUTED).size(11.0));
                for e in &self.encoders { badge(ui, e, if e.contains("videotoolbox") { SUCCESS } else { WARNING }); }
            });
        });
    }

    fn ui_settings(&mut self, ui: &mut egui::Ui) {
        // Helper: styled text input field with label
        fn field(ui: &mut egui::Ui, label: &str, hint: &str, value: &mut String, password: bool) -> bool {
            ui.add_space(2.0);
            ui.label(egui::RichText::new(label).color(TEXT_SECONDARY).size(11.0));
            ui.add_space(2.0);
            let te = egui::TextEdit::singleline(value)
                .hint_text(egui::RichText::new(hint).color(TEXT_MUTED))
                .font(egui::FontId::monospace(12.0))
                .text_color(TEXT_PRIMARY)
                .desired_width(f32::INFINITY)
                .margin(egui::Margin::symmetric(10, 6))
                .password(password);
            let frame = egui::Frame::new()
                .fill(BG_INPUT)
                .stroke(egui::Stroke::new(1.0, BORDER_SUBTLE))
                .corner_radius(6.0)
                .inner_margin(egui::Margin::same(0));
            let mut changed = false;
            frame.show(ui, |ui| {
                if ui.add(te).changed() { changed = true; }
            });
            ui.add_space(4.0);
            changed
        }

        section_label(ui, "CONNECTION");
        card!(ui, |ui| {
            ui.columns(2, |cols| {
                if field(&mut cols[0], "Server Address", "e.g. 192.168.1.100:9879", &mut self.draft.address, false) { self.dirty = true; }
                if field(&mut cols[0], "Secret", "Shared secret key", &mut self.draft.secret, true) { self.dirty = true; }
                if field(&mut cols[1], "Display Name", "e.g. Mac Studio", &mut self.draft.name, false) { self.dirty = true; }
                cols[1].add_space(2.0);
                cols[1].label(egui::RichText::new("Max Concurrent Jobs").color(TEXT_SECONDARY).size(11.0));
                cols[1].add_space(2.0);
                if cols[1].add(egui::Slider::new(&mut self.draft.max_jobs, 1..=8).text("jobs")).changed() { self.dirty = true; }
                cols[1].add_space(4.0);
            });
        });

        ui.add_space(6.0);
        section_label(ui, "PATHS");
        card!(ui, |ui| {
            ui.columns(2, |cols| {
                if field(&mut cols[0], "FFmpeg Path", "/path/to/ffmpeg", &mut self.draft.ffmpeg_path, false) { self.dirty = true; }
                if field(&mut cols[1], "Temp Directory", "/tmp/recode/rrp", &mut self.draft.tmp_dir, false) { self.dirty = true; }
            });
        });

        ui.add_space(6.0);
        section_label(ui, "GENERAL");
        card!(ui, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                let label = if self.draft.auto_start { "On" } else { "Off" };
                let color = if self.draft.auto_start { ACCENT } else { TEXT_MUTED };
                // Toggle switch
                let (rect, resp) = ui.allocate_exact_size(egui::vec2(36.0, 20.0), egui::Sense::click());
                let bg = if self.draft.auto_start { ACCENT } else { egui::Color32::from_rgb(60, 65, 72) };
                ui.painter().rect_filled(rect, 10.0, bg);
                let knob_x = if self.draft.auto_start { rect.right() - 10.0 } else { rect.left() + 10.0 };
                ui.painter().circle_filled(egui::pos2(knob_x, rect.center().y), 7.0, egui::Color32::WHITE);
                if resp.clicked() { self.draft.auto_start = !self.draft.auto_start; self.dirty = true; }
                ui.add_space(6.0);
                ui.label(egui::RichText::new("Auto-connect on launch").color(TEXT_PRIMARY).size(12.0));
                ui.label(egui::RichText::new(label).color(color).size(10.0));
            });
            ui.label(egui::RichText::new("Automatically connect to the encoding server when the app starts.").color(TEXT_MUTED).size(10.0));
            ui.add_space(4.0);
        });

        // Save button
        ui.add_space(12.0);
        ui.horizontal(|ui| {
            let btn_fill = if self.dirty { ACCENT } else { TEXT_MUTED };
            let btn_text = if self.dirty { "Save Settings" } else { "Settings Saved" };
            if ui.add_enabled(self.dirty, egui::Button::new(egui::RichText::new(format!("  {}  ", btn_text)).color(egui::Color32::WHITE).size(12.0))
                .fill(btn_fill).corner_radius(6.0).min_size(egui::vec2(140.0, 34.0))).clicked() {
                self.settings = self.draft.clone();
                self.settings.save();
                self.dirty = false;
                self.log("Settings saved");
            }
            if self.dirty {
                ui.add_space(8.0);
                ui.label(egui::RichText::new("Unsaved changes").color(WARNING).size(11.0));
            }
        });
    }


}

// ── Entry Point ────────────────────────────────────────────────────────────
fn main() -> eframe::Result<()> {
    // System tray
    let menu = Menu::new();
    // Initial placeholder — build_tray_menu() replaces this immediately after
    menu.append(&MenuItem::with_id("show", "Show Window", true, None)).unwrap();
    menu.append(&MenuItem::with_id("quit", "Quit", true, None)).unwrap();

    // Tray icon: Recode circular arrow (22x22 RGBA) with animation frames
    let icon_data = include_bytes!("tray_icon.bin").to_vec();
    let icon = tray_icon::Icon::from_rgba(icon_data, 22, 22).unwrap();
    let tray_frames_data = include_bytes!("tray_frames.bin");

    let tray_instance = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("Recode GPU Server — Idle")
        .with_icon(icon)
        .build()
        .unwrap();
    unsafe { TRAY = Some(tray_instance); }
    build_tray_menu(false);

    // Handle menu events in background (match by string ID since menu items get recreated)
    std::thread::spawn(move || {
        loop {
            if let Ok(event) = MenuEvent::receiver().recv() {
                let id_str = event.id().0.as_str();
                if id_str == "quit" {
                    // Clean up all child processes before exit
                    let _ = std::process::Command::new("pkill").args(["-f", "recode-remote connect"]).output();
                    let _ = std::process::Command::new("pkill").args(["-f", "ffmpeg.*recode/rrp"]).output();
                    // Unmount FUSE
                    if let Ok(output) = std::process::Command::new("mount").output() {
                        for line in String::from_utf8_lossy(&output.stdout).lines() {
                            if line.contains("macfuse") && line.contains("/tmp/recode") {
                                if let Some(mp) = line.split(" on ").nth(1).and_then(|s| s.split(' ').next()) {
                                    let _ = std::process::Command::new("diskutil").args(["unmount", "force", mp]).output();
                                }
                            }
                        }
                    }
                    QUIT_FLAG.store(true, Ordering::Relaxed);
                    std::process::exit(0);
                }
                if id_str == "show" {
                    SHOW_FLAG.store(true, Ordering::Relaxed);
                }
            }
        }
    });


    // On macOS, restore the bundle's dock icon after egui opens (egui overrides it)
    #[cfg(target_os = "macos")]
    {
        let icns_path = std::env::current_exe().ok()
            .and_then(|p| p.parent()?.parent().map(|d| d.join("Resources/AppIcon.icns")))
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        std::thread::spawn(move || {
            for delay_ms in [300, 600, 1000, 2000, 4000] {
                std::thread::sleep(Duration::from_millis(delay_ms));
                unsafe {
                    use cocoa::base::nil;
                    use cocoa::foundation::NSString;
                    let app = cocoa::appkit::NSApp();
                    let path = NSString::alloc(nil).init_str(&icns_path);
                    let image: cocoa::base::id = msg_send![class!(NSImage), alloc];
                    let image: cocoa::base::id = msg_send![image, initWithContentsOfFile: path];
                    if image != nil {
                        let _: () = msg_send![app, setApplicationIconImage: image];
                    }
                }
            }
        });
    }

    eframe::run_native("Recode GPU Server", eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([800.0, 640.0])
            .with_min_inner_size([460.0, 420.0])
            .with_title("Recode GPU Server")
            .with_icon(Arc::new(egui::IconData { rgba: vec![0, 0, 0, 0], width: 1, height: 1 })),
        ..Default::default()
    }, Box::new(|cc| Ok(Box::new(App::new(cc)))))
}
