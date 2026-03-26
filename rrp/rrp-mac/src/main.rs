use eframe::egui;
use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tray_icon::{TrayIconBuilder, menu::{Menu, MenuEvent, MenuItem}};
use std::sync::atomic::{AtomicBool, Ordering};

static QUIT_FLAG: AtomicBool = AtomicBool::new(false);
static SHOW_FLAG: AtomicBool = AtomicBool::new(false);
const VERSION: &str = "0.1.0";

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

#[derive(PartialEq, Clone, Copy)]
enum Panel { Encoding, Logs, Help, Settings }

// ── Main App ───────────────────────────────────────────────────────────────
struct App {
    settings: Settings, draft: Settings, child: Option<Child>, running: bool,
    status: Arc<Mutex<ConnStatus>>, logs: Arc<Mutex<Vec<String>>>,
    panel: Panel, tick: Instant, gpu_name: String, encoders: Vec<String>,
    started: bool, dirty: bool, transcodes: Vec<Transcode>, visible: bool,
}

impl App {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let s = Settings::load();
        Self {
            draft: s.clone(), settings: s, child: None, running: false,
            status: Arc::new(Mutex::new(ConnStatus::default())),
            logs: Arc::new(Mutex::new(Vec::new())), panel: Panel::Encoding,
            tick: Instant::now(), gpu_name: gpu_name(), encoders: Vec::new(),
            started: false, dirty: false, transcodes: Vec::new(), visible: true,
        }
    }
    fn start(&mut self) {
        if self.running || self.settings.address.is_empty() || self.settings.secret.is_empty() { return; }
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
                            let mut lg = logs.lock().unwrap();
                            lg.push(l); if lg.len() > 1000 { lg.drain(0..200); }
                        }
                    });
                }
                self.child = Some(c); self.running = true;
                self.log("Connected to encoding server");
            }
            Err(e) => self.log(&format!("Failed to start: {e}")),
        }
    }
    fn stop(&mut self) {
        if let Some(mut c) = self.child.take() { let _ = c.kill(); let _ = c.wait(); }
        self.running = false; *self.status.lock().unwrap() = ConnStatus::default();
        self.transcodes.clear(); self.log("Disconnected");
    }
    fn log(&self, m: &str) { self.logs.lock().unwrap().push(format!("{} {}", ts(), m)); }
    fn poll(&mut self) {
        if let Some(ref mut c) = self.child {
            if let Ok(Some(_)) = c.try_wait() {
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

fn kill_pid(pid: u64) {
    if pid > 0 { let _ = std::process::Command::new("kill").args(["-9", &pid.to_string()]).output(); }
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

            // Try to get original filename from RRP_INPUT env var (set by recode-remote)
            let fname = get_rrp_env(pid, "RRP_INPUT").map(|p| std::path::Path::new(&p).file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or(p)).unwrap_or_else(|| {
                let input = p[10].split("-i ").nth(1).and_then(|s| s.split(' ').next()).unwrap_or("").trim_matches('"');
                std::path::Path::new(input).file_name().map(|f| f.to_string_lossy().into()).unwrap_or(input.into())
            });

            let client = get_rrp_env(pid, "RRP_CLIENT").unwrap_or_default();
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
    ui.painter().rect_filled(rect, 4.0, alpha(color, 25));
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

macro_rules! card {
    ($ui:expr, $body:expr) => {
        egui::Frame::new()
            .fill(BG_CARD)
            .stroke(egui::Stroke::new(1.0, BORDER))
            .corner_radius(10.0)
            .inner_margin(egui::Margin::same(14))
            .outer_margin(egui::Margin::symmetric(0, 2))
            .show($ui, $body);
    };
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
        if self.settings.auto_start && !self.started { self.started = true; self.start(); }
        // Always repaint periodically (needed for tray "Show Window" when hidden)
        ctx.request_repaint_after(Duration::from_secs(1));
        if self.tick.elapsed() > Duration::from_secs(2) {
            self.poll(); self.tick = Instant::now();
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
        style.visuals.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, BORDER);
        style.visuals.widgets.hovered.bg_fill = BG_HOVER;
        style.visuals.widgets.hovered.weak_bg_fill = BG_HOVER;
        style.visuals.widgets.active.bg_fill = ACCENT;
        style.visuals.selection.bg_fill = alpha(ACCENT, 40);
        style.visuals.selection.stroke = egui::Stroke::new(1.0, ACCENT);
        style.visuals.window_stroke = egui::Stroke::new(1.0, BORDER);
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
                .stroke(egui::Stroke::new(1.0, BORDER))
                .inner_margin(egui::Margin::symmetric(16, 10))
        ).show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.spacing_mut().item_spacing.x = 6.0;
                // Recode spinning arrow icon
                ui.label(egui::RichText::new("\u{21BB}").color(ACCENT).size(18.0));
                ui.label(egui::RichText::new("Recode GPU Server").color(TEXT_PRIMARY).size(14.0).strong());
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
                .stroke(egui::Stroke::new(1.0, BORDER))
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
                .stroke(egui::Stroke::new(1.0, BORDER))
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
        // GPU card
        card!(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new(&self.gpu_name).color(TEXT_PRIMARY).size(14.0).strong());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(egui::RichText::new(format!("Max {} jobs", self.settings.max_jobs)).color(TEXT_MUTED).size(10.0));
                });
            });
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                for enc in self.encoders.iter().filter(|e| !e.starts_with("lib")) {
                    let c = if enc.contains("videotoolbox") { SUCCESS } else if enc.contains("lib") { WARNING } else { ACCENT };
                    badge(ui, enc, c);
                }
                if self.encoders.is_empty() {
                    ui.label(egui::RichText::new("Detecting encoders...").color(TEXT_MUTED).size(11.0));
                }
            });
        });

        ui.add_space(6.0);

        // Active encodes
        if !self.transcodes.is_empty() {
            ui.horizontal(|ui| {
                section_label(ui, &format!("ACTIVE ENCODES  \u{2022}  {}", self.transcodes.len()));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if self.transcodes.len() > 1 {
                        if ui.add(egui::Button::new(egui::RichText::new("Cancel All").color(egui::Color32::WHITE).size(10.0))
                            .fill(ERROR).corner_radius(4.0).min_size(egui::vec2(65.0, 22.0))).clicked() {
                            for t in &self.transcodes { kill_pid(t.pid); }
                        }
                    }
                });
            });
            for t in &self.transcodes {
                card!(ui, |ui| {
                    ui.horizontal(|ui| {
                        badge(ui, "Recode", ACCENT);
                        if !t.video_codec.is_empty() { badge(ui, &t.video_codec, SUCCESS); }
                        ui.label(egui::RichText::new(&t.input).color(TEXT_PRIMARY).size(12.0));
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.add(egui::Button::new(egui::RichText::new("Cancel").color(egui::Color32::WHITE).size(10.0))
                                .fill(ERROR).corner_radius(4.0).min_size(egui::vec2(50.0, 22.0))).clicked() {
                                kill_pid(t.pid);
                            }
                        });
                    });
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new(format!("CPU: {:.0}%", t.cpu)).color(TEXT_SECONDARY).size(10.0));
                        ui.label(egui::RichText::new(format!("PID: {}", t.pid)).color(TEXT_MUTED).size(9.0));
                    });
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

    fn ui_logs(&self, ui: &mut egui::Ui) {
        section_label(ui, "CONNECTOR LOGS");
        card!(ui, |ui| {
            egui::ScrollArea::vertical().max_height(ui.available_height() - 20.0).stick_to_bottom(true).show(ui, |ui| {
                let logs = self.logs.lock().unwrap();
                if logs.is_empty() {
                    ui.label(egui::RichText::new("No logs yet. Start the connector to see output here.").color(TEXT_MUTED).size(11.0));
                }
                for line in logs.iter() {
                    let c = if line.contains("ERROR") || line.contains("error") || line.contains("fail") { ERROR }
                        else if line.contains("WARN") || line.contains("warn") { WARNING }
                        else if line.contains("INFO") || line.contains("info") { TEXT_MUTED }
                        else { TEXT_MUTED };
                    ui.label(egui::RichText::new(line).color(c).size(10.0).font(egui::FontId::monospace(10.0)));
                }
            });
        });
    }

    fn ui_help(&self, ui: &mut egui::Ui) {
        section_label(ui, "ABOUT");
        card!(ui, |ui| {
            ui.label(egui::RichText::new("Recode GPU Server").color(TEXT_PRIMARY).size(16.0).strong());
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
        // Section title with description (matching web UI pattern)
        ui.label(egui::RichText::new("Connection Settings").color(TEXT_PRIMARY).size(15.0).strong());
        ui.label(egui::RichText::new("Configure how this Mac connects to the Recode encoding server.").color(TEXT_SECONDARY).size(11.0));
        ui.add_space(8.0);

        // Connection card — 2-column grid with label above control (like web UI)
        card!(ui, |ui| {
            ui.columns(2, |cols| {
                // Column 1
                cols[0].label(egui::RichText::new("Server address").color(TEXT_MUTED).size(10.0));
                if cols[0].add(egui::TextEdit::singleline(&mut self.draft.address).hint_text("host:port")).changed() { self.dirty = true; }
                cols[0].add_space(6.0);
                cols[0].label(egui::RichText::new("Secret").color(TEXT_MUTED).size(10.0));
                if cols[0].add(egui::TextEdit::singleline(&mut self.draft.secret).password(true)).changed() { self.dirty = true; }

                // Column 2
                cols[1].label(egui::RichText::new("Display name").color(TEXT_MUTED).size(10.0));
                if cols[1].add(egui::TextEdit::singleline(&mut self.draft.name)).changed() { self.dirty = true; }
                cols[1].add_space(6.0);
                cols[1].label(egui::RichText::new("Max concurrent jobs").color(TEXT_MUTED).size(10.0));
                if cols[1].add(egui::Slider::new(&mut self.draft.max_jobs, 1..=8)).changed() { self.dirty = true; }
            });
        });

        ui.add_space(8.0);
        ui.label(egui::RichText::new("Paths").color(TEXT_PRIMARY).size(15.0).strong());
        ui.label(egui::RichText::new("Paths to tools used for encoding.").color(TEXT_SECONDARY).size(11.0));
        ui.add_space(8.0);

        card!(ui, |ui| {
            ui.columns(2, |cols| {
                cols[0].label(egui::RichText::new("FFmpeg path").color(TEXT_MUTED).size(10.0));
                if cols[0].add(egui::TextEdit::singleline(&mut self.draft.ffmpeg_path)).changed() { self.dirty = true; }

                cols[1].label(egui::RichText::new("Temp directory").color(TEXT_MUTED).size(10.0));
                if cols[1].add(egui::TextEdit::singleline(&mut self.draft.tmp_dir)).changed() { self.dirty = true; }
            });
        });

        ui.add_space(8.0);
        ui.label(egui::RichText::new("General").color(TEXT_PRIMARY).size(15.0).strong());
        ui.add_space(4.0);

        card!(ui, |ui| {
            if ui.checkbox(&mut self.draft.auto_start, egui::RichText::new("Auto-connect on launch").color(TEXT_SECONDARY).size(12.0)).changed() { self.dirty = true; }
            ui.label(egui::RichText::new("Automatically connects to the server when the app starts.").color(TEXT_MUTED).size(10.0));
        });

        // Save button
        ui.add_space(12.0);
        ui.horizontal(|ui| {
            let btn_fill = if self.dirty { ACCENT } else { TEXT_MUTED };
            let btn_text = if self.dirty { "Save Settings" } else { "Saved" };
            if ui.add_enabled(self.dirty, egui::Button::new(egui::RichText::new(format!("  {}  ", btn_text)).color(egui::Color32::WHITE).size(12.0))
                .fill(btn_fill).corner_radius(6.0).min_size(egui::vec2(120.0, 32.0))).clicked() {
                self.settings = self.draft.clone();
                self.settings.save();
                self.dirty = false;
                self.log("Settings saved");
            }
            if self.dirty {
                ui.add_space(8.0);
                ui.label(egui::RichText::new("\u{26A0}  Unsaved changes").color(WARNING).size(11.0));
            }
        });
    }


}

// ── Entry Point ────────────────────────────────────────────────────────────
fn main() -> eframe::Result<()> {
    // System tray
    let menu = Menu::new();
    let show_item = MenuItem::new("Show Window", true, None);
    let quit_item = MenuItem::new("Quit", true, None);
    menu.append(&show_item).unwrap();
    menu.append(&quit_item).unwrap();

    // Tray icon: native-rendered ↻ character (22x22 RGBA, generated by Swift)
    let icon_data: Vec<u8> = vec![0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,113,136,255,0,156,188,255,0,151,182,255,0,151,181,255,
0,151,182,255,0,155,186,255,1,121,145,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,12,47,59,255,0,150,181,255,
0,191,229,255,0,187,225,255,0,187,225,255,0,192,231,255,0,148,178,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,1,151,181,255,0,186,224,255,0,183,220,255,0,188,226,255,0,145,175,255,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,13,62,77,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,158,190,255,0,187,224,255,0,186,223,255,0,188,226,255,0,145,174,255,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,128,154,255,0,177,213,255,12,68,84,255,21,23,30,255,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,133,161,255,0,190,229,255,0,171,205,255,0,155,187,255,0,192,230,255,
0,145,175,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,157,189,255,0,192,230,255,
0,159,192,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,22,22,29,255,8,75,92,255,0,184,221,255,0,186,223,255,9,91,111,255,
13,40,50,255,0,150,181,255,0,150,181,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,22,23,30,255,
9,65,79,255,0,175,211,255,0,190,228,255,6,109,131,255,21,22,28,255,0,0,0,0,0,0,0,0,0,0,0,0,4,134,161,255,0,193,231,255,
2,148,178,255,0,0,0,0,23,22,29,255,13,45,57,255,1,106,128,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,21,21,28,255,5,118,143,255,0,191,229,255,0,157,189,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,164,196,255,0,189,227,255,7,97,118,255,22,21,27,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,22,23,30,255,9,62,77,255,0,182,219,255,0,184,221,255,13,63,77,255,0,0,0,0,
0,0,0,0,16,54,67,255,0,179,214,255,0,184,221,255,8,64,78,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,169,203,255,0,189,227,255,
1,88,107,255,0,0,0,0,0,0,0,0,16,59,73,255,0,185,222,255,0,182,219,255,12,54,66,255,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,160,193,255,0,188,226,255,0,104,126,255,0,0,0,0,0,0,0,0,16,54,67,255,0,178,214,255,0,184,221,255,8,65,80,255,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,170,204,255,0,188,226,255,0,90,109,255,0,0,0,0,0,0,0,0,0,0,0,0,0,163,196,255,0,189,227,255,
7,99,121,255,22,20,27,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,22,23,30,255,11,64,79,255,0,183,220,255,0,183,220,255,12,64,78,255,0,0,0,0,0,0,0,0,0,0,0,0,
4,132,158,255,0,192,231,255,2,150,181,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,20,22,29,255,3,123,149,255,0,191,229,255,0,156,189,255,0,0,0,0,0,0,0,0,
0,0,0,0,22,22,29,255,11,75,92,255,0,184,221,255,0,186,224,255,7,95,115,255,22,21,27,255,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,22,21,27,255,12,67,82,255,0,176,212,255,0,190,228,255,6,107,130,255,
21,22,29,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,130,156,255,0,191,229,255,0,176,212,255,10,81,98,255,21,22,29,255,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,22,21,27,255,12,58,71,255,0,162,195,255,0,190,229,255,
0,158,190,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,157,189,255,0,190,229,255,
0,180,216,255,4,117,141,255,16,48,60,255,0,0,0,0,20,22,29,255,20,21,28,255,0,0,0,0,0,0,0,0,6,98,120,255,0,170,204,255,
0,191,229,255,0,174,209,255,10,64,78,255,21,23,30,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
14,49,61,255,2,149,179,255,0,190,229,255,0,189,228,255,0,173,209,255,0,141,169,255,4,122,146,255,5,119,144,255,0,135,162,255,0,167,201,255,
0,188,225,255,0,191,229,255,0,166,200,255,9,69,85,255,22,23,30,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,22,23,30,255,0,0,0,0,5,113,136,255,0,171,206,255,0,188,226,255,0,189,227,255,0,190,229,255,0,190,229,255,
0,189,228,255,0,188,226,255,0,179,214,255,2,129,157,255,12,53,65,255,21,22,30,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,21,23,30,255,0,0,0,0,2,99,120,255,0,152,182,255,
0,175,211,255,0,178,214,255,0,159,191,255,1,114,137,255,14,57,70,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,];
    let icon = tray_icon::Icon::from_rgba(icon_data, 22, 22).unwrap();

    let _tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("Recode GPU Server")
        .with_icon(icon)
        .build()
        .unwrap();

    let show_id = show_item.id().clone();
    let quit_id = quit_item.id().clone();

    // Handle menu events in background
    std::thread::spawn(move || {
        loop {
            if let Ok(event) = MenuEvent::receiver().recv() {
                if event.id() == &quit_id {
                    QUIT_FLAG.store(true, Ordering::Relaxed);
                    std::process::exit(0);
                }
                if event.id() == &show_id {
                    SHOW_FLAG.store(true, Ordering::Relaxed);
                }
            }
        }
    });

    eframe::run_native("Recode GPU Server", eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([540.0, 640.0])
            .with_min_inner_size([460.0, 420.0])
            .with_title("Recode GPU Server"),
        ..Default::default()
    }, Box::new(|cc| Ok(Box::new(App::new(cc)))))
}
