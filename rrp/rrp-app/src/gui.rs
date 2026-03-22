use axum::{Router, extract::State as AxumState, response::{Html, IntoResponse}, Json};
use http::{StatusCode, header};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

// Embedded WASM GUI assets (built from rrp-gui crate)
const WASM_HTML: &[u8] = include_bytes!("../../rrp-wasm/index.html");
const WASM_JS: &[u8] = include_bytes!("../../rrp-wasm/rrp_gui.js");
const WASM_BG: &[u8] = include_bytes!("../../rrp-wasm/rrp_gui_bg.wasm");

#[derive(Clone, Serialize)]
pub struct ActiveJob {
    pub job_id: String,
    pub client: String,
    pub input: String,
    pub attempt: u32,
    pub started_at: f64,
    pub frame: u64,
    pub speed: f32,
    pub pct: f32,
    pub bitrate_kbps: f32,
    pub output_size: u64,
}

#[derive(Clone, Serialize)]
pub struct ErrorEntry {
    pub job_id: String,
    pub input: String,
    pub client: String,
    pub error: String,
    pub timestamp: f64,
}

#[derive(Clone)]
pub struct GuiState {
    pub port: u16,
    pub ffmpeg: String,
    pub max_jobs: usize,
    pub encoders: Vec<String>,
    pub jobs: Arc<RwLock<Vec<ActiveJob>>>,
    pub completed: Arc<RwLock<u64>>,
    pub failed: Arc<RwLock<u64>>,
    pub errors: Arc<RwLock<Vec<ErrorEntry>>>,
    pub cpu_percent: Arc<RwLock<f32>>,
    pub gpu_percent: Arc<RwLock<f32>>,
    pub gpu_name: Arc<RwLock<String>>,
    pub net_rx_mbps: Arc<RwLock<f32>>,
    pub net_tx_mbps: Arc<RwLock<f32>>,
    pub secret: Arc<RwLock<String>>,
    pub max_jobs_rw: Arc<RwLock<usize>>,
}

pub async fn start_gui(state: GuiState, gui_port: u16) {
    let app = Router::new()
        .route("/", axum::routing::get(serve_html))
        .route("/rrp_gui.js", axum::routing::get(serve_js))
        .route("/rrp_gui_bg.wasm", axum::routing::get(serve_wasm))
        .route("/api/status", axum::routing::get(api_status))
        .route("/api/settings", axum::routing::post(api_update_settings))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", gui_port)).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("GUI: failed to bind port {}: {}", gui_port, e);
            return;
        }
    };
    tracing::info!("GUI available at http://localhost:{}", gui_port);
    axum::serve(listener, app).await.ok();
}

#[derive(serde::Deserialize)]
struct SettingsUpdate {
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    max_jobs: Option<usize>,
}

async fn api_update_settings(
    AxumState(state): AxumState<GuiState>,
    Json(body): Json<SettingsUpdate>,
) -> Json<serde_json::Value> {
    if let Some(secret) = body.secret {
        if !secret.is_empty() {
            *state.secret.write().await = secret;
            tracing::info!("Secret updated via GUI");
        }
    }
    if let Some(max_jobs) = body.max_jobs {
        if max_jobs >= 1 && max_jobs <= 32 {
            *state.max_jobs_rw.write().await = max_jobs;
            tracing::info!("Max jobs updated to {} via GUI", max_jobs);
        }
    }
    Json(serde_json::json!({"ok": true}))
}

async fn serve_html() -> impl IntoResponse {
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/html")], WASM_HTML)
}

async fn serve_js() -> impl IntoResponse {
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/javascript")], WASM_JS)
}

async fn serve_wasm() -> impl IntoResponse {
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/wasm")], WASM_BG)
}

async fn api_status(AxumState(state): AxumState<GuiState>) -> Json<serde_json::Value> {
    let jobs = state.jobs.read().await;
    let completed = *state.completed.read().await;
    let failed = *state.failed.read().await;
    let errors = state.errors.read().await;
    let cpu = *state.cpu_percent.read().await;
    let gpu = *state.gpu_percent.read().await;
    let gpu_name = state.gpu_name.read().await;
    let net_rx = *state.net_rx_mbps.read().await;
    let net_tx = *state.net_tx_mbps.read().await;
    let current_max = *state.max_jobs_rw.read().await;
    let has_secret = !state.secret.read().await.is_empty();
    Json(serde_json::json!({
        "port": state.port,
        "ffmpeg": state.ffmpeg,
        "max_jobs": current_max,
        "has_secret": has_secret,
        "encoders": state.encoders,
        "active_jobs": *jobs,
        "completed": completed,
        "failed": failed,
        "errors": *errors,
        "cpu_percent": cpu,
        "gpu_percent": gpu,
        "gpu_name": *gpu_name,
        "net_rx_mbps": net_rx,
        "net_tx_mbps": net_tx,
    }))
}
