use anyhow::{Result, bail};
use rrp_proto::*;
use serde_json;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, mpsc};
use tracing::{info, warn};

/// A connected GPU server on the control channel
struct ConnectedGpu {
    name: String,
    address: String,
    encoders: Vec<String>,
    encoder_type: String,
    os: String,
    arch: String,
    max_jobs: usize,
    has_fuse: bool,
    active_jobs: u32,
    last_heartbeat: std::time::Instant,
    /// Send job assignments to this GPU's control channel handler
    job_tx: mpsc::Sender<ReverseControlMsg>,
}

/// Shared state for the listener
struct ListenerState {
    gpus: RwLock<HashMap<String, ConnectedGpu>>,
    secret: String,
    /// Pending data connections: job_id -> (TcpStream, oneshot to signal ready)
    pending_data: RwLock<HashMap<String, PendingData>>,
    /// Job requests from Python server: job_id -> job details + file paths
    job_queue: RwLock<HashMap<String, JobRequest>>,
}

struct PendingData {
    stream: tokio::net::TcpStream,
    ready_tx: tokio::sync::oneshot::Sender<()>,
}

/// A job request from the Python server (via job file)
struct JobRequest {
    job_id: String,
    ffmpeg_args: Vec<String>,
    input_files: Vec<FileInfo>,
    /// Local paths to actual files (for serving reads)
    local_paths: Vec<String>,
    output_path: String,
}

pub async fn run(port: u16, secret: String, status_file: String) -> Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Reverse-connect listener on {}", addr);

    let state = Arc::new(ListenerState {
        gpus: RwLock::new(HashMap::new()),
        secret,
        pending_data: RwLock::new(HashMap::new()),
        job_queue: RwLock::new(HashMap::new()),
    });

    // Status file writer
    let state2 = state.clone();
    let status_path = PathBuf::from(&status_file);
    if let Some(parent) = status_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            write_status_file(&state2, &status_path).await;
        }
    });

    // Job file watcher — Python server drops JSON files into a jobs directory
    let state3 = state.clone();
    let jobs_dir = PathBuf::from(status_file.replace("listener-status.json", "listener-jobs"));
    let _ = std::fs::create_dir_all(&jobs_dir);
    tokio::spawn(async move {
        watch_job_files(state3, jobs_dir).await;
    });

    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_incoming(stream, peer, &state).await {
                warn!("{}: {}", peer, e);
            }
        });
    }
}

async fn write_status_file(state: &ListenerState, path: &PathBuf) {
    let gpus = state.gpus.read().await;
    let gpu_list: Vec<serde_json::Value> = gpus.values().map(|g| {
        serde_json::json!({
            "name": g.name,
            "address": g.address,
            "encoders": g.encoders,
            "encoder_type": g.encoder_type,
            "os": g.os,
            "arch": g.arch,
            "max_jobs": g.max_jobs,
            "active_jobs": g.active_jobs,
            "has_fuse": g.has_fuse,
            "online": g.last_heartbeat.elapsed().as_secs() < 45,
        })
    }).collect();
    let status = serde_json::json!({
        "enabled": true,
        "running": true,
        "gpus": gpu_list,
    });
    if let Ok(json) = serde_json::to_string_pretty(&status) {
        let _ = std::fs::write(path, json);
    }
}

/// Watch for job files dropped by the Python server.
/// Each file is a JSON with job details. The listener picks it up,
/// finds a suitable GPU, and assigns the job.
async fn watch_job_files(state: Arc<ListenerState>, jobs_dir: PathBuf) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let entries = match std::fs::read_dir(&jobs_dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.extension().map(|e| e == "json").unwrap_or(false) { continue; }
            match std::fs::read_to_string(&path) {
                Ok(contents) => {
                    let _ = std::fs::remove_file(&path);
                    if let Ok(job) = serde_json::from_str::<serde_json::Value>(&contents) {
                        let job_id = job["job_id"].as_str().unwrap_or("").to_string();
                        if job_id.is_empty() { continue; }

                        let ffmpeg_args: Vec<String> = job["ffmpeg_args"].as_array()
                            .map(|a: &Vec<serde_json::Value>| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                            .unwrap_or_default();
                        let input_files: Vec<FileInfo> = job["input_files"].as_array()
                            .map(|a: &Vec<serde_json::Value>| a.iter().filter_map(|v| {
                                Some(FileInfo {
                                    original_path: v["original_path"].as_str()?.to_string(),
                                    virtual_name: v["virtual_name"].as_str()?.to_string(),
                                    size: v["size"].as_u64()?,
                                })
                            }).collect())
                            .unwrap_or_default();
                        let local_paths: Vec<String> = job["local_paths"].as_array()
                            .map(|a: &Vec<serde_json::Value>| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                            .unwrap_or_default();
                        let output_path = job["output_path"].as_str().unwrap_or("").to_string();

                        let target_gpu = job["target_gpu"].as_str().unwrap_or("").to_string();

                        info!("Job file received: {} ({} inputs, target={})", job_id, input_files.len(), if target_gpu.is_empty() { "any" } else { &target_gpu });
                        dispatch_job(&state, job_id, ffmpeg_args, input_files, local_paths, output_path, &target_gpu).await;
                    }
                }
                Err(e) => { warn!("Failed to read job file {:?}: {}", path, e); }
            }
        }
    }
}

/// Find a suitable GPU server and assign the job
async fn dispatch_job(
    state: &ListenerState,
    job_id: String,
    ffmpeg_args: Vec<String>,
    input_files: Vec<FileInfo>,
    local_paths: Vec<String>,
    output_path: String,
    target_gpu: &str,
) {
    let gpus = state.gpus.read().await;
    // Find GPU with capacity — prefer target_gpu if specified
    let candidate = if !target_gpu.is_empty() {
        gpus.iter()
            .filter(|(_, g)| g.name == target_gpu)
            .filter(|(_, g)| g.last_heartbeat.elapsed().as_secs() < 45)
            .filter(|(_, g)| (g.active_jobs as usize) < g.max_jobs)
            .next()
            .or_else(|| {
                // Target not available, fall back to any GPU
                gpus.iter()
                    .filter(|(_, g)| g.last_heartbeat.elapsed().as_secs() < 45)
                    .filter(|(_, g)| (g.active_jobs as usize) < g.max_jobs)
                    .min_by_key(|(_, g)| g.active_jobs)
            })
    } else {
        gpus.iter()
            .filter(|(_, g)| g.last_heartbeat.elapsed().as_secs() < 45)
            .filter(|(_, g)| (g.active_jobs as usize) < g.max_jobs)
            .min_by_key(|(_, g)| g.active_jobs)
    };

    let (conn_id, _) = match candidate {
        Some(c) => c,
        None => {
            warn!("No GPU servers available for job {}", job_id);
            // Write failure response
            let resp_path = PathBuf::from(format!("/tmp/recode/rrp/listener-jobs/{}.result", job_id));
            let _ = std::fs::write(&resp_path, serde_json::json!({"error": "No GPU servers available"}).to_string());
            return;
        }
    };
    let conn_id = conn_id.clone();
    let job_tx = gpus[&conn_id].job_tx.clone();
    drop(gpus);

    // Store job details for when the data connection arrives
    state.job_queue.write().await.insert(job_id.clone(), JobRequest {
        job_id: job_id.clone(),
        ffmpeg_args: ffmpeg_args.clone(),
        input_files: input_files.clone(),
        local_paths,
        output_path: output_path.clone(),
    });

    // Send assignment over control channel
    let port = 9879u16; // same port as listener — data connections come here too
    let assignment = ReverseControlMsg::JobAssignment {
        job_id: job_id.clone(),
        ffmpeg_args,
        input_files,
        output_path,
        connect_port: port,
    };
    if job_tx.send(assignment).await.is_err() {
        warn!("Failed to send job {} to GPU", job_id);
        state.job_queue.write().await.remove(&job_id);
    }
}

async fn handle_incoming(
    mut stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    state: &Arc<ListenerState>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf).await?;

    match type_buf[0] {
        CONN_TYPE_CONTROL => handle_control(stream, peer, state).await,
        CONN_TYPE_DATA => handle_data(stream, peer, state).await,
        _ => bail!("Unknown connection type: {}", type_buf[0]),
    }
}

async fn handle_control(
    mut stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    state: &Arc<ListenerState>,
) -> Result<()> {
    let (mut rx, mut tx) = stream.split();

    let auth: ReverseControlMsg = read_msg(&mut rx).await?;
    let (name, encoders, os, arch, max_jobs, has_fuse) = match auth {
        ReverseControlMsg::Auth { secret, server_name, encoders, os, arch, max_jobs, has_fuse } => {
            if secret != state.secret {
                write_msg(&mut tx, &ReverseControlMsg::AuthFail("bad secret".into())).await?;
                bail!("Auth failed from {}", peer);
            }
            (server_name, encoders, os, arch, max_jobs, has_fuse)
        }
        _ => bail!("Expected Auth"),
    };
    write_msg(&mut tx, &ReverseControlMsg::AuthOk).await?;

    let encoder_type = if encoders.iter().any(|e| e.contains("nvenc")) { "nvenc" }
        else if encoders.iter().any(|e| e.contains("videotoolbox")) { "videotoolbox" }
        else if encoders.iter().any(|e| e.contains("qsv")) { "qsv" }
        else if encoders.iter().any(|e| e.contains("amf")) { "amf" }
        else { "cpu" }.to_string();

    info!("GPU server '{}' connected from {} ({}/{}, {})", name, peer, os, arch, encoder_type);

    let conn_id = format!("{}@{}", name, peer);
    let (job_tx, mut job_rx) = mpsc::channel::<ReverseControlMsg>(16);

    state.gpus.write().await.insert(conn_id.clone(), ConnectedGpu {
        name: name.clone(), address: peer.ip().to_string(), encoders, encoder_type, os, arch, max_jobs, has_fuse,
        active_jobs: 0,
        last_heartbeat: std::time::Instant::now(),
        job_tx,
    });

    let result: Result<()> = async {
        loop {
            tokio::select! {
                msg = read_msg::<ReverseControlMsg, _>(&mut rx) => {
                    match msg? {
                        ReverseControlMsg::Heartbeat { active_jobs } => {
                            if let Some(gpu) = state.gpus.write().await.get_mut(&conn_id) {
                                gpu.active_jobs = active_jobs;
                                gpu.last_heartbeat = std::time::Instant::now();
                            }
                        }
                        ReverseControlMsg::JobFinished { job_id, exit_code } => {
                            info!("GPU '{}' finished job {} (exit {})", name, job_id, exit_code);
                            if let Some(gpu) = state.gpus.write().await.get_mut(&conn_id) {
                                gpu.active_jobs = gpu.active_jobs.saturating_sub(1);
                            }
                            state.job_queue.write().await.remove(&job_id);
                        }
                        _ => {}
                    }
                }
                Some(job_msg) = job_rx.recv() => {
                    write_msg(&mut tx, &job_msg).await?;
                    tx.flush().await?;
                    match read_msg::<ReverseControlMsg, _>(&mut rx).await? {
                        ReverseControlMsg::JobAccepted { job_id } => {
                            info!("GPU '{}' accepted job {}", name, job_id);
                            if let Some(gpu) = state.gpus.write().await.get_mut(&conn_id) {
                                gpu.active_jobs += 1;
                            }
                        }
                        ReverseControlMsg::JobReject { job_id, reason } => {
                            warn!("GPU '{}' rejected job {}: {}", name, job_id, reason);
                            state.job_queue.write().await.remove(&job_id);
                        }
                        _ => {}
                    }
                }
            }
        }
    }.await;

    state.gpus.write().await.remove(&conn_id);
    info!("GPU server '{}' disconnected", name);
    result
}

/// Handle a data connection from a GPU server.
/// The GPU server connects here to serve a specific job.
/// We serve local files to the GPU server's FUSE read requests.
async fn handle_data(
    mut stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    state: &Arc<ListenerState>,
) -> Result<()> {
    let (mut rx, mut tx) = stream.split();
    let dc: DataConnect = read_msg(&mut rx).await?;
    if dc.secret != state.secret {
        bail!("Data connection auth failed from {}", peer);
    }

    info!("Data connection for job {} from {}", dc.job_id, peer);

    // Look up job details
    let job = state.job_queue.read().await.get(&dc.job_id).map(|j| {
        (j.local_paths.clone(), j.output_path.clone())
    });
    let (local_paths, _output_path) = match job {
        Some(j) => j,
        None => bail!("No job details found for {}", dc.job_id),
    };

    // Open local files
    let mut files: Vec<std::fs::File> = Vec::new();
    for path in &local_paths {
        files.push(std::fs::File::open(path)?);
    }

    // Serve file reads (same protocol as the existing client's FUSE mode)
    // Also receive output file at the end
    let cancel_path = format!("/tmp/recode/rrp/listener-jobs/{}.cancel", dc.job_id);
    let mut exit_code = 1i32;
    loop {
        // Check for cancel file
        if std::path::Path::new(&cancel_path).exists() {
            warn!("Job {} cancelled by client", dc.job_id);
            let _ = std::fs::remove_file(&cancel_path);
            exit_code = -1;
            break;
        }
        let (tag, payload) = match read_tagged(&mut rx).await {
            Ok(tp) => tp,
            Err(_) => break,
        };

        match tag {
            TAG_FILE_READ_REQ => {
                let req: FileReadReq = bincode::deserialize(&payload)?;
                let resp = if let Some(file) = files.get_mut(req.file_idx as usize) {
                    use std::io::{Read, Seek, SeekFrom};
                    file.seek(SeekFrom::Start(req.offset)).ok();
                    let mut buf = vec![0u8; req.length as usize];
                    match file.read(&mut buf) {
                        Ok(n) => {
                            buf.truncate(n);
                            FileReadResp { data: buf, error: false }
                        }
                        Err(_) => FileReadResp { data: vec![], error: true },
                    }
                } else {
                    FileReadResp { data: vec![], error: true }
                };
                write_tagged(&mut tx, TAG_FILE_READ_RESP, &resp).await?;
                tx.flush().await?;
            }
            TAG_PROGRESS => {
                // Write progress to a file so the Python server can read it
                if let Ok(p) = bincode::deserialize::<ProgressMsg>(&payload) {
                    let progress_path = format!("/tmp/recode/rrp/listener-jobs/{}.progress", dc.job_id);
                    let progress_json = serde_json::json!({
                        "frame": p.frame, "time_secs": p.time_secs,
                        "speed": p.speed, "bitrate_kbps": p.bitrate_kbps,
                        "output_size": p.output_size,
                    });
                    let _ = std::fs::write(&progress_path, progress_json.to_string());
                }
            }
            TAG_CONTROL => {
                if let Ok(ctrl) = bincode::deserialize::<ControlMsg>(&payload) {
                    match ctrl {
                        ControlMsg::JobComplete { exit_code: ec } => {
                            exit_code = ec;
                            break;
                        }
                        ControlMsg::JobError(e) => {
                            warn!("Remote error for job {}: {}", dc.job_id, e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            _ if tag == STREAM_OUTPUT => {
                // Receive output file
                let total: u64 = bincode::deserialize(&payload)?;
                info!("Receiving output for job {}: {:.1} MB", dc.job_id, total as f64 / 1_048_576.0);
                let output_path = state.job_queue.read().await.get(&dc.job_id)
                    .map(|j| j.output_path.clone()).unwrap_or_default();
                if !output_path.is_empty() {
                    let mut file = tokio::fs::File::create(&output_path).await?;
                    let mut hasher = Sha256::new();
                    let mut remaining = total;
                    let mut buf = vec![0u8; 256 * 1024];
                    while remaining > 0 {
                        let to_read = std::cmp::min(remaining as usize, buf.len());
                        let n = rx.read(&mut buf[..to_read]).await?;
                        if n == 0 { break; }
                        hasher.update(&buf[..n]);
                        file.write_all(&buf[..n]).await?;
                        remaining -= n as u64;
                    }
                    let mut hash = [0u8; 32];
                    rx.read_exact(&mut hash).await?;
                    let computed: [u8; 32] = hasher.finalize().into();
                    if computed != hash { warn!("Output SHA256 mismatch for job {}!", dc.job_id); }
                    file.flush().await?;
                    info!("Output saved for job {}: {:.1} MB", dc.job_id, total as f64 / 1_048_576.0);
                }
            }
            _ => {}
        }
    }

    // Write result file for Python server
    let result_path = format!("/tmp/recode/rrp/listener-jobs/{}.result", dc.job_id);
    let _ = std::fs::write(&result_path, serde_json::json!({"exit_code": exit_code}).to_string());

    info!("Data connection for job {} complete (exit {})", dc.job_id, exit_code);
    Ok(())
}
