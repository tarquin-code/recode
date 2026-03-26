use anyhow::{Result, Context, bail};
use rrp_proto::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{info, warn, error};

/// Read per-GPU max jobs from settings.json. Falls back to 1 per GPU.
fn read_gpu_max_jobs(gpu_count: usize) -> Vec<usize> {
    let settings_path = std::path::Path::new("/opt/Recode/settings.json");
    let mut per_gpu = vec![1usize; gpu_count];
    if let Ok(data) = std::fs::read_to_string(settings_path) {
        if let Ok(settings) = serde_json::from_str::<serde_json::Value>(&data) {
            // Read disabled_gpus
            let disabled: Vec<usize> = settings.get("disabled_gpus")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            // Read gpu_max_jobs
            if let Some(max_jobs_map) = settings.get("gpu_max_jobs").and_then(|v| v.as_object()) {
                for (k, v) in max_jobs_map {
                    if let (Ok(idx), Some(val)) = (k.parse::<usize>(), v.as_u64()) {
                        if idx < per_gpu.len() {
                            per_gpu[idx] = val as usize;
                        }
                    }
                }
            }
            // Disabled GPUs get 0
            for idx in disabled {
                if idx < per_gpu.len() {
                    per_gpu[idx] = 0;
                }
            }
        }
    }
    per_gpu
}

/// Detect number of GPUs via nvidia-smi
fn detect_gpu_count() -> usize {
    // Try multiple paths — PATH may be minimal when spawned by service
    for bin in &["nvidia-smi", "/usr/bin/nvidia-smi", "/usr/local/bin/nvidia-smi"] {
        if let Ok(output) = std::process::Command::new(bin)
            .args(["--query-gpu=index", "--format=csv,noheader"])
            .output()
        {
            if output.status.success() {
                let count = String::from_utf8_lossy(&output.stdout).lines().count();
                if count > 0 { return count; }
            }
        }
    }
    1
}

pub async fn run(
    address: String, secret: String, name: String,
    ffmpeg: String, tmp_dir: String, max_jobs: usize,
    status_file: String,
) -> Result<()> {
    std::fs::create_dir_all(&tmp_dir)?;
    let active_jobs = Arc::new(AtomicU32::new(0));
    // Track active job IDs for GC — GC only cleans dirs NOT in this set
    let active_job_ids: Arc<std::sync::Mutex<std::collections::HashSet<String>>> = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));

    // Start garbage collector for orphaned job dirs
    spawn_gc(tmp_dir.clone(), active_job_ids.clone());

    let status_path = PathBuf::from(&status_file);
    if let Some(parent) = status_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    write_status(&status_path, false, 0);

    let mut backoff = 1u64;
    let mut encoders: Vec<String> = Vec::new();
    let mut gpu_count: usize = 1;
    let mut gpu_max_jobs_vec: Vec<usize> = vec![1];
    let sem = Arc::new(tokio::sync::Semaphore::new(max_jobs));
    let gpu_limits: Arc<Mutex<Vec<(u32, u32)>>> = Arc::new(Mutex::new(vec![(0, 1)]));
    let mut gpu_capabilities: Vec<serde_json::Value> = Vec::new();

    // Detect GPUs and capabilities once at startup (before connecting)
    {
        let new_encoders = crate::server::detect_encoders(&ffmpeg);
        let has_hw = new_encoders.iter().any(|e| e.contains("nvenc") || e.contains("videotoolbox") || e.contains("vaapi") || e.contains("qsv"));
        if has_hw || encoders.is_empty() {
            encoders = new_encoders;
        }
        gpu_count = detect_gpu_count();
        gpu_max_jobs_vec = read_gpu_max_jobs(gpu_count);
        let total: usize = gpu_max_jobs_vec.iter().sum();
        let effective = if total > 0 { total } else { max_jobs };
        if effective > sem.available_permits() {
            sem.add_permits(effective - sem.available_permits());
        }
        let mut lim = gpu_limits.lock().await;
        *lim = gpu_max_jobs_vec.iter().map(|&m| (0u32, m as u32)).collect();
        drop(lim);
        info!("GPU connector: {} encoders={:?} max_jobs={} gpus={} per_gpu={:?}", name, encoders, effective, gpu_count, gpu_max_jobs_vec);

        // Always scan GPU capabilities fresh on every startup
        let has_nvenc = encoders.iter().any(|e| e.contains("nvenc"));
        if has_nvenc {
            info!("Running GPU capability tests...");
            gpu_capabilities = crate::server::detect_gpu_capabilities(&ffmpeg, gpu_count);
            info!("GPU capabilities: {:?}", gpu_capabilities);
        }
    }

    loop {
        // Re-detect encoders on reconnect only if no hardware encoders found yet
        let already_has_hw = encoders.iter().any(|e| e.contains("nvenc") || e.contains("videotoolbox") || e.contains("vaapi") || e.contains("qsv"));
        if !already_has_hw {
            let new_encoders = crate::server::detect_encoders(&ffmpeg);
            let has_hw = new_encoders.iter().any(|e| e.contains("nvenc") || e.contains("videotoolbox") || e.contains("vaapi") || e.contains("qsv"));
            if has_hw || encoders.is_empty() {
                encoders = new_encoders;
            }
        }
        let new_gpu_count = if gpu_count <= 1 { detect_gpu_count() } else { gpu_count };
        if new_gpu_count > gpu_count {
            gpu_count = new_gpu_count;
            gpu_max_jobs_vec = read_gpu_max_jobs(gpu_count);
            let total: usize = gpu_max_jobs_vec.iter().sum();
            let effective = if total > 0 { total } else { max_jobs };
            if effective > sem.available_permits() + active_jobs.load(Ordering::Relaxed) as usize {
                sem.add_permits(effective - sem.available_permits() - active_jobs.load(Ordering::Relaxed) as usize);
            }
            let mut lim = gpu_limits.lock().await;
            *lim = gpu_max_jobs_vec.iter().map(|&m| (0u32, m as u32)).collect();
            drop(lim);
            info!("GPU count changed: {} → {}", gpu_count, new_gpu_count);
        }
        let effective_max: usize = gpu_max_jobs_vec.iter().sum::<usize>().max(max_jobs.min(1));

        info!("Connecting to client at {}...", address);
        write_status(&status_path, false, active_jobs.load(Ordering::Relaxed));
        match connect_and_run(
            &address, &secret, &name, &ffmpeg, &tmp_dir,
            &sem, &active_jobs, &encoders, effective_max, &status_path,
            gpu_count, &gpu_limits, &active_job_ids, &gpu_capabilities,
        ).await {
            Ok(()) => {
                info!("Disconnected from {}", address);
                write_status_err(&status_path, false, active_jobs.load(Ordering::Relaxed), "Disconnected");
                backoff = 1;
            }
            Err(e) => {
                let err_msg = format!("{}", e);
                warn!("Connection to {} failed: {}", address, err_msg);
                write_status_err(&status_path, false, active_jobs.load(Ordering::Relaxed), &err_msg);
                backoff = (backoff * 2).min(30);
            }
        }
        info!("Reconnecting in {}s...", backoff);
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
    }
}

fn write_status(path: &PathBuf, connected: bool, active_jobs: u32) {
    write_status_err(path, connected, active_jobs, "");
}

fn write_status_err(path: &PathBuf, connected: bool, active_jobs: u32, error: &str) {
    let err_json = error.replace('\\', "\\\\").replace('"', "\\\"");
    let json = format!(r#"{{"connected":{},"active_jobs":{},"error":"{}"}}"#, connected, active_jobs, err_json);
    let _ = std::fs::write(path, json);
}

async fn connect_and_run(
    address: &str, secret: &str, name: &str, ffmpeg: &str, tmp_dir: &str,
    sem: &Arc<tokio::sync::Semaphore>, active_jobs: &Arc<AtomicU32>,
    encoders: &[String], max_jobs: usize, status_path: &PathBuf,
    gpu_count: usize, gpu_limits: &Arc<Mutex<Vec<(u32, u32)>>>,
    active_job_ids: &Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    gpu_capabilities: &[serde_json::Value],
) -> Result<()> {
    let mut stream = TcpStream::connect(address).await.context("connect failed")?;
    stream.set_nodelay(true)?;

    // Send connection type prefix
    stream.write_all(&[CONN_TYPE_CONTROL]).await?;

    let (mut rx, mut tx) = stream.split();

    // Authenticate
    write_msg(&mut tx, &ReverseControlMsg::Auth {
        secret: secret.to_string(),
        server_name: name.to_string(),
        encoders: encoders.to_vec(),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        max_jobs,
        has_fuse: cfg!(feature = "fuse"),
        gpu_capabilities: serde_json::to_string(gpu_capabilities).unwrap_or_default(),
    }).await?;
    tx.flush().await?;

    match read_msg::<ReverseControlMsg, _>(&mut rx).await? {
        ReverseControlMsg::AuthOk => {
            info!("Authenticated with {}", address);
            write_status(status_path, true, active_jobs.load(Ordering::Relaxed));
        }
        ReverseControlMsg::AuthFail(e) => bail!("Auth failed: {}", e),
        _ => bail!("Unexpected response"),
    }

    // Channel for spawned jobs to notify completion
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<(String, i32, String)>(16);

    let mut heartbeat_interval = tokio::time::interval(
        std::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS)
    );

    loop {
        tokio::select! {
            _ = heartbeat_interval.tick() => {
                let hb = ReverseControlMsg::Heartbeat {
                    active_jobs: active_jobs.load(Ordering::Relaxed),
                };
                write_msg(&mut tx, &hb).await?;
                tx.flush().await?;
            }
            msg = read_msg::<ReverseControlMsg, _>(&mut rx) => {
                match msg? {
                    ReverseControlMsg::JobAssignment { job_id, ffmpeg_args, input_files, output_path, connect_port, post_commands } => {
                        let permit = match sem.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                write_msg(&mut tx, &ReverseControlMsg::JobReject {
                                    job_id, reason: "At capacity".into()
                                }).await?;
                                tx.flush().await?;
                                continue;
                            }
                        };
                        write_msg(&mut tx, &ReverseControlMsg::JobAccepted { job_id: job_id.clone() }).await?;
                        tx.flush().await?;
                        active_jobs.fetch_add(1, Ordering::Relaxed);

                        let addr = address.to_string();
                        let sec = secret.to_string();
                        let ff = ffmpeg.to_string();
                        let td = tmp_dir.to_string();
                        let active = active_jobs.clone();
                        let done = done_tx.clone();
                        let enc = encoders.to_vec();
                        let gc = gpu_count;
                        let gl = gpu_limits.clone();
                        let aj: Arc<std::sync::Mutex<std::collections::HashSet<String>>> = active_job_ids.clone();

                        // Register job as active (GC won't touch it)
                        aj.lock().unwrap().insert(job_id.clone());

                        // Assign GPU: pick least-loaded GPU that's under its per-GPU limit
                        let assigned_gpu = {
                            let mut limits = gl.lock().await;
                            let mut best: Option<(usize, u32)> = None;
                            for (i, &(load, max)) in limits.iter().enumerate() {
                                if max == 0 { continue; } // disabled
                                if load >= max { continue; } // at capacity
                                match best {
                                    None => best = Some((i, load)),
                                    Some((_, best_load)) if load < best_load => best = Some((i, load)),
                                    _ => {}
                                }
                            }
                            let gpu = best.map(|(i, _)| i).unwrap_or(0) as u32;
                            if (gpu as usize) < limits.len() {
                                limits[gpu as usize].0 += 1;
                            }
                            info!("Assigned GPU {} (limits: {:?})", gpu, limits.iter().map(|(l,m)| format!("{}/{}", l, m)).collect::<Vec<_>>());
                            gpu
                        };

                        tokio::spawn(async move {
                            // Strip unsupported flags based on GPU capabilities
                            let ffmpeg_args = strip_unsupported_flags(ffmpeg_args, &enc);
                            // Assign GPU: rewrite -gpu and -hwaccel_device flags
                            let ffmpeg_args = if gc > 1 {
                                assign_gpu(ffmpeg_args, assigned_gpu)
                            } else {
                                ffmpeg_args
                            };
                            let (exit_code, stderr) = match execute_job(
                                &addr, &sec, &ff, &td,
                                &job_id, ffmpeg_args, input_files, &output_path, connect_port,
                                post_commands,
                            ).await {
                                Ok((ec, se)) => (ec, se),
                                Err(e) => {
                                    error!("Job {} failed: {}", job_id, e);
                                    (1, format!("{}", e))
                                }
                            };
                            active.fetch_sub(1, Ordering::Relaxed);
                            // Remove from active set (GC can now clean up)
                            aj.lock().unwrap().remove(&job_id);
                            // Decrement GPU load
                            {
                                let mut limits = gl.lock().await;
                                if (assigned_gpu as usize) < limits.len() {
                                    limits[assigned_gpu as usize].0 = limits[assigned_gpu as usize].0.saturating_sub(1);
                                }
                            }
                            drop(permit);
                            let _ = done.send((job_id, exit_code, stderr)).await;
                        });
                    }
                    _ => {}
                }
            }
            Some((job_id, exit_code, stderr)) = done_rx.recv() => {
                // Notify client that job is done
                write_msg(&mut tx, &ReverseControlMsg::JobFinished { job_id, exit_code, stderr }).await?;
                tx.flush().await?;
            }
        }
    }
}

/// Strip ffmpeg flags that this GPU doesn't support
fn strip_unsupported_flags(mut args: Vec<String>, encoders: &[String]) -> Vec<String> {
    let needs_strip_temporal_aq = encoders.iter()
        .any(|e| e.contains("hevc_nvenc/no-temporal-aq") || e.contains("h264_nvenc/no-temporal-aq"));
    if needs_strip_temporal_aq {
        // Remove -temporal-aq and its value
        let mut i = 0;
        let mut filtered = Vec::new();
        while i < args.len() {
            if args[i] == "-temporal-aq" && i + 1 < args.len() {
                info!("Stripping -temporal-aq (unsupported by this GPU)");
                i += 2; // skip flag and value
            } else {
                filtered.push(args[i].clone());
                i += 1;
            }
        }
        args = filtered;
    }
    args
}

/// Rewrite ffmpeg args to use a specific GPU index
fn assign_gpu(mut args: Vec<String>, gpu: u32) -> Vec<String> {
    let gpu_str = gpu.to_string();
    let mut i = 0;
    let mut found_gpu = false;
    let mut found_hwaccel_device = false;
    while i < args.len() {
        if args[i] == "-gpu" && i + 1 < args.len() {
            args[i + 1] = gpu_str.clone();
            found_gpu = true;
            i += 2;
        } else if args[i] == "-hwaccel_device" && i + 1 < args.len() {
            args[i + 1] = gpu_str.clone();
            found_hwaccel_device = true;
            i += 2;
        } else {
            i += 1;
        }
    }
    // Add -hwaccel_device after -hwaccel if present
    if !found_hwaccel_device {
        if let Some(pos) = args.iter().position(|a| a == "-hwaccel") {
            args.insert(pos + 2, gpu_str.clone());
            args.insert(pos + 2, "-hwaccel_device".to_string());
        }
    }
    // Add -gpu after -c:v <encoder> — works for hevc_nvenc, h264_nvenc, etc.
    if !found_gpu {
        if let Some(pos) = args.iter().position(|a| a == "-c:v") {
            // Check if the encoder is GPU-based (nvenc)
            if pos + 1 < args.len() && args[pos + 1].contains("nvenc") {
                args.insert(pos + 2, gpu_str.clone());
                args.insert(pos + 2, "-gpu".to_string());
            }
        }
    }
    info!("Assigned GPU {}: -gpu={} -hwaccel_device={}", gpu, found_gpu || args.iter().any(|a| a == "-gpu"), found_hwaccel_device || args.iter().any(|a| a == "-hwaccel_device"));
    args
}

/// Garbage collector — periodically cleans up orphaned job dirs.
/// Uses the active_job_ids set to determine what's still running — never uses pgrep.
/// Runs every 60 seconds. Only cleans dirs NOT in the active set.
fn spawn_gc(tmp_dir: String, active_job_ids: Arc<std::sync::Mutex<std::collections::HashSet<String>>>) {
    // Run GC in a dedicated OS thread to avoid blocking the tokio runtime
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(60));
            let rrp_dir = PathBuf::from(&tmp_dir);
            let entries = match std::fs::read_dir(&rrp_dir) {
                Ok(e) => e,
                Err(_) => continue,
            };
            // Get snapshot of active job IDs
            let active: std::collections::HashSet<String> = {
                // Use try_lock to avoid blocking if the mutex is held
                match active_job_ids.try_lock() {
                    Ok(guard) => guard.clone(),
                    Err(_) => continue, // skip this cycle if locked
                }
            };

            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() { continue; }
                let dir_name = match path.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };
                // Skip non-job dirs (status files, etc.)
                if dir_name.len() < 8 { continue; }

                // If this job is in the active set, don't touch it
                if active.contains(&dir_name) { continue; }

                // Orphaned dir — clean up
                // Step 1: Unmount FUSE if present
                let mnt = path.join("mnt");
                if mnt.exists() {
                    #[cfg(feature = "fuse")]
                    crate::server::fuse_unmount(&mnt);
                    std::thread::sleep(std::time::Duration::from_secs(2));
                }
                // Step 2: Delete files
                match std::fs::remove_dir_all(&path) {
                    Ok(_) => info!("GC: cleaned orphaned job dir {}", dir_name),
                    Err(e) => warn!("GC: failed to remove {}: {}", dir_name, e),
                }
            }
        }
    });
}

async fn execute_job(
    address: &str, secret: &str, ffmpeg: &str, tmp_dir: &str,
    job_id: &str, ffmpeg_args: Vec<String>, input_files: Vec<FileInfo>,
    output_path: &str, connect_port: u16,
    post_commands: Vec<String>,
) -> Result<(i32, String)> {
    // Connect data channel to the client
    let host = address.split(':').next().unwrap_or(address);
    let data_addr = format!("{}:{}", host, connect_port);
    info!("Job {}: opening data connection to {}", job_id, data_addr);
    let mut stream = TcpStream::connect(&data_addr).await
        .context("data connection failed")?;
    stream.set_nodelay(true)?;

    // Send data connection type prefix + auth
    stream.write_all(&[CONN_TYPE_DATA]).await?;
    write_msg(&mut stream, &DataConnect {
        job_id: job_id.to_string(),
        secret: secret.to_string(),
    }).await?;
    stream.flush().await?;

    let (mut rx, mut tx) = stream.split();

    // Create job dir
    let job_dir = PathBuf::from(tmp_dir).join(job_id);
    std::fs::create_dir_all(&job_dir)?;
    let _cleanup = crate::server::CleanupGuard(job_dir.clone());

    // Run FUSE job using the extracted function from server.rs
    #[cfg(feature = "fuse")]
    let exit_code = crate::server::run_fuse_job(
        &mut rx, &mut tx, ffmpeg, &ffmpeg_args, &input_files,
        &job_dir, job_id, &data_addr, &post_commands,
    ).await?;

    #[cfg(not(feature = "fuse"))]
    let exit_code = {
        warn!("FUSE not available — cannot run reverse-connect job");
        1
    };

    // Read stderr BEFORE CleanupGuard drops and deletes the dir
    let stderr_path = job_dir.join("ffmpeg_stderr.log");
    let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
    let stderr = if stderr.len() > 2000 { stderr[stderr.len()-2000..].to_string() } else { stderr };

    Ok((exit_code, stderr))
}
