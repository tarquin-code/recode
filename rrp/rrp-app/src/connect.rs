use anyhow::{Result, Context, bail};
use rrp_proto::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn, error};

/// Detect number of GPUs via nvidia-smi
fn detect_gpu_count() -> usize {
    match std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=index", "--format=csv,noheader"])
        .output()
    {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).lines().count()
        }
        _ => 1,
    }
}

pub async fn run(
    address: String, secret: String, name: String,
    ffmpeg: String, tmp_dir: String, max_jobs: usize,
    status_file: String,
) -> Result<()> {
    std::fs::create_dir_all(&tmp_dir)?;
    let sem = Arc::new(tokio::sync::Semaphore::new(max_jobs));
    let active_jobs = Arc::new(AtomicU32::new(0));
    let encoders = crate::server::detect_encoders(&ffmpeg);
    let gpu_count = detect_gpu_count();
    let next_gpu = Arc::new(AtomicU32::new(0));

    info!("GPU connector: {} encoders={:?} max_jobs={} gpus={}", name, encoders, max_jobs, gpu_count);

    let status_path = PathBuf::from(&status_file);
    if let Some(parent) = status_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    write_status(&status_path, false, 0);

    let mut backoff = 1u64;
    loop {
        info!("Connecting to client at {}...", address);
        write_status(&status_path, false, active_jobs.load(Ordering::Relaxed));
        match connect_and_run(
            &address, &secret, &name, &ffmpeg, &tmp_dir,
            &sem, &active_jobs, &encoders, max_jobs, &status_path,
            gpu_count, &next_gpu,
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
    gpu_count: usize, next_gpu: &Arc<AtomicU32>,
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
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<(String, i32)>(16);

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
                    ReverseControlMsg::JobAssignment { job_id, ffmpeg_args, input_files, output_path, connect_port } => {
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
                        // Assign GPU round-robin
                        let assigned_gpu = next_gpu.fetch_add(1, Ordering::Relaxed) % gpu_count as u32;
                        let gc = gpu_count;

                        tokio::spawn(async move {
                            // Strip unsupported flags based on GPU capabilities
                            let ffmpeg_args = strip_unsupported_flags(ffmpeg_args, &enc);
                            // Assign GPU: rewrite -gpu and -hwaccel_device flags
                            let ffmpeg_args = if gc > 1 {
                                assign_gpu(ffmpeg_args, assigned_gpu)
                            } else {
                                ffmpeg_args
                            };
                            let exit_code = match execute_job(
                                &addr, &sec, &ff, &td,
                                &job_id, ffmpeg_args, input_files, &output_path, connect_port,
                            ).await {
                                Ok(ec) => ec,
                                Err(e) => {
                                    error!("Job {} failed: {}", job_id, e);
                                    1
                                }
                            };
                            active.fetch_sub(1, Ordering::Relaxed);
                            drop(permit);
                            let _ = done.send((job_id, exit_code)).await;
                        });
                    }
                    _ => {}
                }
            }
            Some((job_id, exit_code)) = done_rx.recv() => {
                // Notify client that job is done
                write_msg(&mut tx, &ReverseControlMsg::JobFinished { job_id, exit_code }).await?;
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
    // Add flags if not present
    if !found_hwaccel_device {
        // Insert -hwaccel_device after -hwaccel if present
        if let Some(pos) = args.iter().position(|a| a == "-hwaccel") {
            args.insert(pos + 2, gpu_str.clone());
            args.insert(pos + 2, "-hwaccel_device".to_string());
        }
    }
    if !found_gpu {
        // Insert -gpu before the encoder flag (-c:v)
        if let Some(pos) = args.iter().position(|a| a == "-c:v") {
            args.insert(pos + 2, gpu_str.clone());
            args.insert(pos + 2, "-gpu".to_string());
        }
    }
    info!("Assigned GPU {} for this job", gpu);
    args
}

async fn execute_job(
    address: &str, secret: &str, ffmpeg: &str, tmp_dir: &str,
    job_id: &str, ffmpeg_args: Vec<String>, input_files: Vec<FileInfo>,
    output_path: &str, connect_port: u16,
) -> Result<i32> {
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
        &job_dir, job_id, &data_addr,
    ).await?;

    #[cfg(not(feature = "fuse"))]
    let exit_code = {
        warn!("FUSE not available — cannot run reverse-connect job");
        1
    };

    Ok(exit_code)
}
