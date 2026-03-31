use anyhow::{Context, Result, bail};
#[cfg(feature = "fuse")]
use fuser::{Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};
use rrp_proto::*;
use sha2::{Sha256, Digest};
#[cfg(feature = "fuse")]
use std::collections::HashMap;
#[cfg(feature = "fuse")]
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tracing::{error, info, warn};

#[cfg(feature = "fuse")]
const TTL: Duration = Duration::from_secs(3600);

/// Removes the job directory on drop — guarantees cleanup on cancel, disconnect, or error.
/// Note: ffmpeg should already be killed/waited by run_fuse_job before this runs.
/// This is a safety net for unexpected drops.
pub struct CleanupGuard(pub PathBuf);
impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if !self.0.exists() { return; }

        // Unmount FUSE first (ffmpeg should already be dead at this point)
        #[cfg(feature = "fuse")]
        {
            let mnt = self.0.join("mnt");
            if mnt.exists() {
                fuse_unmount(&mnt);
            }
        }

        // Delete all files
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("Cleanup failed for {:?}: {}", self.0, e);
        }
    }
}

#[cfg(feature = "fuse")]
/// Platform-specific FUSE unmount — uses lazy unmount to handle busy mounts
pub fn fuse_unmount(path: &std::path::Path) {
    let path_str = path.to_string_lossy();
    #[cfg(target_os = "linux")]
    {
        // Try normal unmount first, then lazy unmount if busy
        let _ = std::process::Command::new("fusermount3").args(["-u", path_str.as_ref()]).output();
        let _ = std::process::Command::new("fusermount").args(["-u", path_str.as_ref()]).output();
        let _ = std::process::Command::new("fusermount3").args(["-uz", path_str.as_ref()]).output();
        let _ = std::process::Command::new("fusermount").args(["-uz", path_str.as_ref()]).output();
    }
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("diskutil").args(["unmount", "force", path_str.as_ref()]).output();
        let _ = std::process::Command::new("umount").args(["-f", path_str.as_ref()]).output();
    }
}

pub async fn run(port: u16, secret: String, ffmpeg: String, tmp_dir: String, max_jobs: usize) -> Result<()> {
    info!("RRP server (TCP) on port {} (max {} jobs)", port, max_jobs);
    std::fs::create_dir_all(&tmp_dir)?;

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    let secret = Arc::new(secret);
    let ffmpeg = Arc::new(ffmpeg);
    let tmp_dir = Arc::new(tmp_dir);
    let sem = Arc::new(tokio::sync::Semaphore::new(max_jobs));

    loop {
        let (stream, peer) = listener.accept().await?;
        let secret = secret.clone();
        let ffmpeg = ffmpeg.clone();
        let tmp_dir = tmp_dir.clone();
        let sem = sem.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, peer, &secret, &ffmpeg, &tmp_dir, sem).await {
                error!("{}: {}", peer, e);
            }
        });
    }
}

async fn handle(mut stream: TcpStream, peer: SocketAddr, secret: &str, ffmpeg: &str, tmp_dir: &str, sem: Arc<tokio::sync::Semaphore>) -> Result<()> {
    info!("Connection from {}", peer);
    stream.set_nodelay(true)?;
    let (mut rx, mut tx) = stream.split();

    // Auth
    let auth: ControlMsg = read_msg(&mut rx).await.context("read auth")?;
    match auth {
        ControlMsg::Auth { timestamp, hmac } => {
            if !verify_hmac(secret, timestamp, &hmac) {
                write_msg(&mut tx, &ControlMsg::AuthFail("bad auth".into())).await?;
                bail!("Auth failed from {}", peer);
            }
            write_msg(&mut tx, &ControlMsg::AuthOk).await?;
        }
        _ => bail!("Expected Auth"),
    }

    // Next message: GetInfo (ping with capabilities), SubmitJob, or disconnect
    let job: ControlMsg = match read_msg(&mut rx).await {
        Ok(msg) => msg,
        Err(_) => {
            info!("{}: ping/disconnect after auth", peer);
            return Ok(());
        }
    };

    // Handle GetInfo request (ping with capabilities)
    if matches!(job, ControlMsg::GetInfo) {
        let encoders = detect_encoders(ffmpeg);
        let os = std::env::consts::OS.to_string();
        let arch = std::env::consts::ARCH.to_string();
        let has_fuse = cfg!(feature = "fuse");
        info!("{}: info request — encoders: {:?}, {}/{}, fuse={}", peer, encoders, os, arch, has_fuse);
        write_msg(&mut tx, &ControlMsg::ServerInfo { encoders, os, arch, has_fuse }).await?;
        return Ok(());
    }

    let (job_id, ffmpeg_args, input_files, _out_path, transfer_mode) = match job {
        ControlMsg::SubmitJob { job_id, ffmpeg_args, input_files, output_path, transfer_mode } =>
            (job_id, ffmpeg_args, input_files, output_path, transfer_mode),
        _ => bail!("Expected SubmitJob"),
    };

    let _permit = sem.acquire().await?;
    let job_dir = PathBuf::from(tmp_dir).join(&job_id);
    std::fs::create_dir_all(&job_dir)?;
    let _cleanup = CleanupGuard(job_dir.clone());
    write_msg(&mut tx, &ControlMsg::JobAccepted).await?;
    info!("Job {}: {} inputs, mode={:?}", job_id, input_files.len(), transfer_mode);

    // Write marker file so the Recode UI can identify RRP jobs cross-user
    let marker = job_dir.join(".rrp_info");
    let orig_input = input_files.first().map(|f| f.original_path.as_str()).unwrap_or("");
    let marker_json = format!(r#"{{"client":"{}","input":"{}","job_id":"{}"}}"#,
        peer.ip(), orig_input.replace('\\', "\\\\").replace('"', "\\\""), job_id);
    let _ = std::fs::write(&marker, &marker_json);

    #[cfg(feature = "fuse")]
    let mount_dir = job_dir.join("mnt");

    // Determine effective transfer mode (fall back to Upload if FUSE not compiled in)
    #[cfg(not(feature = "fuse"))]
    let transfer_mode = TransferMode::Upload;

    #[cfg(feature = "fuse")]
    if transfer_mode == TransferMode::Mount {
        let exit_code = run_fuse_job(
            &mut rx, &mut tx, ffmpeg, &ffmpeg_args, &input_files,
            &job_dir, &job_id, &peer.ip().to_string(), &[],
        ).await?;
        let _ = write_tagged(&mut tx, TAG_CONTROL, &ControlMsg::JobComplete { exit_code }).await;
        let _ = tx.flush().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        info!("Job {} done (FUSE mode), exit {}", job_id, exit_code);

    } else {
        // --- Upload mode (original behavior) ---
        for (i, fi) in input_files.iter().enumerate() {
            info!("Receiving file {}: {} ({:.1} MB)", i, fi.virtual_name, fi.size as f64 / 1_048_576.0);
            let local_path = job_dir.join(&fi.virtual_name);
            let mut file = tokio::fs::File::create(&local_path).await?;
            let mut remaining = fi.size;
            let mut buf = vec![0u8; CHUNK_SIZE];
            while remaining > 0 {
                let to_read = std::cmp::min(remaining as usize, buf.len());
                let n = rx.read(&mut buf[..to_read]).await?;
                if n == 0 { bail!("Connection closed during file transfer"); }
                file.write_all(&buf[..n]).await?;
                remaining -= n as u64;
            }
            file.flush().await?;
            info!("Received {} ({:.1} MB)", fi.virtual_name, fi.size as f64 / 1_048_576.0);
        }

        let output_local = job_dir.join("output.mkv");
        let rw = rewrite_args(&ffmpeg_args, &input_files, &job_dir, &output_local);
        info!("ffmpeg {}", rw.join(" "));

        let mut child = Command::new(ffmpeg)
            .args(&rw)
            .env("RRP_JOB_ID", &job_id)
            .env("RRP_CLIENT", peer.ip().to_string())
            .env("RRP_INPUT", orig_input)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stderr = child.stderr.take().unwrap();
        let mut stderr_reader = BufReader::new(stderr);
        let mut stderr_buf = vec![0u8; 4096];
        let mut line_buf = String::new();

        loop {
            tokio::select! {
                result = stderr_reader.read(&mut stderr_buf) => {
                    match result {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            line_buf.push_str(&String::from_utf8_lossy(&stderr_buf[..n]));
                            let parts: Vec<&str> = line_buf.split(|c| c == '\r' || c == '\n').collect();
                            let incomplete = parts.last().copied().unwrap_or("");
                            for part in &parts[..parts.len().saturating_sub(1)] {
                                let line = part.trim();
                                if line.starts_with("frame=") {
                                    if let Some(p) = parse_progress(line) {
                                        let _ = write_msg(&mut tx, &p).await;
                                    }
                                }
                            }
                            line_buf = incomplete.to_string();
                        }
                    }
                }
                cancel = read_msg::<ControlMsg, _>(&mut rx) => {
                    match cancel {
                        Ok(ControlMsg::CancelJob) => {
                            warn!("Job cancelled by client");
                            let _ = child.kill().await;
                            break;
                        }
                        Err(_) => {
                            warn!("Client disconnected, killing ffmpeg");
                            let _ = child.kill().await;
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        let status = child.wait().await?;
        let exit_code = status.code().unwrap_or(1);

        if exit_code == 0 && output_local.exists() {
            let meta = tokio::fs::metadata(&output_local).await?;
            let total = meta.len();
            info!("Sending output: {:.1} MB", total as f64 / 1_048_576.0);
            tx.write_all(&[STREAM_OUTPUT]).await?;
            write_msg(&mut tx, &total).await?;
            let mut file = tokio::fs::File::open(&output_local).await?;
            let mut hasher = Sha256::new();
            let mut buf = vec![0u8; CHUNK_SIZE];
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 { break; }
                hasher.update(&buf[..n]);
                tx.write_all(&buf[..n]).await?;
            }
            let hash: [u8; 32] = hasher.finalize().into();
            tx.write_all(&hash).await?;
            tx.flush().await?;
            info!("Output sent");
        }

        let _ = write_msg(&mut tx, &ControlMsg::JobComplete { exit_code }).await;
        let _ = tx.flush().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        info!("Job {} done (upload mode), exit {}", job_id, exit_code);
    }

    Ok(())
}

/// Run a FUSE-mounted encode job over an established TCP connection.
/// Used by both the traditional server (client connects in) and
/// reverse-connect mode (GPU server connects out to client).
/// Returns the ffmpeg exit code.
#[cfg(feature = "fuse")]
pub async fn run_fuse_job(
    rx: &mut (impl AsyncReadExt + Unpin),
    tx: &mut (impl AsyncWriteExt + Unpin),
    ffmpeg: &str,
    ffmpeg_args: &[String],
    input_files: &[FileInfo],
    job_dir: &PathBuf,
    job_id: &str,
    peer: &str,
    post_commands: &[String],
) -> Result<i32> {
    let mount_dir = job_dir.join("mnt");
    std::fs::create_dir_all(&mount_dir)?;

    let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<FuseReadRequest>(32);

    let fs = RrpFuse::new(input_files.to_vec(), req_tx);
    let mount_path = mount_dir.clone();
    let mount_ok = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mount_ok2 = mount_ok.clone();
    let fuse_handle = std::thread::spawn(move || {
        let mut options = vec![
            MountOption::RO,
            MountOption::FSName("rrp".into()),
            MountOption::AutoUnmount,
            MountOption::CUSTOM(format!("max_read={}", CHUNK_SIZE)),
        ];
        #[cfg(not(target_os = "macos"))]
        options.push(MountOption::AllowOther);
        #[cfg(target_os = "macos")]
        {
            options.push(MountOption::CUSTOM("nobrowse".into()));
            options.push(MountOption::CUSTOM("defer_permissions".into()));
            options.push(MountOption::CUSTOM("noappledouble".into()));
            options.push(MountOption::CUSTOM("noalerts".into()));
            options.push(MountOption::CUSTOM("noapplexattr".into()));
        }
        match fuser::mount2(fs, &mount_path, &options) {
            Ok(()) => { /* mount ran and unmounted normally */ }
            Err(e) => {
                mount_ok2.store(false, std::sync::atomic::Ordering::SeqCst);
                error!("FUSE mount error: {}", e);
            }
        }
    });

    let mut mounted = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if mount_dir.join(".").exists() && std::fs::read_dir(&mount_dir).map(|mut d| d.next().is_some()).unwrap_or(false) {
            mounted = true;
            mount_ok.store(true, std::sync::atomic::Ordering::SeqCst);
            break;
        }
        // Check if mount thread reported failure
        if !mount_ok.load(std::sync::atomic::Ordering::SeqCst) && fuse_handle.is_finished() {
            break;
        }
    }
    if !mounted {
        let _ = write_tagged(tx, TAG_CONTROL, &ControlMsg::JobError("FUSE mount failed — check macFUSE installation".into())).await;
        bail!("FUSE mount failed after 5s — mount dir empty");
    }
    info!("FUSE mounted at {:?}", mount_dir);

    let output_local = job_dir.join("output.mkv");
    let orig_input = input_files.first().map(|f| f.original_path.as_str()).unwrap_or("");
    let rw = rewrite_args(ffmpeg_args, input_files, &mount_dir, &output_local);
    info!("ffmpeg {}", rw.join(" "));

    // Use -progress to a file for reliable progress updates (pipe buffering is unreliable)
    let progress_file = job_dir.join("ffmpeg_progress.txt");
    let rw: Vec<String> = rw.iter().map(|a| {
        if a == "-stats" { "-nostats".to_string() } else { a.clone() }
    }).collect();
    let mut rw = rw;
    if !rw.iter().any(|a| a == "-progress") {
        let out = rw.pop().unwrap_or_default();
        rw.extend_from_slice(&["-progress".into(), progress_file.to_string_lossy().to_string()]);
        rw.push(out);
    }

    // Write metadata files so GUI can read them (macOS can't read process env vars)
    let _ = std::fs::write(job_dir.join("rrp_input.txt"), orig_input);
    let _ = std::fs::write(job_dir.join("rrp_client.txt"), peer);

    // Probe input duration in background (can't await here — would deadlock FUSE reads)
    {
        let ffprobe_path = {
            let p = std::path::Path::new(ffmpeg);
            if let Some(dir) = p.parent() {
                let fp = dir.join("ffprobe");
                if fp.exists() { fp.to_string_lossy().to_string() } else { "ffprobe".into() }
            } else { "ffprobe".into() }
        };
        let input_path = mount_dir.join(input_files.first().map(|f| f.virtual_name.as_str()).unwrap_or("input_0.mkv"));
        let dur_file = job_dir.join("rrp_duration.txt");
        let jid = job_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await; // wait for FUSE + ffmpeg to start
            if let Ok(output) = Command::new(&ffprobe_path)
                .args(["-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0"])
                .arg(&input_path)
                .output().await {
                let dur_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !dur_str.is_empty() {
                    let _ = std::fs::write(&dur_file, &dur_str);
                    info!("Job {}: input duration {}s", jid, dur_str);
                }
            }
        });
    }

    let stderr_path = job_dir.join("ffmpeg_stderr.log");
    let mut child = Command::new(ffmpeg)
        .args(&rw)
        .env("RRP_JOB_ID", job_id)
        .env("RRP_CLIENT", peer)
        .env("RRP_INPUT", orig_input)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;

    // Spawn a task to continuously read stderr and write to file
    let stderr = child.stderr.take().unwrap();
    let sp = stderr_path.clone();
    let stderr_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut file = std::fs::File::create(&sp).unwrap_or_else(|_| std::fs::File::create("/dev/null").unwrap());
        let mut buf = vec![0u8; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => { let _ = std::io::Write::write_all(&mut file, &buf[..n]); }
            }
        }
    });

    // Poll progress file every 500ms in a background task
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel::<ProgressMsg>(32);
    let pf = progress_file.clone();
    let progress_task = tokio::spawn(async move {
        let mut last_frame: u64 = 0;
        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if let Ok(content) = tokio::fs::read_to_string(&pf).await {
                let mut frame: u64 = 0;
                let mut time_secs: f64 = 0.0;
                let mut speed: f32 = 0.0;
                let mut bitrate: f32 = 0.0;
                let mut total_size: u64 = 0;
                fn parse_val(line: &str, prefix_len: usize) -> Option<f64> {
                    let v = line[prefix_len..].trim();
                    if v == "N/A" { None } else { v.trim_end_matches('x').trim_end_matches("kbits/s").trim().parse().ok() }
                }
                for line in content.lines() {
                    let line = line.trim();
                    if line.starts_with("frame=") { frame = parse_val(line, 6).unwrap_or(0.0) as u64; }
                    else if line.starts_with("out_time_us=") { if let Some(v) = parse_val(line, 12) { time_secs = v / 1_000_000.0; } }
                    else if line.starts_with("out_time_ms=") { if let Some(v) = parse_val(line, 12) { time_secs = v / 1_000_000.0; } }
                    else if line.starts_with("speed=") { if let Some(v) = parse_val(line, 6) { speed = v as f32; } }
                    else if line.starts_with("bitrate=") { if let Some(v) = parse_val(line, 8) { bitrate = v as f32; } }
                    else if line.starts_with("total_size=") { if let Some(v) = parse_val(line, 11) { total_size = v as u64; } }
                }
                // When CUDA hwaccel reports N/A for time/speed/bitrate, estimate
                if time_secs == 0.0 && frame > 0 {
                    let elapsed = start.elapsed().as_secs_f64();
                    time_secs = frame as f64 / 24.0; // assume ~24fps source
                    if elapsed > 0.0 { speed = time_secs as f32 / elapsed as f32; }
                }
                if bitrate == 0.0 && total_size > 0 && time_secs > 0.0 {
                    bitrate = (total_size as f64 * 8.0 / time_secs / 1000.0) as f32;
                }
                if frame > last_frame || (frame == 0 && total_size > 0 && total_size != last_frame) {
                    last_frame = if frame > 0 { frame } else { total_size };
                    let _ = progress_tx.try_send(ProgressMsg { frame, time_secs, speed, bitrate_kbps: bitrate, output_size: total_size });
                }
            }
        }
    });

    let mut child_done = false;
    let mut keepalive_interval = tokio::time::interval(Duration::from_secs(30));
    keepalive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_progress_sent = std::time::Instant::now();
    loop {
        // Drain any pending progress and send to client
        let mut sent = 0u32;
        while let Ok(p) = progress_rx.try_recv() {
            let _ = write_tagged(tx, TAG_PROGRESS, &p).await;
            sent += 1;
        }
        if sent > 0 { let _ = tx.flush().await; last_progress_sent = std::time::Instant::now(); }

        // If ffmpeg already exited, break out
        if child_done { break; }

        tokio::select! {
            _ = keepalive_interval.tick() => {
                // Only send keepalive if no real progress was sent recently
                if last_progress_sent.elapsed() > Duration::from_secs(25) {
                    let _ = write_tagged(tx, TAG_PROGRESS, &ProgressMsg {
                        frame: 0, time_secs: 0.0, speed: 0.0, bitrate_kbps: 0.0, output_size: 0,
                    }).await;
                    let _ = tx.flush().await;
                }
            }
            Some(freq) = req_rx.recv() => {
                let req = FileReadReq {
                    file_idx: freq.file_idx,
                    offset: freq.offset,
                    length: freq.length,
                };
                if write_tagged(tx, TAG_FILE_READ_REQ, &req).await.is_err() {
                    warn!("Failed to send read request to client");
                    let _ = freq.resp_tx.send(Err(()));
                    let _ = child.kill().await;
                    break;
                }
                tx.flush().await?;

                match read_tagged(rx).await {
                    Ok((TAG_FILE_READ_RESP, payload)) => {
                        match bincode::deserialize::<FileReadResp>(&payload) {
                            Ok(resp) if !resp.error => { let _ = freq.resp_tx.send(Ok(resp.data)); }
                            _ => { let _ = freq.resp_tx.send(Err(())); }
                        }
                    }
                    Ok((TAG_CONTROL, payload)) => {
                        if let Ok(ControlMsg::CancelJob) = bincode::deserialize(&payload) {
                            warn!("Job cancelled by client");
                            let _ = freq.resp_tx.send(Err(()));
                            let _ = child.kill().await;
                            break;
                        }
                    }
                    Err(_) => {
                        warn!("Client disconnected during FUSE read");
                        let _ = freq.resp_tx.send(Err(()));
                        let _ = child.kill().await;
                        break;
                    }
                    _ => { let _ = freq.resp_tx.send(Err(())); }
                }
            }
            status = child.wait() => {
                // ffmpeg exited — break to send output
                info!("ffmpeg exited: {:?}", status);
                child_done = true;
            }
        }
    }

    progress_task.abort();
    // Give stderr a moment to flush, then stop
    tokio::time::sleep(Duration::from_millis(500)).await;
    stderr_task.abort();

    // Step 1: Kill ffmpeg first, wait for it to exit
    let exit_code = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(s)) => s.code().unwrap_or(1),
        _ => {
            warn!("ffmpeg still running — killing");
            let _ = child.kill().await;
            // Wait up to 3 seconds for ffmpeg to fully exit and release GPU/file handles
            match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
                Ok(Ok(s)) => s.code().unwrap_or(1),
                _ => {
                    warn!("ffmpeg did not exit after kill — force waiting");
                    let _ = child.wait().await;
                    1
                }
            }
        }
    };

    // Run post-commands while FUSE is still mounted (source files accessible)
    let mut post_exit_code = exit_code;
    if exit_code == 0 && !post_commands.is_empty() {
        info!("Job {}: running {} post-command(s)", job_id, post_commands.len());
        let stderr_file = std::fs::OpenOptions::new()
            .create(true).append(true).open(&stderr_path).ok();
        for (idx, cmd) in post_commands.iter().enumerate() {
            // Rewrite placeholders and input file paths
            let mut rewritten = cmd.clone();
            rewritten = rewritten.replace("{JOBDIR}", &job_dir.to_string_lossy());
            rewritten = rewritten.replace("{OUTPUT}", &output_local.to_string_lossy());
            for f in input_files {
                let mount_path = mount_dir.join(&f.virtual_name);
                rewritten = rewritten.replace(&f.original_path, &mount_path.to_string_lossy());
            }
            info!("Job {} post-cmd {}/{}: {}",
                job_id, idx + 1, post_commands.len(),
                if rewritten.len() > 120 { format!("{}...", &rewritten[..120]) } else { rewritten.clone() });
            let result = Command::new("sh")
                .arg("-c")
                .arg(&rewritten)
                .stdout(Stdio::null())
                .stderr(stderr_file.as_ref()
                    .and_then(|f| f.try_clone().ok())
                    .map(|f| Stdio::from(f))
                    .unwrap_or(Stdio::null()))
                .status()
                .await;
            match result {
                Ok(status) if status.success() => {
                    info!("Job {} post-cmd {}/{} complete", job_id, idx + 1, post_commands.len());
                }
                Ok(status) => {
                    error!("Job {} post-cmd {}/{} failed (exit {:?})", job_id, idx + 1, post_commands.len(), status.code());
                    post_exit_code = status.code().unwrap_or(1);
                    break;
                }
                Err(e) => {
                    error!("Job {} post-cmd {}/{} error: {}", job_id, idx + 1, post_commands.len(), e);
                    post_exit_code = 1;
                    break;
                }
            }
        }
    }
    let exit_code = post_exit_code;

    // Send output back to client BEFORE unmounting FUSE (output is on local disk, not FUSE)
    if exit_code == 0 && output_local.exists() {
        let meta = tokio::fs::metadata(&output_local).await?;
        let total = meta.len();
        info!("Sending output: {:.1} GB ({} bytes)", total as f64 / 1_073_741_824.0, total);
        write_tagged(tx, STREAM_OUTPUT, &total).await?;
        let mut file = tokio::fs::File::open(&output_local).await?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; CHUNK_SIZE];
        let mut sent: u64 = 0;
        let mut last_log = std::time::Instant::now();
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 { break; }
            hasher.update(&buf[..n]);
            // Timeout each write to detect stalled connections (5 minutes per chunk)
            match tokio::time::timeout(Duration::from_secs(300), tx.write_all(&buf[..n])).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!("Output transfer write error at {:.1} GB: {}", sent as f64 / 1_073_741_824.0, e);
                    return Err(e.into());
                }
                Err(_) => {
                    error!("Output transfer stalled at {:.1} GB — write timeout after 5 minutes", sent as f64 / 1_073_741_824.0);
                    return Err(anyhow::anyhow!("Output transfer stalled"));
                }
            }
            sent += n as u64;
            // Log progress every 30 seconds
            if last_log.elapsed() >= Duration::from_secs(30) {
                let pct = if total > 0 { (sent as f64 / total as f64) * 100.0 } else { 0.0 };
                info!("Output transfer: {:.1} GB / {:.1} GB ({:.0}%)", sent as f64 / 1_073_741_824.0, total as f64 / 1_073_741_824.0, pct);
                last_log = std::time::Instant::now();
            }
        }
        let hash: [u8; 32] = hasher.finalize().into();
        tx.write_all(&hash).await?;
        tx.flush().await?;
        info!("Output sent: {:.1} GB", sent as f64 / 1_073_741_824.0);
    }

    // Send JobComplete so the listener knows the final exit code
    let _ = write_tagged(tx, TAG_CONTROL, &ControlMsg::JobComplete { exit_code }).await;
    let _ = tx.flush().await;

    // Unmount FUSE after output is sent (safe to unmount now)
    tokio::time::sleep(Duration::from_secs(1)).await;
    fuse_unmount(&mount_dir);
    // Join with timeout — don't block forever if macFUSE unmount hangs
    let fuse_join = tokio::task::spawn_blocking(move || { let _ = fuse_handle.join(); });
    let _ = tokio::time::timeout(Duration::from_secs(10), fuse_join).await;

    Ok(exit_code)
}

fn rewrite_args(ffmpeg_args: &[String], input_files: &[FileInfo], input_dir: &PathBuf, output_local: &PathBuf) -> Vec<String> {
    let mut rw = Vec::new();
    let mut i = 0;
    while i < ffmpeg_args.len() {
        if ffmpeg_args[i] == "-i" && i + 1 < ffmpeg_args.len() {
            rw.push("-i".into());
            let orig = &ffmpeg_args[i + 1];
            let mapped = input_files.iter().find(|f| f.original_path == *orig)
                .map(|f| input_dir.join(&f.virtual_name).to_string_lossy().to_string())
                .unwrap_or_else(|| orig.clone());
            rw.push(mapped);
            i += 2;
        } else if i == ffmpeg_args.len() - 1 {
            rw.push(output_local.to_string_lossy().to_string());
            i += 1;
        } else {
            rw.push(ffmpeg_args[i].clone());
            i += 1;
        }
    }
    rw
}

// ============ FUSE Filesystem ============

#[cfg(feature = "fuse")]
struct FuseReadRequest {
    file_idx: u32,
    offset: u64,
    length: u32,
    resp_tx: std::sync::mpsc::Sender<Result<Vec<u8>, ()>>,
}

#[cfg(feature = "fuse")]
struct RrpFuse {
    files: Vec<FileInfo>,
    inode_map: HashMap<u64, usize>,
    name_map: HashMap<String, u64>,
    req_tx: tokio::sync::mpsc::Sender<FuseReadRequest>,
}

#[cfg(feature = "fuse")]
impl RrpFuse {
    fn new(files: Vec<FileInfo>, req_tx: tokio::sync::mpsc::Sender<FuseReadRequest>) -> Self {
        let mut inode_map = HashMap::new();
        let mut name_map = HashMap::new();
        for (i, f) in files.iter().enumerate() {
            let ino = (i + 2) as u64;
            inode_map.insert(ino, i);
            name_map.insert(f.virtual_name.clone(), ino);
        }
        RrpFuse { files, inode_map, name_map, req_tx }
    }

    fn file_attr(&self, ino: u64) -> Option<fuser::FileAttr> {
        if ino == 1 { return Some(dir_attr(1)); }
        let idx = self.inode_map.get(&ino)?;
        let fi = &self.files[*idx];
        Some(fuser::FileAttr {
            ino, size: fi.size, blocks: (fi.size + 511) / 512,
            atime: SystemTime::UNIX_EPOCH, mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH, crtime: SystemTime::UNIX_EPOCH,
            kind: fuser::FileType::RegularFile, perm: 0o444, nlink: 1,
            uid: 0, gid: 0, rdev: 0, blksize: 512, flags: 0,
        })
    }
}

#[cfg(feature = "fuse")]
fn dir_attr(ino: u64) -> fuser::FileAttr {
    fuser::FileAttr {
        ino, size: 0, blocks: 0,
        atime: SystemTime::UNIX_EPOCH, mtime: SystemTime::UNIX_EPOCH,
        ctime: SystemTime::UNIX_EPOCH, crtime: SystemTime::UNIX_EPOCH,
        kind: fuser::FileType::Directory, perm: 0o555, nlink: 2,
        uid: 0, gid: 0, rdev: 0, blksize: 512, flags: 0,
    }
}

#[cfg(feature = "fuse")]
impl Filesystem for RrpFuse {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent != 1 { reply.error(libc::ENOENT); return; }
        let name_str = name.to_string_lossy();
        if let Some(&ino) = self.name_map.get(name_str.as_ref()) {
            if let Some(attr) = self.file_attr(ino) { reply.entry(&TTL, &attr, 0); return; }
        }
        reply.error(libc::ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if let Some(attr) = self.file_attr(ino) { reply.attr(&TTL, &attr); }
        else { reply.error(libc::ENOENT); }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        let idx = match self.inode_map.get(&ino) {
            Some(&i) => i,
            None => { reply.error(libc::ENOENT); return; }
        };
        let fi = &self.files[idx];
        let offset = offset as u64;
        if offset >= fi.size { reply.data(&[]); return; }
        let remaining = fi.size - offset;
        let read_len = std::cmp::min(size as u64, remaining) as u32;

        let (resp_tx, resp_rx) = std::sync::mpsc::channel();
        let freq = FuseReadRequest { file_idx: idx as u32, offset, length: read_len, resp_tx };
        if self.req_tx.blocking_send(freq).is_err() { reply.error(libc::EIO); return; }
        match resp_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(data)) => reply.data(&data),
            _ => reply.error(libc::EIO),
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if ino != 1 { reply.error(libc::ENOENT); return; }
        let mut entries: Vec<(u64, fuser::FileType, String)> = vec![
            (1, fuser::FileType::Directory, ".".into()),
            (1, fuser::FileType::Directory, "..".into()),
        ];
        for (i, f) in self.files.iter().enumerate() {
            entries.push(((i + 2) as u64, fuser::FileType::RegularFile, f.virtual_name.clone()));
        }
        for (idx, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.0, (idx + 1) as i64, entry.1, entry.2) { break; }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        if self.inode_map.contains_key(&ino) { reply.opened(0, 0); }
        else { reply.error(libc::ENOENT); }
    }
}

// ============ Progress parsing ============

fn parse_progress(line: &str) -> Option<ProgressMsg> {
    let frame = extract_u64(line, "frame=")?;
    let time = extract_time(line)?;
    let speed = extract_f32(line, "speed=").unwrap_or(0.0);
    let bitrate = extract_f32(line, "bitrate=").unwrap_or(0.0);
    let size = extract_u64(line, "size=").unwrap_or(0) * 1024;
    Some(ProgressMsg { frame, time_secs: time, speed, bitrate_kbps: bitrate, output_size: size })
}
fn extract_u64(s: &str, key: &str) -> Option<u64> {
    let i = s.find(key)? + key.len();
    s[i..].trim_start().split(|c: char| !c.is_ascii_digit()).next()?.parse().ok()
}
fn extract_f32(s: &str, key: &str) -> Option<f32> {
    let i = s.find(key)? + key.len();
    let v: String = s[i..].trim_start().chars().take_while(|c| c.is_ascii_digit() || *c == '.').collect();
    v.parse().ok()
}
fn extract_time(s: &str) -> Option<f64> {
    let i = s.find("time=")? + 5;
    let t: String = s[i..].trim_start().chars().take_while(|c| c.is_ascii_digit() || *c == ':' || *c == '.').collect();
    let p: Vec<&str> = t.split(':').collect();
    if p.len() == 3 { Some(p[0].parse::<f64>().ok()? * 3600.0 + p[1].parse::<f64>().ok()? * 60.0 + p[2].parse::<f64>().ok()?) }
    else { None }
}

/// Detect available HEVC encoders by probing the ffmpeg binary
pub fn detect_encoders(ffmpeg: &str) -> Vec<String> {
    let mut encoders = Vec::new();
    // List of encoders to test — hw encoders need actual GPU probe
    let candidates = [
        "hevc_nvenc", "hevc_videotoolbox", "hevc_amf", "hevc_qsv", "hevc_vaapi", "libx265",
        "h264_nvenc", "h264_videotoolbox", "h264_amf", "h264_qsv", "h264_vaapi", "libx264",
    ];
    // First check what's compiled in
    let compiled: Vec<String> = match std::process::Command::new(ffmpeg)
        .args(["-hide_banner", "-encoders"]).output() {
        Ok(output) => {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let found: Vec<String> = candidates.iter().filter(|c| stdout.contains(*c)).map(|c| c.to_string()).collect();
                tracing::info!("Compiled encoders found: {:?}", found);
                found
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::warn!("ffmpeg -encoders exit {:?}: {}", output.status.code(), stderr.chars().take(200).collect::<String>());
                vec![]
            }
        }
        Err(e) => {
            tracing::warn!("ffmpeg -encoders spawn failed: {} (path={})", e, ffmpeg);
            vec![]
        }
    };
    // Software encoders don't need hardware probing
    for enc in &compiled {
        if enc.starts_with("lib") {
            encoders.push(enc.clone());
            continue;
        }
        // Hardware encoder — try encoding 1 frame to verify GPU actually supports it
        if probe_encoder(ffmpeg, enc, true) {
            encoders.push(enc.clone());
        } else if enc.contains("nvenc") && probe_encoder(ffmpeg, enc, false) {
            // NVENC works without temporal-aq — mark as basic support
            encoders.push(format!("{}/no-temporal-aq", enc));
        }
    }
    if encoders.is_empty() { encoders.push("libx265".into()); encoders.push("libx264".into()); }
    encoders
}

/// Test GPU capabilities — what resolutions/HDR combinations each GPU can handle.
/// Returns a JSON-serializable map of capabilities per GPU.
pub fn detect_gpu_capabilities(ffmpeg: &str, gpu_count: usize) -> Vec<serde_json::Value> {
    use serde_json::json;

    // Detect which encoder to use for capability testing
    let encoders = detect_encoders(ffmpeg);
    let has_nvenc = encoders.iter().any(|e| e.contains("nvenc"));
    let has_vt = encoders.iter().any(|e| e.contains("videotoolbox"));
    let hw_encoder = if has_nvenc { "hevc_nvenc" } else if has_vt { "hevc_videotoolbox" } else { "libx265" };
    let is_nvenc = hw_encoder == "hevc_nvenc";
    let is_vt = hw_encoder.contains("videotoolbox");
    tracing::info!("Capability test encoder: {} (gpus={})", hw_encoder, gpu_count);

    let tests = [
        ("1080p_sdr",   "1920x1080", "yuv420p",    false),
        ("1080p_10bit", "1920x1080", "yuv420p10le", false),
        ("1080p_hdr",   "1920x1080", "yuv420p10le", true),
        ("4k_sdr",      "3840x2160", "yuv420p",     false),
        ("4k_10bit",    "3840x2160", "yuv420p10le", false),
        ("4k_hdr",      "3840x2160", "yuv420p10le", true),
    ];
    let tmp_dir = std::env::temp_dir();
    let mut results = Vec::new();
    for gpu in 0..gpu_count {
        let mut caps = serde_json::Map::new();
        caps.insert("gpu".into(), json!(gpu));
        let gpu_str = gpu.to_string();
        for &(name, size, pix_fmt, hdr) in &tests {
            let test_file = tmp_dir.join(format!("_captest_{}_{}.mkv", gpu, name));
            let test_path = test_file.to_string_lossy().to_string();

            // Step 1: Generate test HEVC file
            let input_arg = format!("color=black:s={}:d=2:r=25", size);
            let mut gen_args = vec![
                "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", input_arg.as_str(),
                "-frames:v", "50", "-pix_fmt", pix_fmt,
                "-c:v", hw_encoder,
            ];
            if is_nvenc {
                gen_args.extend_from_slice(&["-gpu", gpu_str.as_str()]);
            }
            if is_vt {
                gen_args.extend_from_slice(&["-allow_sw", "1"]);
            }
            if hdr {
                gen_args.extend_from_slice(&["-color_primaries", "bt2020", "-color_trc", "smpte2084", "-colorspace", "bt2020nc"]);
            }
            gen_args.push(test_path.as_str());

            let gen_ok = std::process::Command::new(ffmpeg).args(&gen_args).output()
                .map(|o| o.status.success()).unwrap_or(false);

            if !gen_ok {
                caps.insert(name.into(), json!(false));
                tracing::info!("GPU {} capability {}: FAIL (generate)", gpu, name);
                let _ = std::fs::remove_file(&test_file);
                continue;
            }

            // Step 2: Full decode + encode pipeline
            let mut enc_args = vec![
                "-hide_banner", "-loglevel", "error", "-y",
            ];
            // CUDA hwaccel only for NVENC
            if is_nvenc {
                enc_args.extend_from_slice(&[
                    "-hwaccel", "cuda", "-hwaccel_device", gpu_str.as_str(),
                    "-hwaccel_output_format", "cuda", "-extra_hw_frames", "16",
                ]);
            }
            enc_args.extend_from_slice(&["-i", test_path.as_str(), "-c:v", hw_encoder]);
            if is_nvenc {
                enc_args.extend_from_slice(&["-gpu", gpu_str.as_str(), "-rc", "vbr", "-cq", "28", "-preset", "p4",
                    "-multipass", "qres", "-spatial-aq", "1", "-aq-strength", "8"]);
            }
            if is_vt {
                enc_args.extend_from_slice(&["-allow_sw", "1", "-b:v", "5M"]);
            }
            if hdr {
                enc_args.extend_from_slice(&["-color_primaries", "bt2020", "-color_trc", "smpte2084", "-colorspace", "bt2020nc"]);
            }
            enc_args.extend_from_slice(&["-f", "null", "-"]);

            let ok = std::process::Command::new(ffmpeg).args(&enc_args).output()
                .map(|o| o.status.success()).unwrap_or(false);

            let _ = std::fs::remove_file(&test_file);
            caps.insert(name.into(), json!(ok));
            tracing::info!("GPU {} capability {}: {}", gpu, name, if ok { "OK" } else { "FAIL" });
        }
        // Test Vulkan/libplacebo (server-wide, not per-GPU)
        if gpu == 0 {
            let vulkan_ok = std::process::Command::new(ffmpeg)
                .args([
                    "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", "color=black:s=1920x1080:d=2:r=25",
                    "-frames:v", "50", "-init_hw_device", "vulkan",
                    "-vf", "hwupload,libplacebo=format=yuv420p10le,hwdownload,format=yuv420p10le",
                    "-c:v", "libx265", "-f", "null", "-",
                ])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            caps.insert("vulkan_libplacebo".into(), json!(vulkan_ok));
            tracing::info!("Vulkan/libplacebo: {}", if vulkan_ok { "OK" } else { "NOT AVAILABLE" });
        }
        results.push(json!(caps));
    }
    results
}

/// Test if a hardware encoder actually works with typical encoding flags.
fn probe_encoder(ffmpeg: &str, encoder: &str, full_flags: bool) -> bool {
    let mut args = vec![
        "-hide_banner", "-loglevel", "error",
        "-f", "lavfi", "-i", "color=black:s=1920x1080:d=0.04:r=25",
        "-frames:v", "1",
        "-c:v", encoder,
    ];
    if encoder.contains("nvenc") {
        args.extend_from_slice(&[
            "-rc", "vbr", "-cq", "28", "-preset", "p4",
            "-multipass", "qres", "-spatial-aq", "1",
            "-aq-strength", "8",
        ]);
        if full_flags {
            args.extend_from_slice(&["-temporal-aq", "1"]);
        }
    }
    if encoder.contains("videotoolbox") {
        args.extend_from_slice(&["-allow_sw", "1"]);
    }
    args.extend_from_slice(&["-f", "null", "-"]);
    match std::process::Command::new(ffmpeg).args(&args).output() {
        Ok(output) => {
            let ok = output.status.success();
            if !ok {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::warn!("Probe {} (full={}) failed: exit={:?} stderr={}", encoder, full_flags, output.status.code(), stderr.chars().take(200).collect::<String>());
            }
            ok
        },
        Err(e) => {
            tracing::warn!("Probe {} spawn failed: {}", encoder, e);
            false
        },
    }
}
