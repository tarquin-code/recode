use anyhow::{Context, Result, bail};
use clap::Parser;
use fuser::{Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};
use rrp_proto::*;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
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

const TTL: Duration = Duration::from_secs(3600);

/// Removes the job directory on drop — guarantees cleanup on cancel, disconnect, or error.
struct CleanupGuard(PathBuf);
impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if self.0.exists() {
            // Unmount any FUSE mounts first
            let mnt = self.0.join("mnt");
            if mnt.exists() {
                let _ = std::process::Command::new("fusermount3").args(["-u", &mnt.to_string_lossy()]).output();
                let _ = std::process::Command::new("fusermount").args(["-u", &mnt.to_string_lossy()]).output();
            }
            if let Err(e) = std::fs::remove_dir_all(&self.0) {
                eprintln!("Cleanup failed for {:?}: {}", self.0, e);
            }
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "recode-remote-server")]
struct Args {
    #[arg(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,
    #[arg(short, long, env = "RRP_SECRET")]
    secret: String,
    #[arg(long, default_value = "/opt/Recode/bin/ffmpeg")]
    ffmpeg: String,
    #[arg(long, default_value = "/tmp/rrp")]
    tmp_dir: String,
    #[arg(long, default_value_t = 4)]
    max_jobs: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    info!("RRP server (TCP) on port {} (max {} jobs)", args.port, args.max_jobs);
    std::fs::create_dir_all(&args.tmp_dir)?;

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    let args = Arc::new(args);
    let sem = Arc::new(tokio::sync::Semaphore::new(args.max_jobs));

    loop {
        let (stream, peer) = listener.accept().await?;
        let args = args.clone();
        let sem = sem.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, peer, &args, sem).await {
                error!("{}: {}", peer, e);
            }
        });
    }
}

async fn handle(mut stream: TcpStream, peer: SocketAddr, args: &Args, sem: Arc<tokio::sync::Semaphore>) -> Result<()> {
    info!("Connection from {}", peer);
    stream.set_nodelay(true)?;
    let (mut rx, mut tx) = stream.split();

    // Auth
    let auth: ControlMsg = read_msg(&mut rx).await.context("read auth")?;
    match auth {
        ControlMsg::Auth { timestamp, hmac } => {
            if !verify_hmac(&args.secret, timestamp, &hmac) {
                write_msg(&mut tx, &ControlMsg::AuthFail("bad auth".into())).await?;
                bail!("Auth failed from {}", peer);
            }
            write_msg(&mut tx, &ControlMsg::AuthOk).await?;
        }
        _ => bail!("Expected Auth"),
    }

    // Job (may be a ping — client disconnects after auth)
    let job: ControlMsg = match read_msg(&mut rx).await {
        Ok(msg) => msg,
        Err(_) => {
            info!("{}: ping/disconnect after auth", peer);
            return Ok(());
        }
    };
    let (job_id, ffmpeg_args, input_files, _out_path, transfer_mode) = match job {
        ControlMsg::SubmitJob { job_id, ffmpeg_args, input_files, output_path, transfer_mode } =>
            (job_id, ffmpeg_args, input_files, output_path, transfer_mode),
        _ => bail!("Expected SubmitJob"),
    };

    let _permit = sem.acquire().await?;
    let job_dir = PathBuf::from(&args.tmp_dir).join(&job_id);
    std::fs::create_dir_all(&job_dir)?;
    // Cleanup guard — removes job_dir when dropped, no matter how the function exits
    let _cleanup = CleanupGuard(job_dir.clone());
    write_msg(&mut tx, &ControlMsg::JobAccepted).await?;
    info!("Job {}: {} inputs, mode={:?}", job_id, input_files.len(), transfer_mode);

    // Write marker file so the Recode UI can identify RRP jobs cross-user
    let marker = job_dir.join(".rrp_info");
    let orig_input = input_files.first().map(|f| f.original_path.as_str()).unwrap_or("");
    let marker_json = format!(r#"{{"client":"{}","input":"{}","job_id":"{}"}}"#,
        peer.ip(), orig_input.replace('\\', "\\\\").replace('"', "\\\""), job_id);
    let _ = std::fs::write(&marker, &marker_json);

    // Mount point for FUSE mode
    let mount_dir = job_dir.join("mnt");
    let _fuse_session: Option<()> = None;

    if transfer_mode == TransferMode::Mount {
        // --- FUSE mode: mount files on-demand ---
        std::fs::create_dir_all(&mount_dir)?;

        // Channel for FUSE thread → tokio: read requests
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<FuseReadRequest>(32);

        let fs = RrpFuse::new(input_files.clone(), req_tx);
        let mount_path = mount_dir.clone();
        let mount_ok = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mount_ok2 = mount_ok.clone();
        let fuse_handle = std::thread::spawn(move || {
            let options = vec![
                MountOption::RO,
                MountOption::FSName("rrp".into()),
                MountOption::AllowOther,
            ];
            mount_ok2.store(true, std::sync::atomic::Ordering::SeqCst);
            if let Err(e) = fuser::mount2(fs, &mount_path, &options) {
                mount_ok2.store(false, std::sync::atomic::Ordering::SeqCst);
                error!("FUSE mount error: {}", e);
            }
        });

        // Wait for FUSE mount to be ready
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if mount_dir.join(".").exists() && std::fs::read_dir(&mount_dir).map(|mut d| d.next().is_some()).unwrap_or(false) {
                break;
            }
        }
        if !mount_ok.load(std::sync::atomic::Ordering::SeqCst) {
            let _ = write_tagged(&mut tx, TAG_CONTROL, &ControlMsg::JobError("FUSE mount failed".into())).await;
            bail!("FUSE mount failed");
        }

        // Rewrite ffmpeg args to point at FUSE mount
        let output_local = job_dir.join("output.mkv");
        let rw = rewrite_args(&ffmpeg_args, &input_files, &mount_dir, &output_local);
        info!("ffmpeg {}", rw.join(" "));

        // Run ffmpeg
        let mut child = Command::new(&args.ffmpeg)
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

        // Multiplexed loop: handle FUSE read requests, progress, and cancel
        loop {
            tokio::select! {
                // FUSE read request from filesystem thread
                Some(freq) = req_rx.recv() => {
                    // Send read request to client
                    let req = FileReadReq {
                        file_idx: freq.file_idx,
                        offset: freq.offset,
                        length: freq.length,
                    };
                    if write_tagged(&mut tx, TAG_FILE_READ_REQ, &req).await.is_err() {
                        warn!("Failed to send read request to client");
                        let _ = freq.resp_tx.send(Err(()));
                        let _ = child.kill().await;
                        break;
                    }
                    tx.flush().await?;

                    // Read response from client
                    match read_tagged(&mut rx).await {
                        Ok((TAG_FILE_READ_RESP, payload)) => {
                            match bincode::deserialize::<FileReadResp>(&payload) {
                                Ok(resp) if !resp.error => {
                                    let _ = freq.resp_tx.send(Ok(resp.data));
                                }
                                _ => {
                                    let _ = freq.resp_tx.send(Err(()));
                                }
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
                        _ => {
                            let _ = freq.resp_tx.send(Err(()));
                        }
                    }
                }
                // ffmpeg stderr (progress)
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
                                        let _ = write_tagged(&mut tx, TAG_PROGRESS, &p).await;
                                    }
                                }
                            }
                            line_buf = incomplete.to_string();
                        }
                    }
                }
            }
        }

        let status = child.wait().await?;
        let exit_code = status.code().unwrap_or(1);

        // Unmount FUSE
        let _ = std::process::Command::new("fusermount3")
            .args(["-u", &mount_dir.to_string_lossy()])
            .output();
        let _ = std::process::Command::new("fusermount")
            .args(["-u", &mount_dir.to_string_lossy()])
            .output();
        let _ = fuse_handle.join();

        // Send output back using tagged protocol for FUSE mode
        if exit_code == 0 && output_local.exists() {
            let meta = tokio::fs::metadata(&output_local).await?;
            let total = meta.len();
            info!("Sending output: {:.1} MB", total as f64 / 1_048_576.0);

            // Use tagged STREAM_OUTPUT with size
            write_tagged(&mut tx, STREAM_OUTPUT, &total).await?;

            let mut file = tokio::fs::File::open(&output_local).await?;
            let mut hasher = Sha256::new();
            let mut buf = vec![0u8; 256 * 1024];
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
            let mut buf = vec![0u8; 256 * 1024];
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

        let mut child = Command::new(&args.ffmpeg)
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
            let mut buf = vec![0u8; 256 * 1024];
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

struct FuseReadRequest {
    file_idx: u32,
    offset: u64,
    length: u32,
    resp_tx: std::sync::mpsc::Sender<Result<Vec<u8>, ()>>,
}

struct RrpFuse {
    files: Vec<FileInfo>,
    // inode -> file index mapping (inode 1 = root dir, 2+ = files)
    inode_map: HashMap<u64, usize>,
    name_map: HashMap<String, u64>,
    req_tx: tokio::sync::mpsc::Sender<FuseReadRequest>,
}

impl RrpFuse {
    fn new(files: Vec<FileInfo>, req_tx: tokio::sync::mpsc::Sender<FuseReadRequest>) -> Self {
        let mut inode_map = HashMap::new();
        let mut name_map = HashMap::new();
        for (i, f) in files.iter().enumerate() {
            let ino = (i + 2) as u64; // inodes start at 2 (1 = root)
            inode_map.insert(ino, i);
            name_map.insert(f.virtual_name.clone(), ino);
        }
        RrpFuse { files, inode_map, name_map, req_tx }
    }

    fn file_attr(&self, ino: u64) -> Option<fuser::FileAttr> {
        if ino == 1 {
            return Some(dir_attr(1));
        }
        let idx = self.inode_map.get(&ino)?;
        let fi = &self.files[*idx];
        Some(fuser::FileAttr {
            ino,
            size: fi.size,
            blocks: (fi.size + 511) / 512,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            kind: fuser::FileType::RegularFile,
            perm: 0o444,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        })
    }
}

fn dir_attr(ino: u64) -> fuser::FileAttr {
    fuser::FileAttr {
        ino,
        size: 0,
        blocks: 0,
        atime: SystemTime::UNIX_EPOCH,
        mtime: SystemTime::UNIX_EPOCH,
        ctime: SystemTime::UNIX_EPOCH,
        crtime: SystemTime::UNIX_EPOCH,
        kind: fuser::FileType::Directory,
        perm: 0o555,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}

impl Filesystem for RrpFuse {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent != 1 {
            reply.error(libc::ENOENT);
            return;
        }
        let name_str = name.to_string_lossy();
        if let Some(&ino) = self.name_map.get(name_str.as_ref()) {
            if let Some(attr) = self.file_attr(ino) {
                reply.entry(&TTL, &attr, 0);
                return;
            }
        }
        reply.error(libc::ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if let Some(attr) = self.file_attr(ino) {
            reply.attr(&TTL, &attr);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        let idx = match self.inode_map.get(&ino) {
            Some(&i) => i,
            None => { reply.error(libc::ENOENT); return; }
        };

        let fi = &self.files[idx];
        let offset = offset as u64;
        if offset >= fi.size {
            reply.data(&[]);
            return;
        }
        let remaining = fi.size - offset;
        let read_len = std::cmp::min(size as u64, remaining) as u32;

        // Send request to tokio runtime via channel
        let (resp_tx, resp_rx) = std::sync::mpsc::channel();
        let freq = FuseReadRequest {
            file_idx: idx as u32,
            offset,
            length: read_len,
            resp_tx,
        };

        if self.req_tx.blocking_send(freq).is_err() {
            reply.error(libc::EIO);
            return;
        }

        // Block waiting for response (this is on the FUSE thread, blocking is expected)
        match resp_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(data)) => reply.data(&data),
            _ => reply.error(libc::EIO),
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if ino != 1 {
            reply.error(libc::ENOENT);
            return;
        }

        let mut entries: Vec<(u64, fuser::FileType, String)> = vec![
            (1, fuser::FileType::Directory, ".".into()),
            (1, fuser::FileType::Directory, "..".into()),
        ];
        for (i, f) in self.files.iter().enumerate() {
            entries.push(((i + 2) as u64, fuser::FileType::RegularFile, f.virtual_name.clone()));
        }

        for (idx, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.0, (idx + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        if self.inode_map.contains_key(&ino) {
            reply.opened(0, 0);
        } else {
            reply.error(libc::ENOENT);
        }
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
