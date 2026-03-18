use anyhow::{Context, Result, bail};
use rrp_proto::*;
use sha2::{Sha256, Digest};
use std::env;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    let server_addr = env::var("RRP_SERVER_ADDRESS").context("RRP_SERVER_ADDRESS required")?;
    let server_secret = env::var("RRP_SERVER_SECRET").context("RRP_SERVER_SECRET required")?;
    let args: Vec<String> = env::args().skip(1).collect();

    // --ping mode: just test auth and exit
    if args.first().map(|s| s.as_str()) == Some("--ping") {
        let mut stream = TcpStream::connect(&server_addr).await.context("connect failed")?;
        stream.set_nodelay(true)?;
        let (mut rx, mut tx) = stream.split();
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
        write_msg(&mut tx, &ControlMsg::Auth { timestamp: ts, hmac: generate_hmac(&server_secret, ts) }).await?;
        match read_msg::<ControlMsg, _>(&mut rx).await? {
            ControlMsg::AuthOk => { eprintln!("OK"); std::process::exit(0); }
            ControlMsg::AuthFail(e) => { eprintln!("Auth failed: {}", e); std::process::exit(1); }
            _ => { eprintln!("Unexpected response"); std::process::exit(1); }
        }
    }

    if args.is_empty() { bail!("No ffmpeg arguments"); }

    // Check for --mount flag (FUSE mode)
    let use_mount = args.iter().any(|a| a == "--mount");
    let ffmpeg_args: Vec<String> = args.into_iter().filter(|a| a != "--mount" && a != "--upload").collect();

    let (input_paths, output_path, ffmpeg_args) = parse_args(&ffmpeg_args)?;
    let mut input_files = Vec::new();
    for (i, p) in input_paths.iter().enumerate() {
        let meta = tokio::fs::metadata(p).await?;
        input_files.push(FileInfo {
            original_path: p.to_string_lossy().to_string(),
            virtual_name: format!("input_{}.{}", i, p.extension().unwrap_or_default().to_string_lossy()),
            size: meta.len(),
        });
    }

    let transfer_mode = if use_mount { TransferMode::Mount } else { TransferMode::Upload };

    // Connect via TCP
    let mut stream = TcpStream::connect(&server_addr).await.context("TCP connect failed")?;
    stream.set_nodelay(true)?;
    let (mut rx, mut tx) = stream.split();

    // Auth
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
    write_msg(&mut tx, &ControlMsg::Auth { timestamp: ts, hmac: generate_hmac(&server_secret, ts) }).await?;
    match read_msg::<ControlMsg, _>(&mut rx).await? {
        ControlMsg::AuthOk => {}
        ControlMsg::AuthFail(e) => { eprintln!("Auth failed: {}", e); std::process::exit(1); }
        _ => bail!("Bad auth"),
    }

    // Submit job
    write_msg(&mut tx, &ControlMsg::SubmitJob {
        job_id: format!("{:x}", std::process::id()),
        ffmpeg_args, input_files: input_files.clone(),
        output_path: output_path.to_string_lossy().to_string(),
        transfer_mode: transfer_mode.clone(),
    }).await?;
    match read_msg::<ControlMsg, _>(&mut rx).await? {
        ControlMsg::JobAccepted => {}
        ControlMsg::JobError(e) => { eprintln!("Rejected: {}", e); std::process::exit(1); }
        _ => bail!("Bad response"),
    }

    if transfer_mode == TransferMode::Mount {
        // FUSE mode: serve file reads on demand
        fuse_mode_loop(&mut rx, &mut tx, &input_paths, &output_path).await
    } else {
        // Upload mode: send files first, then wait for progress/output
        upload_mode_loop(&mut rx, &mut tx, &input_paths, &input_files, &output_path).await
    }
}

async fn fuse_mode_loop<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin>(
    rx: &mut R, tx: &mut W,
    input_paths: &[PathBuf], output_path: &PathBuf,
) -> Result<()> {
    eprintln!("FUSE mount mode — serving files on demand");
    let mut exit_code = 1;

    // Open all input files for random access
    let mut files = Vec::new();
    for p in input_paths {
        files.push(std::fs::File::open(p)?);
    }

    loop {
        let (tag, payload) = match read_tagged(rx).await {
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
                write_tagged(tx, TAG_FILE_READ_RESP, &resp).await?;
                tx.flush().await?;
            }
            TAG_PROGRESS => {
                if let Ok(p) = bincode::deserialize::<ProgressMsg>(&payload) {
                    let h = (p.time_secs / 3600.0) as u32;
                    let m = ((p.time_secs % 3600.0) / 60.0) as u32;
                    let s = p.time_secs % 60.0;
                    eprint!("\rframe={:>6} fps=0.0 q=0.0 size={:>8}kB time={:02}:{:02}:{:05.2} bitrate={:.1}kbits/s speed={:.2}x    ",
                        p.frame, p.output_size/1024, h, m, s, p.bitrate_kbps, p.speed);
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
                            eprintln!("\nRemote error: {}", e);
                            exit_code = 1;
                            break;
                        }
                        _ => {}
                    }
                }
            }
            _ if tag == STREAM_OUTPUT => {
                // Output file transfer — size is in the tagged payload
                let total: u64 = bincode::deserialize(&payload)?;
                eprintln!("\nReceiving output ({:.1} MB)...", total as f64 / 1_048_576.0);
                let mut file = tokio::fs::File::create(output_path).await?;
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
                if computed != hash { eprintln!("WARNING: output SHA256 mismatch!"); }
                file.flush().await?;
                eprintln!("Output saved: {:.1} MB (verified)", total as f64 / 1_048_576.0);
            }
            _ => {}
        }
    }

    eprintln!();
    std::process::exit(exit_code);
}

async fn upload_mode_loop<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin>(
    rx: &mut R, tx: &mut W,
    input_paths: &[PathBuf], input_files: &[FileInfo], output_path: &PathBuf,
) -> Result<()> {
    // Send input files — bulk TCP push
    for (i, fi) in input_files.iter().enumerate() {
        let total = fi.size;
        eprint!("\rTransferring {}: 0%", fi.virtual_name);
        let mut file = tokio::fs::File::open(&input_paths[i]).await?;
        let mut buf = vec![0u8; 256 * 1024];
        let mut sent = 0u64;
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 { break; }
            tx.write_all(&buf[..n]).await?;
            sent += n as u64;
            if sent % (20 * 1024 * 1024) < 256 * 1024 {
                eprint!("\rTransferring {}: {:.0}%    ", fi.virtual_name, sent as f64 / total as f64 * 100.0);
            }
        }
        eprintln!("\rTransferred {}: {:.1} MB    ", fi.virtual_name, sent as f64 / 1_048_576.0);
    }
    tx.flush().await?;

    // Now read progress and output from server
    let mut exit_code = 1;

    loop {
        let mut peek = [0u8; 1];
        match rx.read_exact(&mut peek).await {
            Err(_) => break,
            Ok(_) => {}
        }

        if peek[0] == STREAM_OUTPUT {
            let total: u64 = read_msg(rx).await?;
            eprintln!("\nReceiving output ({:.1} MB)...", total as f64 / 1_048_576.0);
            let mut file = tokio::fs::File::create(output_path).await?;
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
            if computed != hash { eprintln!("WARNING: output SHA256 mismatch!"); }
            file.flush().await?;
            eprintln!("Output saved: {:.1} MB (verified)", total as f64 / 1_048_576.0);
        } else {
            let mut len_rest = [0u8; 3];
            rx.read_exact(&mut len_rest).await?;
            let len = u32::from_be_bytes([peek[0], len_rest[0], len_rest[1], len_rest[2]]) as usize;
            let mut payload = vec![0u8; len];
            rx.read_exact(&mut payload).await?;

            if let Ok(p) = bincode::deserialize::<ProgressMsg>(&payload) {
                let h = (p.time_secs / 3600.0) as u32;
                let m = ((p.time_secs % 3600.0) / 60.0) as u32;
                let s = p.time_secs % 60.0;
                eprint!("\rframe={:>6} fps=0.0 q=0.0 size={:>8}kB time={:02}:{:02}:{:05.2} bitrate={:.1}kbits/s speed={:.2}x    ",
                    p.frame, p.output_size/1024, h, m, s, p.bitrate_kbps, p.speed);
            } else if let Ok(ctrl) = bincode::deserialize::<ControlMsg>(&payload) {
                match ctrl {
                    ControlMsg::JobComplete { exit_code: ec } => {
                        exit_code = ec;
                        break;
                    }
                    ControlMsg::JobError(e) => {
                        eprintln!("\nRemote error: {}", e);
                        exit_code = 1;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    eprintln!();
    std::process::exit(exit_code);
}

fn parse_args(args: &[String]) -> Result<(Vec<PathBuf>, PathBuf, Vec<String>)> {
    let mut inputs = Vec::new();
    let output = PathBuf::from(args.last().context("No output")?);
    let mut i = 0;
    while i < args.len() {
        if args[i] == "-i" && i + 1 < args.len() { inputs.push(PathBuf::from(&args[i+1])); }
        i += 1;
    }
    Ok((inputs, output, args.to_vec()))
}
