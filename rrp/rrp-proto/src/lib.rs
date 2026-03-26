//! Recode Remote Protocol (RRP) — shared types and helpers
//! TCP-based for maximum throughput on LAN.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const STREAM_FILE_TRANSFER: u8 = 1;
pub const STREAM_PROGRESS: u8 = 2;
pub const STREAM_OUTPUT: u8 = 3;

// Tagged message types for multiplexed FUSE mode
pub const TAG_FILE_READ_REQ: u8 = 0x10;
pub const TAG_FILE_READ_RESP: u8 = 0x11;
pub const TAG_PROGRESS: u8 = 0x12;
pub const TAG_CONTROL: u8 = 0x13;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum TransferMode {
    Upload, // Current: send entire file before encoding
    Mount,  // FUSE: mount file on-demand, read over network
}

impl Default for TransferMode {
    fn default() -> Self { TransferMode::Upload }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ControlMsg {
    Auth { timestamp: u64, hmac: [u8; 32] },
    SubmitJob {
        job_id: String,
        ffmpeg_args: Vec<String>,
        input_files: Vec<FileInfo>,
        output_path: String,
        #[serde(default)]
        transfer_mode: TransferMode,
    },
    CancelJob,
    GetInfo,
    AuthOk,
    AuthFail(String),
    ServerInfo { encoders: Vec<String>, os: String, arch: String, #[serde(default)] has_fuse: bool },
    JobAccepted,
    JobComplete { exit_code: i32 },
    JobError(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileInfo {
    pub original_path: String,
    pub virtual_name: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgressMsg {
    pub frame: u64,
    pub time_secs: f64,
    pub speed: f32,
    pub bitrate_kbps: f32,
    pub output_size: u64,
}

/// FUSE mode: server requests a chunk of an input file from the client
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileReadReq {
    pub file_idx: u32,
    pub offset: u64,
    pub length: u32,
}

/// FUSE mode: client responds with file data
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileReadResp {
    pub data: Vec<u8>,
    pub error: bool,
}

pub fn encode<T: Serialize>(msg: &T) -> Vec<u8> {
    let payload = bincode::serialize(msg).unwrap();
    let len = (payload.len() as u32).to_be_bytes();
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len);
    frame.extend_from_slice(&payload);
    frame
}

/// Read one framed message from any AsyncRead
pub async fn read_msg<T: for<'de> Deserialize<'de>, R: AsyncReadExt + Unpin>(r: &mut R) -> anyhow::Result<T> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok(bincode::deserialize(&payload)?)
}

/// Write one framed message to any AsyncWrite
pub async fn write_msg<T: Serialize, W: AsyncWriteExt + Unpin>(w: &mut W, msg: &T) -> anyhow::Result<()> {
    let frame = encode(msg);
    w.write_all(&frame).await?;
    Ok(())
}

/// Write a tagged message (for FUSE multiplexed mode)
pub async fn write_tagged<T: Serialize, W: AsyncWriteExt + Unpin>(w: &mut W, tag: u8, msg: &T) -> anyhow::Result<()> {
    let payload = bincode::serialize(msg)?;
    let len = (payload.len() as u32).to_be_bytes();
    let mut frame = Vec::with_capacity(1 + 4 + payload.len());
    frame.push(tag);
    frame.extend_from_slice(&len);
    frame.extend_from_slice(&payload);
    w.write_all(&frame).await?;
    Ok(())
}

/// Read one tagged message — returns (tag, raw payload)
pub async fn read_tagged<R: AsyncReadExt + Unpin>(r: &mut R) -> anyhow::Result<(u8, Vec<u8>)> {
    let mut tag_buf = [0u8; 1];
    r.read_exact(&mut tag_buf).await?;
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok((tag_buf[0], payload))
}

// Auth helpers
use hmac::{Hmac, Mac};
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;

pub fn generate_hmac(secret: &str, timestamp: u64) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(&timestamp.to_le_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(&mac.finalize().into_bytes());
    out
}

pub fn verify_hmac(secret: &str, timestamp: u64, hmac_bytes: &[u8; 32]) -> bool {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    if now.abs_diff(timestamp) > 30 { return false; }
    generate_hmac(secret, timestamp) == *hmac_bytes
}

pub const DEFAULT_PORT: u16 = 9878;
pub const DEFAULT_LISTEN_PORT: u16 = 9879;
pub const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MB chunks — larger = fewer round trips over WAN

// Reverse-connect: connection type prefixes
pub const CONN_TYPE_CONTROL: u8 = 0x01;
pub const CONN_TYPE_DATA: u8 = 0x02;
pub const HEARTBEAT_INTERVAL_SECS: u64 = 15;

/// Messages for the reverse-connect control channel.
/// GPU servers connect OUT to clients; this protocol manages that channel.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReverseControlMsg {
    /// GPU server → Client: initial auth + capabilities
    Auth {
        secret: String,
        server_name: String,
        encoders: Vec<String>,
        os: String,
        arch: String,
        max_jobs: usize,
        has_fuse: bool,
        #[serde(default)]
        gpu_capabilities: String,
    },
    /// Client → GPU server: auth response
    AuthOk,
    AuthFail(String),
    /// GPU server → Client: periodic heartbeat
    Heartbeat { active_jobs: u32 },
    /// Client → GPU server: assign a job
    JobAssignment {
        job_id: String,
        ffmpeg_args: Vec<String>,
        input_files: Vec<FileInfo>,
        output_path: String,
        connect_port: u16,
        /// Shell commands to run after ffmpeg completes (while FUSE mount is still active).
        /// Supports placeholders: {JOBDIR} = job directory, {OUTPUT} = encoded output file.
        /// Input file paths are rewritten to FUSE mount paths automatically.
        #[serde(default)]
        post_commands: Vec<String>,
    },
    /// GPU server → Client: accept/reject
    JobAccepted { job_id: String },
    JobReject { job_id: String, reason: String },
    /// GPU server → Client: job finished notification
    JobFinished { job_id: String, exit_code: i32, #[serde(default)] stderr: String },
}

/// Sent on a new data connection to identify which job it belongs to
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataConnect {
    pub job_id: String,
    pub secret: String,
}
