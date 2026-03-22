mod client;
mod connect;
mod listener;
mod server;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "recode-remote", version, about = "Recode Remote Protocol — GPU encoding over TCP")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run as GPU server — accept and process remote encode jobs
    Server {
        #[arg(short, long, default_value_t = rrp_proto::DEFAULT_PORT)]
        port: u16,
        #[arg(short, long, env = "RRP_SECRET")]
        secret: String,
        #[arg(long, default_value = "/opt/Recode/bin/ffmpeg")]
        ffmpeg: String,
        #[arg(long, default_value = "/tmp/rrp")]
        tmp_dir: String,
        #[arg(long, default_value_t = 4)]
        max_jobs: usize,
    },
    /// Run as client — send encode jobs to a remote GPU server
    Client {
        /// ffmpeg arguments (passed through to remote server)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Test authentication with a remote server
    Ping,
    /// Listen for incoming GPU server connections (reverse-connect mode)
    Listen {
        #[arg(short, long, default_value_t = rrp_proto::DEFAULT_LISTEN_PORT)]
        port: u16,
        #[arg(short, long, env = "RRP_CLIENT_SECRET")]
        secret: String,
        #[arg(long, default_value = "/tmp/recode/rrp/listener-status.json")]
        status_file: String,
    },
    /// Connect to a remote client as a GPU worker (reverse-connect mode)
    Connect {
        /// Client address (host:port)
        #[arg(short, long)]
        address: String,
        #[arg(short, long, env = "RRP_CLIENT_SECRET")]
        secret: String,
        /// Name to advertise to clients
        #[arg(long, default_value = "GPU Worker")]
        name: String,
        #[arg(long, default_value = "/opt/Recode/bin/ffmpeg")]
        ffmpeg: String,
        #[arg(long, default_value = "/tmp/rrp")]
        tmp_dir: String,
        #[arg(long, default_value_t = 4)]
        max_jobs: usize,
        #[arg(long, default_value = "/tmp/recode/rrp/connect-status.json")]
        status_file: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Note: do NOT use signal(SIGCHLD, SIG_IGN) — it breaks std::process::Command::output()
    // Zombie cleanup is handled by the GC task in connect.rs instead

    fn init_tracing() {
        use tracing_subscriber::fmt::time::ChronoLocal;
        tracing_subscriber::fmt()
            .with_timer(ChronoLocal::new("%d-%m-%Y %H:%M:%S".to_string()))
            .with_ansi(false)
            .init();
    }

    match Cli::parse().command {
        Commands::Server { port, secret, ffmpeg, tmp_dir, max_jobs } => {
            init_tracing();
            server::run(port, secret, ffmpeg, tmp_dir, max_jobs).await
        }
        Commands::Client { args } => {
            client::run_client(args).await
        }
        Commands::Ping => {
            client::run_ping().await
        }
        Commands::Listen { port, secret, status_file } => {
            init_tracing();
            listener::run(port, secret, status_file).await
        }
        Commands::Connect { address, secret, name, ffmpeg, tmp_dir, max_jobs, status_file } => {
            init_tracing();
            connect::run(address, secret, name, ffmpeg, tmp_dir, max_jobs, status_file).await
        }
    }
}
