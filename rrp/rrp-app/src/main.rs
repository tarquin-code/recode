mod client;
mod server;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "recode-remote", about = "Recode Remote Protocol — GPU encoding over TCP")]
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Commands::Server { port, secret, ffmpeg, tmp_dir, max_jobs } => {
            tracing_subscriber::fmt::init();
            server::run(port, secret, ffmpeg, tmp_dir, max_jobs).await
        }
        Commands::Client { args } => {
            client::run_client(args).await
        }
        Commands::Ping => {
            client::run_ping().await
        }
    }
}
