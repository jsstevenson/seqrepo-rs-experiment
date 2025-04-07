mod api;
mod import;
mod dynamodb;

use clap::{Args, Parser, Subcommand};
use std::ops::RangeInclusive;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Serve(ServeArgs),
    Import(ImportArgs),
}

#[derive(Args, Debug)]
struct ServeArgs {
    #[arg(long)]
    db_url: Option<String>,

    #[arg(long, value_parser = port_in_range)]
    port: Option<u16>,
}

const PORT_RANGE: RangeInclusive<usize> = 1..=65535;

fn port_in_range(s: &str) -> Result<u16, String> {
    let port: usize = s
        .parse()
        .map_err(|_| format!("`{s}` isn't a port number"))?;
    if PORT_RANGE.contains(&port) {
        Ok(port as u16)
    } else {
        Err(format!(
            "port not in range {}-{}",
            PORT_RANGE.start(),
            PORT_RANGE.end()
        ))
    }
}

#[derive(Args, Debug)]
struct ImportArgs {
    path: PathBuf,
}

#[tokio::main()]
async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Serve(args) => {
            api::serve(args.port).await;
        }
        Commands::Import(args) => {
            let _ = import::import(&args.path).await;
        }
    }
}
