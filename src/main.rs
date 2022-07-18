mod cmd;
mod process;
mod queries;
mod types;
use crate::cmd::Config;
use crate::process::*;
use crate::queries::db_setup;

use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let config = Config::parse();

    // Setup SQLite.
    let pool = db_setup().await?;

    // Do everything else.
    process(
        tokio::fs::File::open(config.input_file).await?,
        tokio::io::stdout(),
        &pool,
    )
    .await?;

    Ok(())
}
