//! Binary entrypoint for running a single `holo_fusion` node.
//!
//! The binary keeps startup logic intentionally thin and delegates all runtime
//! orchestration to `holo_fusion::run`.

use anyhow::Result;
use holo_fusion::{run, HoloFusionConfig};

/// Builds runtime configuration from environment variables and starts the server.
///
/// The async runtime is provided by Tokio because the SQL server, health server,
/// Ballista integration, and embedded HoloStore node are all asynchronous.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HoloFusionConfig::from_env()?;
    run(config).await
}
