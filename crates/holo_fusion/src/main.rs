//! Binary entrypoint for running a single `holo_fusion` node.
//!
//! The binary keeps startup logic intentionally thin and delegates all runtime
//! orchestration to `holo_fusion::run`.

use anyhow::Result;
use holo_fusion::{run, HoloFusionConfig};
use tracing_subscriber::EnvFilter;

/// Builds runtime configuration from environment variables and starts the server.
///
/// The async runtime is provided by Tokio because the SQL server, health server,
/// Ballista integration, and embedded HoloStore node are all asynchronous.
#[tokio::main]
async fn main() -> Result<()> {
    // Install structured logging once at process startup. We prefer env-driven
    // filtering so operators can tune verbosity without rebuilding.
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(
            // Keep a practical default if `RUST_LOG` is absent or invalid.
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("holo_fusion=info,holo_store=info,warn")),
        )
        .init();
    let config = HoloFusionConfig::from_env()?;
    run(config).await
}
