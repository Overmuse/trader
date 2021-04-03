use anyhow::Result;
use dotenv::dotenv;
use tracing::info;
use trader::{
    run,
    telemetry::{get_subscriber, init_subscriber},
    Settings,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let subscriber = get_subscriber();
    init_subscriber(subscriber);
    info!("Starting trader");
    let settings = Settings::new()?;

    run(settings).await
}
