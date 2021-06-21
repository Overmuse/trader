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
    let _guard = sentry::init((
        settings.sentry.address.clone(),
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ));

    run(settings).await
}
