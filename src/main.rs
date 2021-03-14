use dotenv::dotenv;
use tracing::{error, info};
use trader::{
    run,
    telemetry::{get_subscriber, init_subscriber},
};

#[tokio::main]
async fn main() {
    dotenv().ok();
    let subscriber = get_subscriber("trader".into(), "info".into());
    init_subscriber(subscriber);

    match run().await {
        Ok(_) => info!("Done!"),
        Err(e) => error!("An error occured: {:?}", e),
    }
}
