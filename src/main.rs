use alpaca::Client;
use dotenv::dotenv;
use futures::StreamExt;
use rdkafka::{
    config::ClientConfig, consumer::stream_consumer::StreamConsumer, consumer::Consumer,
};
use std::env;
use tracing::{error, info, warn};
use trader::errors::Result;
use trader::handle_message;
use trader::telemetry::{get_subscriber, init_subscriber};

async fn run() -> Result<()> {
    info!("Initiating trader");
    let api = Client::from_env()?;

    let c: StreamConsumer = ClientConfig::new()
        .set("group.id", &env::var("GROUP_ID")?)
        .set("bootstrap.servers", &env::var("BOOTSTRAP_SERVERS")?)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", &env::var("SASL_USERNAME")?)
        .set("sasl.password", &env::var("SASL_PASSWORD")?)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    c.subscribe(&[&env::var("ORDER_INTENT_TOPIC")?])
        .expect("Cannot subscribe to specified topic");

    c.start()
        .for_each_concurrent(10, |msg| async {
            match msg {
                Ok(msg) => {
                    let order = handle_message(&api, msg.detach()).await;
                    match order {
                        Ok(o) => info!("Submitted order: {:#?}", o),
                        Err(e) => warn!("Failed to submit order: {:?}", e),
                    }
                }
                Err(e) => warn!("Error received: {:?}", e),
            }
        })
        .await;
    Ok(())
}

fn main() {
    dotenv().ok();
    let subscriber = get_subscriber("trader".into(), "info".into());
    init_subscriber(subscriber);

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    match rt.block_on(run()) {
        Ok(_) => info!("Done!"),
        Err(e) => error!("An error occured: {:?}", e),
    };
}
