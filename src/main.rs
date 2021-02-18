use alpaca::Client;
use anyhow::Result;
use clap::{App, Arg};
use dotenv::dotenv;
use futures::StreamExt;
use rdkafka::{
    config::ClientConfig, consumer::stream_consumer::StreamConsumer, consumer::Consumer,
};
use std::env;
use tracing::{error, info, warn};
use trader::handle_message;
use trader::telemetry::{get_subscriber, init_subscriber};

async fn run() -> Result<()> {
    info!("Initiating trader");
    let matches = App::new("Trader")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Kafka stream trader")
        .arg(
            Arg::with_name("group_id")
                .short("g")
                .long("group-id")
                .takes_value(true)
                .default_value("trader"),
        )
        .arg(
            Arg::with_name("url")
                .short("u")
                .long("url")
                .takes_value(true)
                .default_value("https://paper-api.alpaca.markets/v2"),
        )
        .get_matches();

    let group_id = matches.value_of("group_id").unwrap();
    let url = matches.value_of("url").unwrap().to_string();
    let api = Client::new(
        url,
        env::var("ALPACA_KEY_ID")?,
        env::var("ALPACA_SECRET_KEY")?,
    )?;

    let c: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", &std::env::var("KAFKA_BROKERS")?)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", &std::env::var("CLUSTER_API_KEY")?)
        .set("sasl.password", &std::env::var("CLUSTER_API_SECRET")?)
        .set("enable.partition.eof", "false")
        .set("enable.ssl.certificate.verification", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    c.subscribe(&["intended-trades"])
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
