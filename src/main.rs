use alpaca::Client;
use anyhow::Result;
use clap::{App, Arg};
use dotenv::dotenv;
use futures::StreamExt;
use log::{error, info, warn};
use rdkafka::{
    config::ClientConfig, consumer::stream_consumer::StreamConsumer, consumer::Consumer,
};
use std::env;
use trader::handle_message;

async fn run() -> Result<()> {
    let matches = App::new("Trader")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Kafka stream trader")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
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

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group_id").unwrap();
    let url = matches.value_of("url").unwrap().to_string();
    let api = Client::new(
        url,
        env::var("ALPACA_KEY_ID")?,
        env::var("ALPACA_SECRET_KEY")?,
    )?;

    let c: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
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
    env_logger::builder().format_timestamp_micros().init();
    let mut rt = tokio::runtime::Runtime::new().unwrap();

    match rt.block_on(run()) {
        Ok(_) => info!("Done!"),
        Err(e) => error!("An error occured: {:?}", e),
    };
}
