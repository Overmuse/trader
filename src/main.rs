use alpaca::AlpacaConfig;
use clap::{App, Arg};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use std::env;

use futures::StreamExt;

use trader::handle_message;

#[tokio::main]
async fn main() {
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
        .get_matches();

    let brokers = matches.value_of("brokers").unwrap();
    let api = AlpacaConfig::new(
        "https://paper-api.alpaca.markets".to_string(),
        env::var("ALPACA_KEY_ID").unwrap(),
        env::var("ALPACA_SECRET_KEY").unwrap(),
    )
    .unwrap();

    let c: StreamConsumer = ClientConfig::new()
        .set("group.id", "1")
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    c.subscribe(&["trade-intents"])
        .expect("Cannot subscribe to specified topic");
    let mut message_stream = c.start();

    while let Some(msg) = message_stream.next().await {
        handle_message(&api, msg.unwrap().detach())
            .await
            .map_err(|e| println!("{:?}", e));
    }
}
