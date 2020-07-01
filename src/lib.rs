use alpaca::orders::{submit_order, Order, OrderIntent};
use alpaca::AlpacaConfig;
use anyhow::{anyhow, Result};
use rdkafka::message::OwnedMessage;
use rdkafka::Message;

pub async fn handle_message(api: &AlpacaConfig, msg: OwnedMessage) -> Result<Order> {
    if let Some(Ok(payload)) = msg.payload_view::<str>() {
        let order_intent: OrderIntent = serde_json::from_str(payload)?;
        execute_order(api, order_intent).await
    } else {
        Err(anyhow!("Could not process message"))
    }
}

async fn execute_order(api: &AlpacaConfig, oi: OrderIntent) -> Result<Order> {
    println!("{:?}", &oi);
    submit_order(api, &oi).await
}
