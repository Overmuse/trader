use alpaca::orders::{submit_order, Order, OrderIntent};
use alpaca::AlpacaConfig;
use anyhow::{anyhow, Result};
use log::info;
use rdkafka::message::OwnedMessage;
use rdkafka::Message;

pub async fn handle_message(api: &AlpacaConfig, msg: OwnedMessage) -> Result<Order> {
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => {
            let order_intent: OrderIntent = serde_json::from_str(payload)?;
            execute_order(api, order_intent).await
        }
        Some(Err(e)) => Err(anyhow!("Received error message: {:?}", e)),
        None => Err(anyhow!("Received empty message")),
    }
}

async fn execute_order(api: &AlpacaConfig, oi: OrderIntent) -> Result<Order> {
    info!("{:#?}", &oi);
    submit_order(api, &oi).await
}
