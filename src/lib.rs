use alpaca::{AlpacaConfig, Side};
use alpaca::orders::{AlpacaOrder, OrderType, TimeInForce, submit_order};
use anyhow::{anyhow, Result};
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderIntent {
    pub id: Uuid,
    pub symbol: String,
    pub qty: u32,
    #[serde(flatten, rename(serialize = "type"))]
    pub order_type: OrderType,
    pub side: Side,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub extended_hours: bool,
}

pub async fn handle_message(api: &AlpacaConfig, msg: OwnedMessage) -> Result<AlpacaOrder> {
    if let Some(Ok(payload)) = msg.payload_view::<str>() {
        let order_intent = serde_json::from_str(payload)?;
        execute_order(api, order_intent).await
    } else {
        Err(anyhow!("Could not process message"))
    }
}

async fn execute_order(api: &AlpacaConfig, o: OrderIntent) -> Result<AlpacaOrder> {
    let order = AlpacaOrder {
        symbol: o.symbol,
        qty: o.qty,
        order_type: o.order_type,
        side: o.side,
        time_in_force: o.time_in_force,
        extended_hours: o.extended_hours,
        client_order_id: Some(o.id.to_string()),
    };

    submit_order(api, &order).await
}
