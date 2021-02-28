use crate::errors::{Result, TraderError};
use alpaca::{
    orders::{Order, OrderIntent, SubmitOrder},
    Client,
};
use rdkafka::{message::OwnedMessage, Message};
use tracing::info;

pub mod errors;
pub mod telemetry;

#[tracing::instrument(skip(msg))]
fn parse_message(msg: OwnedMessage) -> Result<OrderIntent> {
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => serde_json::from_str(payload).map_err(TraderError::Serde),
        Some(Err(e)) => Err(TraderError::InvalidMessage(e.to_string())),
        None => Err(TraderError::EmptyMessage),
    }
}

#[tracing::instrument(skip(api))]
async fn execute_order(api: &Client, oi: OrderIntent) -> Result<Order> {
    info!("Submitting order intent: {:#?}", &oi);
    api.send(SubmitOrder(oi)).await.map_err(TraderError::Alpaca)
}

#[tracing::instrument(skip(api, msg))]
pub async fn handle_message(api: &Client, msg: OwnedMessage) -> Result<Order> {
    let order_intent = parse_message(msg)?;
    execute_order(api, order_intent).await
}

#[cfg(test)]
mod test {
    use super::*;
    use rdkafka::message::Timestamp;

    #[tokio::test]
    async fn unwrap_msg() {
        let payload = r#"{
            "symbol":"AAPL",
            "qty":"1",
            "side":"buy",
            "type":"limit",
            "limit_price":"100",
            "time_in_force":"gtc",
            "extended_hours":false,
            "client_order_id":"TEST",
            "order_class":{
                "bracket":{
                    "take_profit":{
                        "limit_price":301.0
                    },
                    "stop_loss":{
                        "stop_price":299.0,
                        "limit_price":298.5
                    }
                }
            }
        }"#
        .as_bytes()
        .to_vec();
        let msg = OwnedMessage::new(
            Some(payload), // payload
            None,          // header
            "test".into(), // topic
            Timestamp::NotAvailable,
            1,    // partition
            0,    // offset
            None, // headers
        );
        let oi = parse_message(msg).await.unwrap();
        assert_eq!(oi.symbol, "AAPL".to_string());
    }

    #[tokio::test]
    async fn empty_msg() {
        let msg = OwnedMessage::new(
            None,          // payload
            None,          // header
            "test".into(), // topic
            Timestamp::NotAvailable,
            1,    // partition
            0,    // offset
            None, // headers
        );
        let e = parse_message(msg).await;
        assert!(e.is_err());
        assert!(matches!(e.err().unwrap(), TraderError::EmptyMessage));
    }

    #[tokio::test]
    async fn bad_msg() {
        let msg = OwnedMessage::new(
            Some("Blargh!".as_bytes().to_vec()), // payload
            None,                                // header
            "test".into(),                       // topic
            Timestamp::NotAvailable,
            1,    // partition
            0,    // offset
            None, // headers
        );
        let e = parse_message(msg).await;
        assert!(e.is_err());
        if let TraderError::Serde(_) = e.err().unwrap() {
        } else {
            panic!("Expected Serde")
        }
    }
}
