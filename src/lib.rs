use alpaca::{
    orders::{submit_order, Order, OrderIntent},
    AlpacaConfig,
};
use log::info;
use rdkafka::{message::OwnedMessage, Message};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TraderError {
    #[error("Error when submitting order to Alpaca: {0}")]
    Alpaca(String),

    #[error("Trader received invalid message: {0}")]
    InvalidMessage(String),

    #[error("Trader received empty message")]
    EmptyMessage,

    #[error("Failed to deserialize message into OrderIntent")]
    Serde(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, TraderError>;

async fn parse_message(msg: OwnedMessage) -> Result<OrderIntent> {
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => serde_json::from_str(payload).map_err(|e| TraderError::Serde(e)),
        Some(Err(e)) => Err(TraderError::InvalidMessage(e.to_string())),
        None => Err(TraderError::EmptyMessage),
    }
}

async fn execute_order(api: &AlpacaConfig, oi: OrderIntent) -> Result<Order> {
    info!("Submitting order intent: {:#?}", &oi);
    submit_order(api, &oi)
        .await
        .map_err(|e| TraderError::Alpaca(e.to_string()))
}

pub async fn handle_message(api: &AlpacaConfig, msg: OwnedMessage) -> Result<Order> {
    let order_intent = parse_message(msg).await?;
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
        if let TraderError::Serde(s) = e.err().unwrap() {
        } else {
            panic!("Expected Serde")
        }
    }
}
