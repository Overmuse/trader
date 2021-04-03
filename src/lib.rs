use alpaca::{
    common::Order,
    orders::{OrderIntent, SubmitOrder},
    Client,
};
use anyhow::{anyhow, Result};
use futures::StreamExt;
use kafka_settings::consumer;
use rdkafka::{message::OwnedMessage, Message};
use tracing::{info, warn};

mod settings;
pub use settings::Settings;
pub mod telemetry;

#[tracing::instrument(name = "Parsing message into OrderIntent", skip(msg))]
fn parse_message(msg: OwnedMessage) -> Result<OrderIntent> {
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => serde_json::from_str(payload).map_err(From::from),
        Some(Err(e)) => Err(e.into()),
        None => Err(anyhow!("Empty message received")),
    }
}

#[tracing::instrument(name = "Executing OrderIntent", skip(api, oi))]
async fn execute_order(api: &Client, oi: OrderIntent) -> Result<Order> {
    info!("Submitting order intent: {:?}", &oi);
    api.send(SubmitOrder(oi)).await.map_err(From::from)
}

#[tracing::instrument(name = "Received message", skip(api, msg))]
async fn handle_message(api: &Client, msg: OwnedMessage) -> Result<Order> {
    let order_intent = parse_message(msg)?;
    execute_order(api, order_intent).await
}

pub async fn run(settings: Settings) -> Result<()> {
    let api = Client::new(
        settings.alpaca.base_url,
        settings.alpaca.key_id,
        settings.alpaca.secret_key,
    )?;

    let c = consumer(&settings.kafka)?;

    c.stream()
        .for_each_concurrent(None, |msg| async {
            match msg {
                Ok(msg) => {
                    let order = handle_message(&api, msg.detach()).await;
                    match order {
                        Ok(o) => info!("Submitted order: {:?}", o),
                        Err(e) => warn!("Failed to submit order: {:?}", e),
                    }
                }
                Err(e) => warn!("Error received: {:?}", e),
            }
        })
        .await;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use alpaca::Client;
    use mockito::mock;
    use rdkafka::message::Timestamp;

    #[test]
    fn unwrap_msg() {
        let payload = r#"{
            "symbol":"AAPL",
            "qty":"1.0",
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
        let oi = parse_message(msg).unwrap();
        assert_eq!(oi.symbol, "AAPL".to_string());
    }

    #[test]
    fn empty_msg() {
        let msg = OwnedMessage::new(
            None,          // payload
            None,          // header
            "test".into(), // topic
            Timestamp::NotAvailable,
            1,    // partition
            0,    // offset
            None, // headers
        );
        let e = parse_message(msg);
        assert!(e.is_err());
    }

    #[test]
    fn bad_msg() {
        let msg = OwnedMessage::new(
            Some("Blargh!".as_bytes().to_vec()), // payload
            None,                                // header
            "test".into(),                       // topic
            Timestamp::NotAvailable,             //timestamp
            1,                                   // partition
            0,                                   // offset
            None,                                // headers
        );
        let e = parse_message(msg);
        assert!(e.is_err());
    }

    #[tokio::test]
    async fn generates_correct_trade() {
        let payload = r#"{"symbol":"AAPL","qty":"1.5","side":"buy","type":"limit","limit_price":"100","time_in_force":"gtc","extended_hours":false,"client_order_id":"TEST","order_class":"simple"}"#;
        let msg = OwnedMessage::new(
            Some(payload.as_bytes().to_vec()), // payload
            None,                              // header
            "test".into(),                     // topic
            Timestamp::NotAvailable,           //timestamp
            1,                                 // partition
            0,                                 // offset
            None,                              // headers
        );

        let _m = mock("POST", "/orders")
            .match_header("apca-api-key-id", "APCA_API_KEY_ID")
            .match_header("apca-api-secret-key", "APCA_API_SECRET_KEY")
            .match_body(payload)
            .with_body(
                r#"{
		    "id": "904837e3-3b76-47ec-b432-046db621571b",
		    "client_order_id": "TEST",
		    "created_at": "2018-10-05T05:48:59Z",
		    "updated_at": "2018-10-05T05:48:59Z",
		    "submitted_at": "2018-10-05T05:48:59Z",
		    "filled_at": null,
		    "expired_at": null,
		    "canceled_at": null,
		    "failed_at": null,
		    "replaced_at": null,
		    "replaced_by": null,
		    "replaces": null,
		    "asset_id": "904837e3-3b76-47ec-b432-046db621571b",
		    "symbol": "AAPL",
		    "asset_class": "us_equity",
                    "notional": null,
		    "qty": "1.5",
		    "filled_qty": "0",
                    "filled_avg_price": null,
		    "type": "limit",
		    "side": "buy",
		    "time_in_force": "gtc",
		    "limit_price": "100.00",
		    "status": "new",
		    "extended_hours": false,
		    "legs": null,
                    "trail_price": null,
                    "trail_percent": null,
                    "hwm": null
		}"#,
            )
            .create();
        let client = Client::new(
            mockito::server_url(),
            "APCA_API_KEY_ID".to_string(),
            "APCA_API_SECRET_KEY".to_string(),
        )
        .unwrap();

        handle_message(&client, msg).await.unwrap();
    }
}
