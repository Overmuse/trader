use alpaca::{
    common::Order,
    orders::{OrderIntent, SubmitOrder},
    Client, Side,
};
use anyhow::{anyhow, Result};
use futures::StreamExt;
use kafka_settings::consumer;
use rdkafka::{message::OwnedMessage, Message};
use tracing::{info, warn};
use trading_base::TradeIntent;

mod settings;
pub use settings::Settings;
pub mod telemetry;

#[tracing::instrument(name = "Parsing message into TradeIntent", skip(msg))]
fn parse_message(msg: OwnedMessage) -> Result<TradeIntent> {
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => serde_json::from_str(payload).map_err(From::from),
        Some(Err(e)) => Err(e.into()),
        None => Err(anyhow!("Empty message received")),
    }
}

fn translate_intent(ti: TradeIntent) -> OrderIntent {
    let (qty, side) = if ti.qty > 0 {
        (ti.qty as usize, Side::Buy)
    } else {
        (ti.qty.abs() as usize, Side::Sell)
    };
    let order_type = match ti.order_type {
        trading_base::OrderType::Market => alpaca::OrderType::Market,
        trading_base::OrderType::Limit { limit_price } => alpaca::OrderType::Limit { limit_price },
        trading_base::OrderType::Stop { stop_price } => alpaca::OrderType::Stop { stop_price },
        trading_base::OrderType::StopLimit {
            stop_price,
            limit_price,
        } => alpaca::OrderType::StopLimit {
            stop_price,
            limit_price,
        },
    };
    let time_in_force = match ti.time_in_force {
        trading_base::TimeInForce::GoodTilCanceled => alpaca::TimeInForce::GoodTilCancelled,
        trading_base::TimeInForce::Day => alpaca::TimeInForce::Day,
        trading_base::TimeInForce::ImmediateOrCancel => alpaca::TimeInForce::ImmediateOrCancel,
        trading_base::TimeInForce::FillOrKill => alpaca::TimeInForce::FillOrKill,
        trading_base::TimeInForce::Open => alpaca::TimeInForce::Open,
        trading_base::TimeInForce::Close => alpaca::TimeInForce::Close,
    };
    OrderIntent::new(&ti.ticker)
        .qty(qty)
        .side(side)
        .order_type(order_type)
        .time_in_force(time_in_force)
        .client_order_id(ti.id.to_string())
}

#[tracing::instrument(name = "Executing OrderIntent", skip(api, oi))]
async fn execute_order(api: &Client, oi: &OrderIntent) -> Result<Order> {
    info!("Submitting order intent: {:?}", &oi);
    api.send(SubmitOrder(oi.clone())).await.map_err(From::from)
}

#[tracing::instrument(name = "Received message", skip(api, msg))]
async fn handle_message(api: &Client, msg: OwnedMessage) -> Result<Order> {
    let trade_intent = parse_message(msg)?;
    let order_intent = translate_intent(trade_intent);
    again::retry(|| execute_order(api, &order_intent)).await
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
            "ticker":"AAPL",
            "qty":1,
            "order_type":"limit",
            "limit_price":"100",
            "time_in_force":"gtc",
            "id":"904837e3-3b76-47ec-b432-046db621571b"
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
        let ti = parse_message(msg).unwrap();
        assert_eq!(&ti.ticker, "AAPL");
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
        let payload = r#"{"ticker":"AAPL","qty":1,"order_type":"limit","limit_price":100,"time_in_force":"gtc","id":"904837e3-3b76-47ec-b432-046db621571b"}"#;
        let order_payload = r#"{"symbol":"AAPL","qty":"1","side":"buy","type":"limit","limit_price":"100","time_in_force":"gtc","extended_hours":false,"client_order_id":"904837e3-3b76-47ec-b432-046db621571b","order_class":"simple"}"#;
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
            .match_body(order_payload)
            .with_body(
                r#"{
		    "id": "904837e3-3b76-47ec-b432-046db621571b",
		    "client_order_id": "904837e3-3b76-47ec-b432-046db621571b",
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
		    "qty": "1",
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

    #[tokio::test]
    async fn handles_trainsient_errors() {
        let payload = r#"{"ticker":"AAPL","qty":1,"order_type":"limit","limit_price":"100","time_in_force":"gtc","id":"904837e3-3b76-47ec-b432-046db621571b"}"#;
        let order_payload = r#"{"symbol":"AAPL","qty":"1","side":"buy","type":"limit","limit_price":"100","time_in_force":"gtc","extended_hours":false,"client_order_id":"904837e3-3b76-47ec-b432-046db621571b","order_class":"simple"}"#;
        let msg = OwnedMessage::new(
            Some(payload.as_bytes().to_vec()), // payload
            None,                              // header
            "test".into(),                     // topic
            Timestamp::NotAvailable,           //timestamp
            1,                                 // partition
            0,                                 // offset
            None,                              // headers
        );

        let m1 = mock("POST", "/orders")
            .match_header("apca-api-key-id", "APCA_API_KEY_ID")
            .match_header("apca-api-secret-key", "APCA_API_SECRET_KEY")
            .match_body(order_payload)
            .with_status(500)
            .create();
        let m2 = mock("POST", "/orders")
            .match_header("apca-api-key-id", "APCA_API_KEY_ID")
            .match_header("apca-api-secret-key", "APCA_API_SECRET_KEY")
            .match_body(order_payload)
            .with_body(
                r#"{
        	    "id": "904837e3-3b76-47ec-b432-046db621571b",
        	    "client_order_id": "904837e3-3b76-47ec-b432-046db621571b",
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
        	    "qty": "1",
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
        m1.assert();
        m2.assert();
    }
}
