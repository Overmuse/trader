use config::{Config, ConfigError, Environment};
use kafka_settings::KafkaSettings;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AlpacaSettings {
    pub base_url: String,
    pub key_id: String,
    pub secret_key: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub alpaca: AlpacaSettings,
    pub kafka: KafkaSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::default();
        s.merge(Environment::new().separator("__"))?;
        s.try_into()
    }
}
