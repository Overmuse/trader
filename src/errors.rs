use thiserror::Error;

#[derive(Debug, Error)]
pub enum TraderError {
    #[error("Error when submitting order to Alpaca: {0}")]
    Alpaca(#[from] alpaca::errors::Error),

    #[error("Trader received invalid message: {0}")]
    InvalidMessage(String),

    #[error("Trader received empty message")]
    EmptyMessage,

    #[error("Failed to deserialize message into OrderIntent")]
    Serde(#[from] serde_json::Error),

    #[error("Missing Env variable: {0}")]
    EnvVar(#[from] std::env::VarError),
}

pub type Result<T> = std::result::Result<T, TraderError>;
