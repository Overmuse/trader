use tracing::subscriber::{set_global_default, Subscriber};
use tracing_subscriber::EnvFilter;

pub fn get_subscriber() -> impl Subscriber + Send + Sync {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .finish()
}

pub fn init_subscriber(subscriber: impl Subscriber + Send + Sync) {
    set_global_default(subscriber).expect("Failed to set subscriber");
}
