use tracing::{subscriber::set_global_default, Subscriber};
use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub fn get_subscriber() -> impl Subscriber + Send + Sync {
    FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish()
}

pub fn init_subscriber(subscriber: impl Subscriber + Send + Sync) {
    LogTracer::init().expect("Failed to set logger");
    set_global_default(subscriber).expect("Failed to set subscriber");
}
