use tracing_subscriber::fmt::format::FmtSpan;

pub mod admin;
mod authenticator;
pub mod broker_response;
pub mod consumer;
pub mod message_batch;
pub mod models;
pub mod producer;

pub fn enable_tracing() {
    // let file_appender = tracing_appender::rolling::daily("/tmp/kraft-rs/logs/", "kraft-rs.log");
    // let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .with_thread_ids(true)
        // .with_writer(non_blocking)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    tracing::info!("Tracing enabled!");
}
