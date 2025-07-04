use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use prometheus::{CounterVec, Encoder, Opts, Registry, TextEncoder};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// Define metric events that can be sent to the metrics server
#[derive(Debug, Clone)]
pub enum MetricEvent {
    MessagesWritten {
        topic: String,
        partition: u8,
        message_count: usize,
    },
}

#[derive(Clone)]
pub struct WalrsMetrics {
    pub registry: Arc<Registry>,
    pub messages_written: CounterVec,
}

impl WalrsMetrics {
    pub fn new() -> Self {
        let registry = Arc::new(Registry::new());

        // Create a counter vector for messages written per topic and partition
        let messages_written = CounterVec::new(
            Opts::new(
                "walrs_messages_written_total",
                "Total number of messages written to each topic and partition",
            ),
            &["topic", "partition"],
        )
        .expect("Failed to create messages_written counter");

        registry
            .register(Box::new(messages_written.clone()))
            .unwrap();

        Self {
            registry,
            messages_written,
        }
    }

    // Process incoming metric events
    pub fn process_event(&self, event: MetricEvent) {
        match event {
            MetricEvent::MessagesWritten {
                topic,
                partition,
                message_count,
            } => {
                self.messages_written
                    .with_label_values(&[&topic, &partition.to_string()])
                    .inc_by(message_count as f64);
            }
        }
    }
}

pub struct MetricsServer {
    metrics: WalrsMetrics,
}

impl MetricsServer {
    pub fn new() -> Self {
        let metrics = WalrsMetrics::new();
        Self { metrics }
    }

    pub async fn start(
        self,
        mut metric_rx: mpsc::Receiver<MetricEvent>,
        port: u16,
        cancellation_token: CancellationToken,
    ) {
        let metrics_for_handler = self.metrics.clone();

        // Spawn the metric event processor
        let metrics_processor = self.metrics.clone();
        let processor_cancellation = cancellation_token.child_token();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = processor_cancellation.cancelled() => {
                        tracing::info!("Metrics processor shutting down");
                        break;
                    }
                    event = metric_rx.recv() => {
                        match event {
                            Some(event) => {
                                metrics_processor.process_event(event);
                            }
                            None => {
                                tracing::info!("Metrics channel closed");
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Start the HTTP server
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .route("/", get(health_handler))
            .with_state(metrics_for_handler);

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .expect("Failed to bind metrics server");

        tracing::info!("Metrics server listening on port {}", port);

        let server = axum::serve(listener, app);

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Metrics server shutting down");
            }
            result = server => {
                if let Err(e) = result {
                    tracing::error!("Metrics server error: {}", e);
                }
            }
        }
    }
}

async fn metrics_handler(State(metrics): State<WalrsMetrics>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();

    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!("Failed to encode metrics: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to encode metrics",
        )
            .into_response();
    }

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, encoder.format_type())],
        buffer,
    )
        .into_response()
}

async fn health_handler() -> impl IntoResponse {
    "OK"
}
