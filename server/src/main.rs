#[tokio::main]
async fn main() {
    commons::init_tracing(Some(tracing::Level::INFO));
}
