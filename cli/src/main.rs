mod cli_man;

#[tokio::main]
async fn main() {
    ctrlc::set_handler(move || {
        println!("\nCtrl-C pressed, exiting...");
        std::process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");
    commons::init_tracing(None);
    tracing::info!("Welcome to WAL-rs CLI! Type 'help' for commands or 'exit' to quit.");
    let mut cli_manager = cli_man::CliManager::new();
    cli_manager.start().await;
}
