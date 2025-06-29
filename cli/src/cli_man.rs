use std::collections::HashMap;
use std::str::FromStr;

use clap::{Parser, Subcommand, ValueEnum, command};

use commons::models::{
    AckLevel, AdminCommand, AdminResponse, ConsumerCommand, ConsumerResponse, Message,
    ProducerResponse,
};

use commons::producer::Producer;

pub struct CliManager {
    brokers: Vec<String>,
    mode: CliMode,
    debug_mode_mapped_brokers: HashMap<String, String>, // Maps broker names to addresses
}

impl CliManager {
    pub fn new() -> Self {
        // Initialize with an empty list of brokers
        let mut debug_mode_mapped_brokers = HashMap::new();
        debug_mode_mapped_brokers.insert(
            "walrs-srvr-0.walrs-headless-service.walrs.svc.cluster.local:5056".into(),
            "localhost:8080".into(),
        );
        debug_mode_mapped_brokers.insert(
            "walrs-srvr-1.walrs-headless-service.walrs.svc.cluster.local:5056".into(),
            "localhost:8081".into(),
        );
        debug_mode_mapped_brokers.insert(
            "walrs-srvr-2.walrs-headless-service.walrs.svc.cluster.local:5056".into(),
            "localhost:8082".into(),
        );
        CliManager {
            brokers: Vec::new(),
            mode: CliMode::Normal,
            debug_mode_mapped_brokers,
        }
    }
    pub async fn start(&mut self) {
        loop {
            print!(">> ");
            std::io::Write::flush(&mut std::io::stdout()).unwrap();

            let mut input = String::new();
            match std::io::stdin().read_line(&mut input) {
                Ok(_) => {
                    let command = input.trim();
                    if self.should_exit(command) {
                        println!("Exiting WAL-rs CLI. Goodbye!");
                        break;
                    }
                    self.handle_command(command).await;
                }
                Err(e) => {
                    eprintln!("Error reading input: {}", e);
                }
            }
        }
    }

    fn should_exit(&self, input: &str) -> bool {
        matches!(input.to_lowercase().as_str(), "exit" | "quit" | "q")
    }

    async fn handle_command(&mut self, input: &str) {
        if input.is_empty() {
            return;
        }

        // Split the input into arguments for clap parsing
        let args: Vec<&str> = input.split_whitespace().collect();

        // Add a dummy program name for clap (required)
        let mut full_args = vec!["cli"];
        full_args.extend(args);

        // Try to parse the command with clap
        match CliCommand::try_parse_from(full_args) {
            Ok(cmd) => {
                self.execute_command(cmd).await;
            }
            Err(e) => {
                // Print clap's error message (includes help)
                println!("{}", e);
            }
        }
    }

    async fn execute_command(&mut self, cmd: CliCommand) {
        match cmd.command {
            Commands::Use { brokers } => {
                if brokers.is_empty() {
                    eprintln!("ERROR: No brokers specified.");
                } else {
                    println!("INFO: Will use brokers: {}", brokers.join(", "));
                    self.brokers = brokers;
                }
            }
            Commands::GetTopicInfo { topic } => {
                if self.brokers.is_empty() {
                    eprintln!("ERROR: No brokers specified. Use 'use' command to set brokers.");
                    return;
                }
                println!("INFO: Getting info for topic: {}", topic);
                let admin_command: AdminCommand = AdminCommand::GetTopicInfo {
                    topic_names: vec![topic],
                };
                match commons::send_and_receive_admin_command(admin_command, &self.brokers[0]).await
                {
                    AdminResponse::RequestAccepted => {
                        println!("✅ Request accepted. Waiting for topic info...");
                    }
                    AdminResponse::TopicInfo { topics } => {
                        if topics.is_empty() {
                            println!("❌ No topics found.");
                        } else {
                            topics.iter().for_each(|t| {
                                println!(
                                    "✅ Topic: {}, Partitions: {}, Replication Factor: {}, Retention: {} minutes, Ack Level: {:?}",
                                    t.name,
                                    t.partitions.len(),
                                    t.replication_factor,
                                    t.retention_period_minutes,
                                    t.ack_level
                                );
                                t.partitions.iter().for_each(|p| {
                                    println!(
                                        "\t✅ Partition number: {}, Leader: {}, Followers: {:?}",
                                        p.number, p.leader_address, p.followers
                                    );
                                });
                            });
                        }
                    }
                    AdminResponse::Error(err) => eprintln!("❌ Failed to get topic info: {}", err),
                }
            }
            Commands::CreateTopic {
                topic,
                partitions,
                replication_factor,
                retention_period_minutes,
                ack_level,
            } => {
                if self.brokers.is_empty() {
                    eprintln!("ERROR: No brokers specified. Use 'use' command to set brokers.");
                    return;
                }
                println!("INFO: Creating topic: {}", topic);
                let admin_command: AdminCommand = AdminCommand::CreateTopic {
                    name: topic,
                    num_partitions: partitions,
                    replication_factor,
                    retention_period_minutes,
                    ack_level,
                };
                match commons::send_and_receive_admin_command(admin_command, &self.brokers[0]).await
                {
                    AdminResponse::RequestAccepted => {
                        println!("✅ Topic creation request accepted.");
                    }
                    AdminResponse::TopicInfo { topics } => {
                        println!(
                            "✅ Topic created successfully. Current topics: {:?}",
                            topics
                        );
                    }
                    AdminResponse::Error(err) => {
                        eprintln!("❌ Failed to create topic: {}", err);
                    }
                }
            }
            Commands::Echo { message } => println!("{}", message),
            Commands::Consume { topic, partition } => {
                if self.brokers.is_empty() {
                    eprintln!("❌ ERROR: No brokers specified. Use 'use' command to set brokers.");
                    return;
                }
                println!(
                    "Consuming messages from topic: {}, partition: {:?}",
                    topic, partition
                );
                let consumer_command = ConsumerCommand::FetchMessages {
                    topic: topic.clone(),
                    partition_number: partition.unwrap_or(0), // Default to partition 0 if not specified
                    offset: None,                             // No offset specified
                };
                match commons::send_and_receive_consumer_command(consumer_command, &self.brokers[0])
                    .await
                {
                    ConsumerResponse::MessagesFetched { messages } => {
                        if messages.is_empty() {
                            tracing::error!("No messages found in topic: {}", topic);
                        } else {
                            tracing::info!("✅ Fetched {} messages", messages.len());
                            for message in messages {
                                let payload = String::from_utf8_lossy(&message.payload);
                                tracing::info!(
                                    "\tMessage: {}, Key: {:?}, Headers: {:?}",
                                    payload,
                                    message.key,
                                    message.headers
                                );
                            }
                        }
                    }
                    ConsumerResponse::Error(err) => {
                        eprintln!("❌ Failed to consume messages: {}", err);
                    }
                }
            }
            Commands::SendMessage { message, topic } => {
                if self.brokers.is_empty() {
                    eprintln!("❌ ERROR: No brokers specified. Use 'use' command to set brokers.");
                    return;
                }
                println!("Sending message: {} to topic: {}", message, topic);
                let mut producer: Producer = Producer::new(self.brokers.clone());
                let message: Message = Message {
                    payload: message.as_bytes().to_vec(),
                    key: None,
                    headers: HashMap::new(),
                };
                producer.send(topic, &message);
                match producer.flush(self.debug_mode_mapped_brokers.clone()).await {
                    Ok(response) => match response {
                        ProducerResponse::MessagesPersisted { count } => {
                            tracing::info!("✅ Successfully sent {} messages.", count);
                        }
                        ProducerResponse::Error { message } => {
                            tracing::error!("❌ Failed to send messages: {}", message);
                        }
                        other => {
                            tracing::error!("❌ Unexpected response: {:?}", other);
                        }
                    },
                    Err(e) => {
                        tracing::error!("❌ Failed to flush producer: {}", e);
                    }
                }
            }
            Commands::Status => {
                tracing::info!("Status: CLI is running. Brokers: {:?}", self.brokers)
            }
            Commands::Clear => {
                tracing::info!("\x1B[2J\x1B[1;1H");
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
            Commands::Time => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                tracing::info!("Current timestamp: {}", now);
            }
            Commands::Mode { mode } => {
                tracing::info!("Switched to {:?} mode", mode);
                self.mode = mode
            }
        }
    }
}

#[derive(Parser)]
#[command(name = "")]
#[command(about = "Interactive CLI commands")]
#[command(disable_help_flag = true)] // We'll handle help manually
struct CliCommand {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Use {
        #[arg(short, long)]
        brokers: Vec<String>,
    },
    GetTopicInfo {
        #[arg(short, long)]
        topic: String,
    },
    CreateTopic {
        #[arg(short, long)]
        topic: String,

        #[arg(short, long)]
        partitions: Option<u8>,

        #[arg(short, long)]
        replication_factor: Option<u8>,

        #[arg(short = 'm', long)]
        retention_period_minutes: Option<u16>,

        #[arg(short, long)]
        ack_level: Option<AckLevel>,
    },
    /// Echo a message
    Echo { message: String },
    /// Send a message to server
    SendMessage {
        #[arg(short, long)]
        message: String,

        #[arg(short, long)]
        topic: String,
    },
    Consume {
        #[arg(short, long)]
        topic: String,

        #[arg(short, long)]
        partition: Option<u8>,
    },
    Mode {
        #[arg(short, long)]
        mode: CliMode,
    },
    /// Show status
    Status,
    /// Clear the screen
    Clear,
    /// Show current time
    Time,
}

#[derive(Clone, Debug, PartialEq, ValueEnum)]
pub enum CliMode {
    Debug,
    Normal,
}

impl FromStr for CliMode {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "debug" => Ok(CliMode::Debug),
            "normal" => Ok(CliMode::Normal),
            _ => Err(()),
        }
    }
}
