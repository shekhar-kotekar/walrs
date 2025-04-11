use std::fs;

use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

//TODO: Write logic to periodically flush the buffer to disk after heartbeat interval.
pub struct SegmentWriter {
    segment_file_path: String,
}

impl SegmentWriter {
    pub fn new(segment_base_path: String, segment_id: u32) -> Self {
        fs::create_dir_all(&segment_base_path)
            .expect(format!("Failed to create directory: {}", segment_base_path).as_str());
        SegmentWriter {
            segment_file_path: format!("{}/{}.log", segment_base_path, segment_id),
        }
    }

    pub async fn start(
        &self,
        mut writer_rx: mpsc::Receiver<SegmentWriterCommand>,
        cancellation_token: CancellationToken,
    ) {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.segment_file_path)
            .await
            .expect(format!("Failed to open file: {}", self.segment_file_path).as_str());

        let mut buffered_writer = tokio::io::BufWriter::new(file);

        loop {
            tokio::select! {
                Some(command) = writer_rx.recv() => {
                    match command {
                        SegmentWriterCommand::Write { data, response_tx } => {
                            let result = buffered_writer.write_all(&data).await;
                            if result.is_ok() {
                                response_tx.send(SegmentWriterResponse::WriteSuccess).unwrap();
                            } else {
                                response_tx.send(SegmentWriterResponse::WriteFailure).unwrap();
                            }
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    buffered_writer.flush().await.unwrap();
                    buffered_writer.shutdown().await.unwrap();
                    tracing::info!("Segment writer for {} is stopped.", self.segment_file_path);
                    break;
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum SegmentWriterCommand {
    Write {
        data: Vec<u8>,
        response_tx: oneshot::Sender<SegmentWriterResponse>,
    },
}

#[derive(Debug, PartialEq)]
pub enum SegmentWriterResponse {
    WriteSuccess,
    WriteFailure,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use tokio::fs;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn segment_writer_should_be_able_to_write_to_file() {
        let temp_dir = TempDir::new("log_dir_prefix").unwrap();
        let segment_base_path: String = temp_dir
            .path()
            .join("test_topic")
            .to_str()
            .unwrap()
            .to_string();

        tracing::debug!("Segment base path: {:?}", segment_base_path);

        let segment_id = 1;
        let segment_writer = SegmentWriter::new(segment_base_path.clone(), segment_id);

        let (writer_tx, writer_rx) = mpsc::channel::<SegmentWriterCommand>(1);
        let cancellation_token = CancellationToken::new();
        let writer_ct = cancellation_token.child_token();
        let writer_handle = tokio::spawn(async move {
            segment_writer.start(writer_rx, writer_ct).await;
        });

        let data = b"Hello, World!";
        let (response_tx, response_rx) = oneshot::channel::<SegmentWriterResponse>();
        let segment_writer_command = SegmentWriterCommand::Write {
            data: data.to_vec(),
            response_tx,
        };
        writer_tx.send(segment_writer_command).await.unwrap();
        let response: SegmentWriterResponse = response_rx.await.unwrap();
        assert_eq!(response, SegmentWriterResponse::WriteSuccess);

        cancellation_token.cancel();
        writer_handle.await.unwrap();

        let expected_file_path = format!("{}/{}.log", segment_base_path, segment_id);
        let expected_file_contents = fs::read(expected_file_path).await.unwrap();
        assert_eq!(expected_file_contents, data);

        fs::remove_dir_all(segment_base_path).await.unwrap();
    }
}
