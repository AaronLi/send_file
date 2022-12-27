use std::path::{Path, PathBuf};

use tokio::fs::File;

use crate::error::SendfileError;
use crate::sendfile_messages::MessageType;

mod receiver;
mod sender;

mod sendfile_messages {
    include!(concat!(env!("OUT_DIR"), "/sendfile_messages.rs"));

    pub(crate) enum MessageType {
        Ack,
        FileTransferStart,
        FileTransferPart,
        AckFileTransferPart
    }
    impl MessageType {
        pub(crate) fn to_id(&self) -> u8 {
            match self {
                MessageType::Ack => 0,
                MessageType::FileTransferStart => 1,
                MessageType::FileTransferPart => 2,
                MessageType::AckFileTransferPart => 3
            }
        }

        pub(crate) fn from_id(id: u8) -> Option<Self> {
            match id {
                0 => Some(MessageType::Ack),
                1 => Some(MessageType::FileTransferStart),
                2 => Some(MessageType::FileTransferPart),
                3 => Some(MessageType::AckFileTransferPart),
                _ => None
            }
        }
    }
}

pub mod error {
    #[derive(Debug, Clone, PartialEq)]
    pub enum SendfileError {
        InvalidMessageType,
        FileAlreadyExists,
        InvalidResponse,
        ReadBytesError,
        TimedOut,
        MessageDecodeError,
        RequestNotAccepted
    }
}

#[derive(Debug)]
enum TransferState {
    Accepted,
    InProgress(Vec<u32>),
    Finished
}

#[derive(Debug)]
struct TransferInfo {
    file_handle: File,
    num_chunks: u32,
    transfer_state: TransferState
}

impl TransferInfo {
    async fn new(root_folder: &Path, filename: &Path, num_chunks: u32) -> Result<Self, error::SendfileError> {
        let file_path = root_folder.join(filename);
        if file_path.exists() {
            Err(SendfileError::FileAlreadyExists)
        }else {
            Ok(TransferInfo {
                file_handle: File::create(root_folder.join(filename)).await.expect("Existence checked"),
                num_chunks,
                transfer_state: TransferState::Accepted
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};
    use std::sync::Once;
    use std::time::Duration;
    use aggligator::cfg::Cfg;

    use log::LevelFilter;
    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;
    use serial_test::serial;
    use tempdir::TempDir;

    use crate::receiver::FileReceiver;
    use crate::sender::FileSender;

    use super::*;

    const PORT: u16 = 21222;
    static INIT: Once = Once::new();
    fn setup_logger() {
        INIT.call_once(
        ||{env_logger::builder().filter_level(LevelFilter::Info).init()}
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_connect() {
        setup_logger();

        let test_folder = TempDir::new(
            &thread_rng().sample_iter(&Alphanumeric).take(10).map(char::from).collect::<String>()
        ).unwrap().path().to_path_buf();
        let server = FileReceiver::new(&test_folder, PORT, ||true);

        tokio::spawn(async move {server.serve(tempdir::TempDir::new("test").unwrap().into_path()).await});

        let mut client = FileSender::connect(vec!["localhost:21222".to_string()], PORT, Cfg::default()).await.unwrap();

        assert!(matches!(client.send_file(&PathBuf::from("/file.txt")).await, Ok(())));

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    #[serial]
    #[tokio::test]
    async fn test_connection_reject() {
        setup_logger();

        let test_folder = TempDir::new(
            &thread_rng().sample_iter(&Alphanumeric).take(10).map(char::from).collect::<String>()
        ).unwrap().path().to_path_buf();
        let server = FileReceiver::new(&test_folder, PORT, ||false);

        tokio::spawn(async move {server.serve(tempdir::TempDir::new("test").unwrap().into_path()).await});

        let mut cfg = Cfg::default();
        cfg.link_non_working_timeout = Duration::from_millis(20);
        cfg.link_ack_timeout_max = Duration::from_millis(20);
        cfg.no_link_timeout = Duration::from_millis(20);

        let mut client_result = FileSender::connect(vec!["localhost:21222".to_string()], PORT, cfg).await;
        let expected = Error::new(ErrorKind::TimedOut, "connect timeout");
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(matches!(client_result.unwrap_err(), expected));
    }
}
