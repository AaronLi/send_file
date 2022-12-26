mod receiver;
mod sender;

use std::collections::HashMap;
use std::{fs, io, thread};
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use aggligator::alc::Stream;
use aggligator::cfg::Cfg;
use aggligator::connect::{Incoming, Server};
use aggligator::control::{Direction, Link};
use aggligator::io::{IoRx, IoTx};
use aggligator_util::net::{tcp_connect, tcp_server};
use aggligator_util::net::adv::{tcp_link_filter, tcp_listen, TcpLinkTag};
use std::future::IntoFuture;
use futures_util::{AsyncReadExt, SinkExt, StreamExt};
use log::info;
use prost::{DecodeError, Message};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use crate::error::SendfileError;
use crate::sendfile_messages::MessageType;

mod sendfile_messages {
    include!(concat!(env!("OUT_DIR"), "/sendfile_messages.rs"));

    pub(crate) enum MessageType {
        Ack,
        FileTransferStart,
        FileTransferPart,
    }
    impl MessageType {
        pub(crate) fn to_id(&self) -> u8 {
            match self {
                MessageType::Ack => 0,
                MessageType::FileTransferStart => 1,
                MessageType::FileTransferPart => 2
            }
        }

        pub(crate) fn from_id(id: u8) -> Option<Self> {
            match id {
                0 => Some(MessageType::Ack),
                1 => Some(MessageType::FileTransferStart),
                2 => Some(MessageType::FileTransferPart),
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

struct TransferInfo {
    file_handle: File,
    next_chunk: u32,
    num_chunks: u32
}

impl TransferInfo {
    async fn new(root_folder: &Path, filename: &Path, num_chunks: u32) -> Result<Self, error::SendfileError> {
        let file_path = root_folder.join(filename);
        if file_path.exists() {
            Err(SendfileError::FileAlreadyExists)
        }else {
            Ok(TransferInfo {
                file_handle: File::create(root_folder.join(filename)).await.expect("Existence checked"),
                next_chunk: 0,
                num_chunks
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fmt;
    use std::io::{Error, ErrorKind};
    use std::sync::Once;
    use serial_test::serial;
    use std::time::Duration;
    use log::LevelFilter;

    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;
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
