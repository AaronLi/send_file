use std::io;
use std::io::Error;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use aggligator::alc::Stream;
use aggligator::cfg::Cfg;
use aggligator_util::net::{tcp_connect, tcp_server};
use futures_util::{SinkExt, StreamExt};
use prost::{DecodeError, Message};
use tokio::io::AsyncReadExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
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

struct FileSender {
    client: Framed<Stream, LengthDelimitedCodec>
}

struct FileReceiver {
    destination_path: PathBuf,
    port: u16
}

impl FileReceiver {
    fn new(path: &Path, port: u16) -> Self {
        FileReceiver{
            destination_path: path.to_path_buf(),
            port
        }
    }

    async fn serve(&self) -> io::Result<()> {
        tcp_server(
            Cfg::default(),
            SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), self.port),
            |stream: Stream| async move {
                let (tx, mut rx) = Framed::new(stream, LengthDelimitedCodec::default()).split();

                loop {
                    match rx.next().await {
                        None => println!("No data"),
                        Some(m) => {
                            match m {
                                Ok(b) => {
                                    match MessageType::from_id(b[0]).unwrap() {
                                        MessageType::Ack => println!("Ack: {:?}", &b[1..]),
                                        MessageType::FileTransferStart => {
                                            let file_transfer_info: Result<sendfile_messages::FileTransferStart, DecodeError> = sendfile_messages::FileTransferStart::decode(&b[1..]);
                                            println!("Transfer start: {:?}", file_transfer_info);
                                        },
                                        MessageType::FileTransferPart => println!("Filepart: {:?}", &b[1..])
                                    }
                                }
                                Err(e) => println!("Error: {:?}", e)
                            }
                        }
                    }
                }
            }
        ).await
    }
}

impl FileSender {
    async fn connect(targets: Vec<String>, port: u16) -> io::Result<Self> {
        Ok(FileSender{
            client: Framed::new(tcp_connect(Cfg::default(), targets, port).await?, LengthDelimitedCodec::default())
        })
    }

    async fn send_file(&mut self, file_path: &Path) -> Result<(), Error> {
        let message = sendfile_messages::FileTransferStart{
            file_name: String::from(file_path.file_name().unwrap().to_str().unwrap()),
            num_chunks: 1234
        };
        self.client.send(vec![sendfile_messages::MessageType::FileTransferStart as u8].into_iter().chain(message.encode_to_vec().into_iter()).collect::<Vec<u8>>().into()).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;
    use tempdir::TempDir;

    use super::*;

    const PORT: u16 = 21222;

    #[tokio::test]
    async fn test_connect() {
        let test_folder = TempDir::new(
            &thread_rng().sample_iter(&Alphanumeric).take(10).map(char::from).collect::<String>()
        ).unwrap().path().to_path_buf();
        let server = FileReceiver::new(&test_folder, PORT);

        tokio::spawn(async move {server.serve().await});

        let mut client = FileSender::connect(vec!["localhost:21222".to_string()], PORT).await.unwrap();

        client.send_file(&PathBuf::from("/file.txt")).await.unwrap();

        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
