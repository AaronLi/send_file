use tokio_util::codec::{Framed, LengthDelimitedCodec};
use aggligator::alc::Stream;
use std::io;
use aggligator_util::net::tcp_connect;
use aggligator::cfg::Cfg;
use std::path::Path;
use std::time::Duration;
use prost::Message;
use futures_util::{SinkExt, StreamExt};
use crate::{MessageType, sendfile_messages, SendfileError};

#[derive(Debug)]
pub struct FileSender {
    client: Framed<Stream, LengthDelimitedCodec>
}

impl FileSender {
    pub async fn connect(targets: Vec<String>, port: u16, cfg: Cfg) -> io::Result<Self> {
        Ok(FileSender{
            client: Framed::new(tcp_connect(cfg, targets, port).await?, LengthDelimitedCodec::default())
        })
    }

    pub async fn send_file(&mut self, file_path: &Path) -> Result<(), SendfileError> {
        let message = sendfile_messages::FileTransferStart{
            file_name: String::from(file_path.file_name().unwrap().to_str().unwrap()),
            num_chunks: 1234
        };
        log::info!("Sent request to transfer file {:?}", message);
        self.client.send(vec![MessageType::FileTransferStart as u8].into_iter().chain(message.encode_to_vec().into_iter()).collect::<Vec<u8>>().into()).await;

        let response = tokio::time::timeout(Duration::from_secs(10), self.client.next()).await.map_err(|_|SendfileError::InvalidMessageType)?;
        match response {
            None => Err(SendfileError::FileAlreadyExists),
            Some(r) => {
                let bytes = r.map_err(|_|{SendfileError::ReadBytesError})?;
                match MessageType::from_id(bytes[0]) {
                    None => Err(SendfileError::InvalidMessageType),
                    Some(id) => {
                        match id {
                            MessageType::Ack => {
                                let message_data = sendfile_messages::Ack::decode(&bytes[1..]).map_err(|_|SendfileError::MessageDecodeError)?;
                                if message_data.ok {
                                    log::info!("Accepted request for File transfer");
                                    Ok(())
                                }else{
                                    log::info!("Request not accepted");
                                    Err(SendfileError::RequestNotAccepted)
                                }

                            },
                            MessageType::FileTransferStart | MessageType::FileTransferPart | MessageType::AckFileTransferPart => Err(SendfileError::InvalidResponse)
                        }
                    }
                }
            }
        }?;
        // File transfer accepted, begin transferring data

        Ok(())
    }
}
