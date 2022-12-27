use tokio_util::codec::{Framed, LengthDelimitedCodec};
use aggligator::alc::Stream;
use std::{io, iter};
use std::io::{SeekFrom};
use aggligator_util::net::tcp_connect;
use aggligator::cfg::Cfg;
use std::path::Path;
use std::time::Duration;
use adler::adler32_slice;
use prost::Message;
use futures_util::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, SinkExt, StreamExt};
use crate::{MessageType, sendfile_messages, SendfileError};
use crate::receiver::CHUNK_SIZE;

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

    pub async fn send_file(&mut self, mut file: impl AsyncRead + AsyncSeek + std::marker::Unpin, file_size: u64, file_name: &str) -> Result<(), SendfileError> {
        let message = sendfile_messages::FileTransferStart{
            file_name: file_name.to_string(),
            num_chunks: (file_size.div_ceil(CHUNK_SIZE as u64)) as u32
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
                            MessageType::FileTransferStart | MessageType::FileTransferPart | MessageType::AckFilePart => Err(SendfileError::InvalidResponse)
                        }
                    }
                }
            }
        }?;
        // File transfer accepted, begin transferring data
        let mut to_send: Vec<u32> = (0..message.num_chunks).into_iter().collect();
        let mut buffer = [0; CHUNK_SIZE as usize];
        while !to_send.is_empty() {
            let chunk = to_send.first().unwrap();
            file.seek(SeekFrom::Start((*chunk) as u64 * CHUNK_SIZE as u64)).await;
            if let Ok(num_bytes) = file.read(&mut buffer[0..CHUNK_SIZE as usize]).await {
                let checksum = adler32_slice(&buffer[0..num_bytes]);
                let message_out = sendfile_messages::FileTransferPart {
                    chunk_index: *chunk,
                    file_name: file_name.to_string(),
                    content: buffer[0..num_bytes].to_vec(),
                    checksum
                };
                log::info!("Sent chunk {}", chunk);
                self.client.send(iter::once(sendfile_messages::MessageType::FileTransferPart.to_id()).chain(message_out.encode_to_vec().into_iter()).collect()).await;

                // await ack
                let potential_ack = self.client.next().await.unwrap().unwrap();
                match sendfile_messages::MessageType::from_id(potential_ack[0]).unwrap() {
                    MessageType::FileTransferStart | MessageType::FileTransferPart  => {},
                    MessageType::Ack => {},
                    MessageType::AckFilePart => {
                        let ack_info = sendfile_messages::AckFilePart::decode(&potential_ack[1..]).unwrap();
                        match ack_info.chunk_index {
                            Some(next_chunk) => {
                                log::info!("Chunk {} acked", to_send[0]);
                                if next_chunk > to_send[0] {
                                    to_send.remove(0);
                                }
                            },
                            None => to_send.clear()
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
