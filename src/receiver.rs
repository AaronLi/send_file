use aggligator::cfg::Cfg;
use aggligator::connect::Server;
use aggligator_util::net::adv::{tcp_link_filter, tcp_listen};
use core::marker::{Send, Sync};
use core::option::Option::{None, Some};
use core::result::Result;
use core::result::Result::{Err, Ok};
use std::collections::{HashMap, VecDeque};
use std::{fs, io, iter};
use std::future::IntoFuture;
use std::io::SeekFrom;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{PathBuf};
use std::sync::Arc;
use adler::adler32_slice;
use debug_ignore::DebugIgnore;
use futures_util::{SinkExt, StreamExt};
use log::{info};
use prost::DecodeError;
use tokio_util::codec::Framed;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use prost::Message;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::time::Instant;
use crate::{MessageType, sendfile_messages, TransferInfo, TransferState};

pub const CHUNK_SIZE: u32 = 1024;

#[derive(Debug, Clone)]
pub struct FileReceiver {
    port: u16,
    connection_verifier: DebugIgnore<Arc<dyn Fn() -> bool + Send + Sync + 'static>>,
    file_transfers: Arc<Mutex<HashMap<String, TransferInfo>>>,
    chunk_history: Arc<Mutex<VecDeque<ChunkSummary>>>
}

#[derive(Debug, Clone)]
struct ChunkSummary {
    size: u32,
    timestamp: Instant
}

impl FileReceiver {
    pub fn new<F>(port: u16, connection_verifier: F) -> Self
        where F: Fn() -> bool + Send + Sync + 'static {
        FileReceiver{
            port,
            connection_verifier: debug_ignore::DebugIgnore(Arc::new(connection_verifier)),
            file_transfers: Arc::new(Mutex::new(HashMap::new())),
            chunk_history: Arc::new(Mutex::new(VecDeque::new()))
        }
    }

    async fn chunk_received(chunk_history: Arc<Mutex<VecDeque<ChunkSummary>>>, size: u32) {
        let mut chunks = chunk_history.lock().await;
        chunks.push_back(
            ChunkSummary{
                size,
                timestamp: tokio::time::Instant::now()
            }
        );
        while let Some(c) = chunks.front() {
            if c.timestamp.elapsed().as_secs() > 10 {
                chunks.pop_front();
            }else{
                break;
            }
        }
    }

    async fn files_open(&self) -> usize {
        self.file_transfers.lock().await.len()
    }

    async fn bytes_per_second(chunk_history: Arc<Mutex<VecDeque<ChunkSummary>>>) -> f32 {
        let chunk_history_locked = chunk_history.lock().await;
        if chunk_history_locked.len() <= 1 {
            return 0f32;
        }
        let elapsed = chunk_history_locked.front().expect("At least 2 elements").timestamp.elapsed().as_secs_f32() - chunk_history_locked.back().expect("At least 2 elements").timestamp.elapsed().as_secs_f32();
        chunk_history_locked.iter().map(|c|c.size as f32).sum::<f32>() / elapsed
    }

    pub async fn serve(self, root: PathBuf) -> io::Result<()> {
        fs::create_dir_all(&root)?;
        let server = Server::new(Cfg::default());
        let mut listener = server.listen().await.unwrap();
        tokio::spawn(async move {
            loop {
                let inc = listener.next().await.unwrap();
                info!("Received connection {:?}", inc.remote_server_id());
                if !(self.connection_verifier)() {
                    inc.refuse().await;
                    info!("Connection refused");
                    continue
                }
                let (mut task, ch, _control) = inc.accept().await;
                let root = root.clone();
                let file_transfers = Arc::clone(&self.file_transfers);
                let chunk_history = Arc::clone(&self.chunk_history);
                task.set_link_filter(tcp_link_filter);

                tokio::spawn(task.into_future());
                tokio::spawn(async move {
                    let framed_channel = Framed::new(ch.into_stream(), LengthDelimitedCodec::default());
                    let (mut tx, mut rx) = framed_channel.split();
                    loop {
                        match rx.next().await {
                            None => break,
                            Some(m) => {
                                match m {
                                    Ok(b) => {
                                        match MessageType::from_id(b[0]).unwrap() {
                                            MessageType::Ack => println!("Ack: {:?}", &b[1..]),
                                            MessageType::FileTransferStart => {
                                                let file_transfer_info: Result<sendfile_messages::FileTransferStart, DecodeError> = sendfile_messages::FileTransferStart::decode(&b[1..]);
                                                match file_transfer_info {
                                                    Ok(info) => {
                                                        println!("Transfer start: {:?}", info);
                                                        let transfer_handle = TransferInfo::new(
                                                            &root.clone(),
                                                            &PathBuf::from(&info.file_name),
                                                            info.num_chunks
                                                        ).await;
                                                        match transfer_handle {
                                                            Ok(handle) => {
                                                                file_transfers.lock().await.insert(info.file_name.clone(), handle);
                                                                tx.send(vec![MessageType::Ack.to_id()].into_iter().chain(sendfile_messages::Ack {ok: true, reason: None}.encode_to_vec().into_iter()).collect::<Vec<u8>>().into()).await.unwrap();
                                                            }
                                                            Err(_) => {
                                                                tx.send(vec![MessageType::Ack.to_id()].into_iter().chain(sendfile_messages::Ack {ok: false, reason: Some("File already exists".to_string())}.encode_to_vec().into_iter()).collect::<Vec<u8>>().into()).await.unwrap();
                                                            }
                                                        }
                                                    },
                                                    Err(_) => println!("Failed to parse transfer request")
                                                }
                                            },
                                            MessageType::FileTransferPart => {
                                                let file_transfer_part = sendfile_messages::FileTransferPart::decode(&b[1..]);
                                                match file_transfer_part {
                                                    Ok(part) => {
                                                        let mut file_info = file_transfers.lock().await;

                                                        match file_info.get_mut(&part.file_name) {
                                                            None => println!("Received file part for uninitiated file"),
                                                            Some(info) => {
                                                                match &mut info.transfer_state {
                                                                    TransferState::Accepted => {
                                                                        let csum = adler32_slice(&part.content);
                                                                        if csum != part.checksum {
                                                                            let _ = tx.send(
                                                                                iter::once(sendfile_messages::MessageType::AckFilePart.to_id()).chain(sendfile_messages::AckFilePart{chunk_index: Some(0), file_name: part.file_name}.encode_to_vec()).collect::<Vec<u8>>().into()
                                                                            ).await;
                                                                            continue
                                                                        }
                                                                        info!("Received chunk {} from client", part.chunk_index);
                                                                        Self::chunk_received(Arc::clone(&chunk_history), part.content.len() as u32).await;
                                                                        info.file_handle.seek(SeekFrom::Start(CHUNK_SIZE as u64 * part.chunk_index as u64)).await;
                                                                        info.file_handle.write(&part.content).await;
                                                                        let remaining_files = (0..info.num_chunks).into_iter().filter(|c|*c!=part.chunk_index).collect::<Vec<u32>>();
                                                                        tx.send(
                                                                          iter::once(sendfile_messages::MessageType::AckFilePart.to_id()).chain(sendfile_messages::AckFilePart{chunk_index: remaining_files.first().cloned(), file_name: part.file_name.clone()}.encode_to_vec()).collect::<Vec<u8>>().into()
                                                                        ).await.unwrap();
                                                                        info.transfer_state = if remaining_files.len() == 0 {
                                                                            TransferState::Finished
                                                                        }else{
                                                                            TransferState::InProgress(remaining_files)
                                                                        };
                                                                        info!("{:?}", info.transfer_state);
                                                                    }
                                                                    TransferState::InProgress(remaining) => {
                                                                        let csum = adler32_slice(&part.content);
                                                                        if csum != part.checksum {
                                                                            tx.send(
                                                                                iter::once(sendfile_messages::MessageType::AckFilePart.to_id()).chain(sendfile_messages::AckFilePart{chunk_index: remaining.first().cloned(), file_name: part.file_name.clone()}.encode_to_vec()).collect::<Vec<u8>>().into()
                                                                            ).await.unwrap();
                                                                            continue
                                                                        }
                                                                        info!("Received chunk {} from client", part.chunk_index);
                                                                        Self::chunk_received(Arc::clone(&chunk_history), part.content.len() as u32).await;
                                                                        info.file_handle.seek(SeekFrom::Start(CHUNK_SIZE as u64 * part.chunk_index as u64)).await;
                                                                        info.file_handle.write(&part.content).await;
                                                                        remaining.retain(|c|*c!=part.chunk_index);
                                                                        tx.send(
                                                                            iter::once(sendfile_messages::MessageType::AckFilePart.to_id()).chain(sendfile_messages::AckFilePart{chunk_index: remaining.first().cloned(), file_name: part.file_name.clone()}.encode_to_vec()).collect::<Vec<u8>>().into()
                                                                        ).await.unwrap();

                                                                        if remaining.len() == 0 {
                                                                            info.transfer_state = TransferState::Finished;

                                                                        }
                                                                    },
                                                                    TransferState::Finished => todo!()
                                                                };
                                                            }
                                                        }
                                                    },
                                                    Err(_) => println!("Failed to parse transfer part")
                                                }
                                            }
                                            MessageType::AckFilePart => println!("Received Ack File Part: {:?}", &b[1..]) // shouldn't occur on receiver side
                                        }
                                    }
                                    Err(e) => println!("Error: {:?}", e)
                                }
                            }
                        }

                        // clean finished file transfers
                        file_transfers.lock().await.retain(|k, v|{
                            let incomplete = !matches!(v.transfer_state, TransferState::Finished);
                            if !incomplete {
                                info!("File {} finished", k);
                            }
                            incomplete
                        });
                        info!("Speed: {}bps", Self::bytes_per_second(Arc::clone(&chunk_history)).await);
                    }
                });
            }
        });
        tcp_listen(server, SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), self.port)).await
    }
}
