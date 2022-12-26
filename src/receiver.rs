use aggligator::cfg::Cfg;
use aggligator::connect::Server;
use aggligator_util::net::adv::{tcp_link_filter, tcp_listen};
use core::marker::{Send, Sync};
use core::option::Option::{None, Some};
use core::result::Result;
use core::result::Result::{Err, Ok};
use std::collections::HashMap;
use std::{fs, io};
use std::future::IntoFuture;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use debug_ignore::DebugIgnore;
use futures_util::{SinkExt, StreamExt};
use log::info;
use prost::DecodeError;
use tokio_util::codec::Framed;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use prost::Message;
use crate::{MessageType, sendfile_messages, TransferInfo};

#[derive(Debug, Clone)]
pub struct FileReceiver {
    destination_path: PathBuf,
    port: u16,
    connection_verifier: DebugIgnore<Arc<dyn Fn() -> bool + Send + Sync + 'static>>
}

impl FileReceiver {
    pub fn new<F>(path: &Path, port: u16, connection_verifier: F) -> Self
        where F: Fn() -> bool + Send + Sync + 'static {
        FileReceiver{
            destination_path: path.to_path_buf(),
            port,
            connection_verifier: debug_ignore::DebugIgnore(Arc::new(connection_verifier))
        }
    }

    pub async fn serve(self, root: PathBuf) -> io::Result<()> {
        fs::create_dir_all(&root);
        let server = Server::new(Cfg::default());
        let mut listener = server.listen().await.unwrap();
        tokio::spawn(async move {
            loop {
                let mut inc = listener.next().await.unwrap();
                info!("Received connection {:?}", inc.remote_server_id());
                if !(self.connection_verifier)() {
                    inc.refuse().await;
                    info!("Connection refused");
                    continue
                }
                let (mut task, ch, mut control) = inc.accept().await;
                let root = root.clone();
                task.set_link_filter(tcp_link_filter);

                tokio::spawn(task.into_future());
                tokio::spawn(async move {
                    let mut framed_channel = Framed::new(ch.into_stream(), LengthDelimitedCodec::default());
                    let (mut tx, mut rx) = framed_channel.split();
                    let mut transfers = HashMap::new();
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
                                                                transfers.insert(info.file_name.clone(), handle);
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
                                                        let file_info = transfers.get_mut(&part.file_name);

                                                        match file_info {
                                                            None => println!("Received file part for uninitiated file"),
                                                            Some(info) => {}
                                                        }
                                                    },
                                                    Err(_) => println!("Failed to parse transfer part")
                                                }
                                                println!("Filepart: {:?}", &b[1..])
                                            }
                                        }
                                    }
                                    Err(e) => println!("Error: {:?}", e)
                                }
                            }
                        }
                    }
                });
            }
        });
        tcp_listen(server, SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), self.port)).await;
        Ok(())
    }
}
