use std::{net::SocketAddr, future::{IntoFuture, Future}, pin::Pin, path::Path, fs};

use branches::unlikely;
use coarsetime::Instant;
use derive_builder::Builder;
use derive_new::new;
use kanal::AsyncSender;
use log::{error, debug, info};
use tokio::{net::{UdpSocket, TcpListener, TcpStream}, select, spawn, io::{AsyncReadExt, AsyncRead}};
use tokio_native_tls::native_tls::Identity;
use tokio_util::sync::CancellationToken;
use tokio_dtls_stream_sink::{Server, Session};
use openssl::ssl::{SslContext, SslFiletype, SslMethod};

use crate::{DataPacket, TlsOption};

#[derive(new)]
pub enum Receiver {
    UdpFramed  {ip: String, port: String, buffer_size: usize},
    TcpStream  {ip: String, port: String, buffer_size: usize},
    DtlsFramed {ip: String, port: String, buffer_size: usize, security: TlsOption},
    TlsStream  {ip: String, port: String, buffer_size: usize, security: TlsOption}
}

fn build_socket(ip: String, port:String) -> SocketAddr {
    (ip + ":" +&port).parse().unwrap()
}

fn build_udp_framed(ip: String, port:String, buffer_size: usize) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::UDP)
        .addr(build_socket(ip, port))
        .buffer_size(buffer_size)
}

fn build_tcp_stream(ip: String, port:String, buffer_size: usize) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::TCP)
        .addr(build_socket(ip, port))
        .buffer_size(buffer_size)
}

fn build_dtls_framed(ip: String, port:String, buffer_size: usize, security: TlsOption) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::UDP)
        .addr(build_socket(ip, port))
        .buffer_size(buffer_size)
        .tls_settings(security)
}

fn build_tls_stream(ip: String, port:String, buffer_size: usize, security: TlsOption) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::TCP)
        .addr(build_socket(ip, port))
        .buffer_size(buffer_size)
        .tls_settings(security)
}

impl From<Receiver> for ReceiverTaskBuilder {
    fn from(val: Receiver) -> Self {
        match val {
            Receiver::UdpFramed  {ip, port, buffer_size} => build_udp_framed(ip, port, buffer_size),
            Receiver::DtlsFramed {ip, port, security, buffer_size} => build_dtls_framed(ip, port, buffer_size, security),
            Receiver::TcpStream  {ip, port, buffer_size} => build_tcp_stream(ip, port, buffer_size),
            Receiver::TlsStream  {ip, port, buffer_size, security} => build_tls_stream(ip, port, buffer_size, security)    
        }
    }
}

pub enum ReceiverType {
    UDP,
    TCP
}

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct ReceiverTask {
    addr: SocketAddr,
    receiver_type: ReceiverType, 
    buffer_size: usize,
    dispatcher_sender: AsyncSender<DataPacket>,
    shutdown_token: CancellationToken,
    #[builder(setter(into, strip_option), default)]
    tls_settings: Option<TlsOption>
}

impl ReceiverTask {
    fn error_run(&self) {
        error!("Key AND/OR Certificate for TLS were not provided");
        let (certificate, key) = self.tls_settings.as_ref().unwrap();
        debug!("Certificate: {:?}", certificate);
        debug!("Key: {:?}", key);
        self.shutdown_token.cancel();
    }

    async fn udp_run(&self, socket: UdpSocket) {
        //Handle incoming UDP packets 
        //We don't need to check shutdown_token.cancelled() using select!. Infact, dispatcher_sender.send().is_err() <=> shutdown_token.cancelled() 
        loop {
            let mut buf  = Vec::with_capacity(self.buffer_size);
            match socket.recv_buf_from(&mut buf).await {
                Err(err) => {
                    error!("Socket recv failed. Reason: {}", err);
                    self.shutdown_token.cancel();
                    break;
                },
                Ok(data) => {
                    if unlikely(self.dispatcher_sender.send((buf,data,Instant::now())).await.is_err()) {
                        error!("Failed to send data to dispatcher");
                        info!("Shutting down receiver task");
                        self.shutdown_token.cancel();
                        break;
                    }
                }
            }
        }
    }

    async fn dtls_run(&self, socket: UdpSocket, cert: String, key: String){
        let mut server = Server::new(socket);
        match Self::build_openssl_context(cert,key) {
            Err(_) => {
                info!("Shutting down receiver task");
                self.shutdown_token.cancel();
            },

            Ok(ctx) => {
                loop {
                    select! {
                        _ = self.shutdown_token.cancelled() => { 
                            info!("Shutting down receiver task");
                            break;
                        }
                        //Accept new DTLS connections (handshake phase)
                        session = server.accept(Some(&ctx)) => if let Ok(session) = session {
                            self.handle_dtls_session(session)
                        }
                    }
                }
            }
        } 
    }

    fn handle_dtls_session(&self, mut session: Session) {
        let dispatcher_sender= self.dispatcher_sender.clone();
        let shutdown_token= self.shutdown_token.clone();

        let mut buf = vec![0u8; self.buffer_size];
        let peer = session.peer();
        spawn(async move {
            //Handle incoming UDP packets for each peer
            //session.read.is_err() => closed connection
            //We don't need to check shutdown_token.cancelled() using select!. Infact dispatcher_sender.send().is_err() => shutdown_token.cancelled() 
            loop {
                select! {
                    _ = shutdown_token.cancelled() => break,
                    res = session.read(&mut buf) => match res {
                        Err(err) => {
                            error!("Peer connection closed. Reason: {}", err);
                            break;
                        },
                        Ok(len) => {
                            if unlikely(dispatcher_sender.send((buf.clone(),(len,peer),Instant::now())).await.is_err()) {
                                error!("Failed to send data to dispatcher");
                                shutdown_token.cancel();
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    async fn tcp_stream_run (&self, socket: TcpListener) {
        loop {
            select! {
                _ = self.shutdown_token.cancelled() => { 
                    info!("Shutting down receiver task");
                    break;
                }
                //Accept new TCP connections
                session = socket.accept() => if let Ok((stream,peer)) = session {
                    debug!("Peer {} connected", peer);
                    self.handle_tcp_session(Box::new(stream),peer);
                }
            }
        }
    }

    fn handle_tcp_session(&self, stream: Box<dyn AsyncRead + Unpin + Send>, peer: SocketAddr) {
        let dispatcher_sender = self.dispatcher_sender.clone();
        let shutdown_token = self.shutdown_token.clone();
        let buffer_size = self.buffer_size;

        spawn(async move {
            Self::common_handle_tcp_session(stream, peer, buffer_size, dispatcher_sender, shutdown_token).await;
        });
    }

    async fn common_handle_tcp_session<S>(mut stream: S, peer: SocketAddr, buffer_size: usize, dispatcher_sender: AsyncSender<DataPacket>,
        shutdown_token: CancellationToken) 
    where 
        S: AsyncRead + Unpin + Send + 'static {

            let mut buf = vec![0;buffer_size];
                //Handle incoming TCP packets for each peer
                //session.read.is_err() => closed connection
                //We don't need to check shutdown_token.cancelled() using select!. Infact dispatcher_sender.send().is_err() => shutdown_token.cancelled() 
            loop {
                select! {
                    _ = shutdown_token.cancelled() => break,
                    res = stream.read(&mut buf) => match res {
                        Err(err) => {
                            error!("Peer connection closed. Reason: {}", err);
                            break;
                        },
                        Ok(len) => match len {
                            //Probabily the connection was closed by the peer. Dropping the stream
                            0 => return,
                            _ => {
                                if unlikely(dispatcher_sender.send((buf.clone(),(len,peer),Instant::now())).await.is_err()) {
                                    error!("Failed to send data to dispatcher");
                                    shutdown_token.cancel();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
     
    }

    // async fn tls_stream_run(&self, socket: TcpListener, cert: String, key: String) {
    //     let certs = Self::load_certs(Path::new(&cert));
    //     let keys = Self::load_keys(Path::new(&key));

    //     if certs.is_err() || keys.is_err() {
    //         error!("Tls configuraion failed certs {}, keys {}", certs.is_err(), keys.is_err());
    //         self.shutdown_token.cancel();
    //         return ;
    //     }

    //     debug!("certs: {:?} {:?}",certs, keys);

    //     let config = ServerConfig::builder()
    //         .with_safe_defaults()
    //         .with_no_client_auth()
    //         .with_single_cert(certs.unwrap(), keys.unwrap().remove(0));

    //     if config.is_err() {
    //         error!("Tls configuraion failed");
    //         self.shutdown_token.cancel();
    //         return ;
    //     }

    //     let acceptor = TlsAcceptor::from(Arc::new(config.unwrap()));

    //     loop {
    //         select! {
    //             _ = self.shutdown_token.cancelled() => { 
    //                 info!("Shutting down receiver task");
    //                 break;
    //             }
    //             //Accept new TLS connections
    //             session = socket.accept() => if let Ok((session,peer)) = session {
    //                 let acceptor = acceptor.clone();
    //                 self.handle_tls_session(session,peer,acceptor);
    //             }
    //         }
    //     }


    // }

    async fn tls_stream_run(&self, socket: TcpListener, cert: String, key: String) {

        let cert = fs::read(Path::new(&cert));
        let key = fs::read(Path::new(&key));
        if cert.is_err() || key.is_err() {
            error!("Tls configuraion failed certs {}, keys {}", cert.is_err(), key.is_err());
            self.shutdown_token.cancel();
            return ;
        }
        let identity = Identity::from_pkcs8(&cert.unwrap(), &key.unwrap());

        if identity.is_err() {
            error!("Tls Identity fails");
            self.shutdown_token.cancel();
            return ;
        }

        let acceptor = tokio_native_tls::native_tls::TlsAcceptor::new(identity.unwrap());
        if acceptor.is_err() {
            error!("Tls acceptor fails");
            self.shutdown_token.cancel();
            return ;
        }
        let acceptor = acceptor.unwrap();

        loop {
            select! {
                _ = self.shutdown_token.cancelled() => { 
                    info!("Shutting down receiver task");
                    break;
                }
                //Accept new TLS connections
                session = socket.accept() => if let Ok((session,peer)) = session {
                    let acceptor = acceptor.clone();
                    self.handle_tls_session(session,peer,acceptor.into());
                }
            }
        }


    }

    fn handle_tls_session(&self, stream: TcpStream, peer: SocketAddr, acceptor: tokio_native_tls::TlsAcceptor) {
        let dispatcher_sender = self.dispatcher_sender.clone();
        let shutdown_token = self.shutdown_token.clone();
        let buffer_size = self.buffer_size;

        spawn(async move {
            //Handle incoming TLS packets for each peer
            //session.read.is_err() => closed connection
            //We don't need to check shutdown_token.cancelled() using select!. Infact dispatcher_sender.send().is_err() => shutdown_token.cancelled() 
            match acceptor.accept(stream).await {
                Err(err) => {
                    error!("Handshake failed. reason {}",err)
                }
                Ok(stream) => {
                    Self::common_handle_tcp_session(stream, peer, buffer_size, dispatcher_sender, shutdown_token).await;
                }
                
            }
        });
    }

    fn build_openssl_context(cert: String, key: String) -> Result<SslContext,()> {
        let mut ctx = SslContext::builder(SslMethod::dtls()).unwrap();

        let setup_context = ctx.set_private_key_file(key, SslFiletype::PEM)
            .and_then(|_| ctx.set_certificate_chain_file(cert))
            .and_then(|_| ctx.check_private_key());
        setup_context.map_err(|err| {
                error!("{}",err);
                ()
            })
            .map(|_| ctx.build())
    }
    
    async fn run(self) {
        match &self.receiver_type {
            ReceiverType::UDP => {
                //Socket binding handling 
                let socket = UdpSocket::bind(self.addr).await;
                if let Err(err) = socket {
                    error!("Socket binding failed. Reaseon: {}", err);
                    self.shutdown_token.cancel();
                    return;
                }

                let socket = socket.unwrap();
                match &self.tls_settings {
                    None => self.udp_run(socket).await,
                    Some((Some(cert), Some(key))) => self.dtls_run(socket, cert.to_owned(), key.to_owned()).await,
                    Some((_,_)) => self.error_run(),
                }
            },
            ReceiverType::TCP => {
                //Socket binding handling 
                let socket = TcpListener::bind(self.addr).await;
                if let Err(err) = socket {
                    error!("Socket binding failed. Reaseon: {}", err);
                    self.shutdown_token.cancel();
                    return;
                }

                let socket = socket.unwrap();
                match &self.tls_settings {
                    None => self.tcp_stream_run(socket).await,
                    Some((Some(cert), Some(key))) => self.tls_stream_run(socket, cert.to_owned(), key.to_owned()).await,
                    Some((_,_)) => self.error_run(),
                }
            }
        }
       
    }
    
}

impl IntoFuture for ReceiverTask {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}
