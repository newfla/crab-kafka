use std::{
    collections::HashSet,
    fs,
    future::{Future, IntoFuture},
    net::SocketAddr,
    path::Path,
    pin::Pin,
};

use anyhow::Result;
use branches::unlikely;
use coarsetime::Instant;
use derive_builder::Builder;
use derive_new::new;
use kanal::AsyncSender;
use log::{debug, error, info};
use openssl::ssl::{SslContext, SslFiletype, SslMethod};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::{TcpListener, TcpStream, UdpSocket},
    select, spawn,
};
use tokio_dtls_stream_sink::{Server, Session};
use tokio_native_tls::native_tls::Identity;
use tokio_util::sync::CancellationToken;

use crate::{DataPacket, TlsOption};

/// Facilities to receive data from sockets
#[derive(new)]
pub enum Receiver {
    /// Read from socket by [`UdpSocket`], using a buffer with length [`crate::Receiver::UdpFramed::buffer_size`]
    UdpFramed {
        ip: String,
        port: String,
        buffer_size: usize,
    },

    /// Read from socket by [`UdpSocket`], using a buffer with length [`crate::Receiver::UdpFramed::buffer_size`] and connecting socket to peer
    UdpConnected {
        ip: String,
        port: String,
        buffer_size: usize,
    },

    /// Read from socket by [`TcpStream`], using a buffer with length [`crate::Receiver::TcpStream::buffer_size`]
    TcpStream {
        ip: String,
        port: String,
        buffer_size: usize,
    },

    /// Read from socket by [`Session`], using a buffer with length [`crate::Receiver::DtlsStream::buffer_size`]
    ///
    /// [`crate::Receiver::DtlsStream::security`] is interpreted as (path to server certificate as PCKS8 file, path to the private key as PCKS8 file)
    DtlsStream {
        ip: String,
        port: String,
        buffer_size: usize,
        security: TlsOption,
    },

    /// Read from socket by [`tokio_native_tls::TlsStream`], using a buffer with length [`crate::Receiver::TlsStream::buffer_size`]
    ///
    /// [`crate::Receiver::TlsStream::security`] is interpreted as (path to server certificate as PCKS8 file, path to the private key as PCKS8 file)
    TlsStream {
        ip: String,
        port: String,
        buffer_size: usize,
        security: TlsOption,
    },
}

fn build_socket_addr(ip: String, port: String) -> SocketAddr {
    (ip + ":" + &port).parse().unwrap()
}

fn build_udp_framed(ip: String, port: String, buffer_size: usize) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::Udp)
        .addr(build_socket_addr(ip, port))
        .buffer_size(buffer_size)
}

fn build_udp_connected(ip: String, port: String, buffer_size: usize) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::UdpConnected)
        .addr(build_socket_addr(ip, port))
        .buffer_size(buffer_size)
}

fn build_tcp_stream(ip: String, port: String, buffer_size: usize) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::Tcp)
        .addr(build_socket_addr(ip, port))
        .buffer_size(buffer_size)
}

fn build_dtls_framed(
    ip: String,
    port: String,
    buffer_size: usize,
    security: TlsOption,
) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::Udp)
        .addr(build_socket_addr(ip, port))
        .buffer_size(buffer_size)
        .tls_settings(security)
}

fn build_tls_stream(
    ip: String,
    port: String,
    buffer_size: usize,
    security: TlsOption,
) -> ReceiverTaskBuilder {
    ReceiverTaskBuilder::default()
        .receiver_type(ReceiverType::Tcp)
        .addr(build_socket_addr(ip, port))
        .buffer_size(buffer_size)
        .tls_settings(security)
}

impl From<Receiver> for ReceiverTaskBuilder {
    fn from(val: Receiver) -> Self {
        match val {
            Receiver::UdpFramed {
                ip,
                port,
                buffer_size,
            } => build_udp_framed(ip, port, buffer_size),
            Receiver::UdpConnected {
                ip,
                port,
                buffer_size,
            } => build_udp_connected(ip, port, buffer_size),
            Receiver::DtlsStream {
                ip,
                port,
                security,
                buffer_size,
            } => build_dtls_framed(ip, port, buffer_size, security),
            Receiver::TcpStream {
                ip,
                port,
                buffer_size,
            } => build_tcp_stream(ip, port, buffer_size),
            Receiver::TlsStream {
                ip,
                port,
                buffer_size,
                security,
            } => build_tls_stream(ip, port, buffer_size, security),
        }
    }
}

pub enum ReceiverType {
    Udp,
    UdpConnected,
    Tcp,
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
    tls_settings: Option<TlsOption>,
}

impl ReceiverTask {
    fn error_run(&self) {
        error!("Key AND/OR Certificate for TLS were not provided");
        let (certificate, key) = self.tls_settings.as_ref().unwrap();
        debug!("Certificate: {:?}", certificate);
        debug!("Key: {:?}", key);
        self.shutdown_token.cancel();
    }

    #[inline]
    async fn send_to_dispatcher(
        buf: &[u8],
        len: usize,
        addr: SocketAddr,
        shutdown_token: &CancellationToken,
        dispatcher_sender: &AsyncSender<DataPacket>,
    ) {
        unsafe {
            if unlikely(
                dispatcher_sender
                    .send((buf.get_unchecked(..len).to_vec(), addr, Instant::now()))
                    .await
                    .is_err(),
            ) {
                error!("Failed to send data to dispatcher");
                info!("Shutting down receiver task");
                shutdown_token.cancel();
            }
        }
    }

    async fn udp_run(&self, socket: UdpSocket) {
        //Handle incoming UDP packets
        //We don't need to check shutdown_token.cancelled() using select!. In fact, dispatcher_sender.send().is_err() <=> shutdown_token.cancelled()
        let mut buf = vec![0u8; self.buffer_size];

        loop {
            match socket.recv_from(&mut buf).await {
                Err(err) => {
                    error!("Socket recv failed. Reason: {}", err);
                    self.shutdown_token.cancel();
                    break;
                }
                Ok((buf_len, addr)) => {
                    Self::send_to_dispatcher(
                        &buf,
                        buf_len,
                        addr,
                        &self.shutdown_token,
                        &self.dispatcher_sender,
                    )
                    .await;
                }
            }
        }
    }

    async fn udp_connected_run(&self, socket: UdpSocket) {
        //Handle incoming UDP packets
        //We don't need to check shutdown_token.cancelled() using select!. In fact, dispatcher_sender.send().is_err() <=> shutdown_token.cancelled()
        let mut buf = vec![0u8; self.buffer_size];
        let mut map = HashSet::new();
        loop {
            match socket.recv_from(&mut buf).await {
                Err(err) => {
                    error!("Socket recv failed. Reason: {}", err);
                    self.shutdown_token.cancel();
                    break;
                }
                Ok((buf_len, addr)) => {
                    if !map.contains(&addr) {
                        let shutdown_token = self.shutdown_token.clone();
                        let dispatcher_sender = self.dispatcher_sender.clone();
                        let buffer_size = self.buffer_size;
                        let socket = Self::build_udp_socket_reuse_addr_port(&self.addr).unwrap();
                        socket.connect(addr).await.unwrap();
                        map.insert(addr);
                        spawn(async move {
                            let mut buf = vec![0u8; buffer_size];
                            loop {
                                match socket.recv(&mut buf).await {
                                    Err(err) => {
                                        error!("Socket recv failed. Reason: {}", err);
                                        shutdown_token.cancel();
                                        break;
                                    }
                                    Ok(buf_len) => {
                                        Self::send_to_dispatcher(
                                            &buf,
                                            buf_len,
                                            addr,
                                            &shutdown_token,
                                            &dispatcher_sender,
                                        )
                                        .await;
                                    }
                                }
                            }
                        });
                    }

                    Self::send_to_dispatcher(
                        &buf,
                        buf_len,
                        addr,
                        &self.shutdown_token,
                        &self.dispatcher_sender,
                    )
                    .await;
                }
            }
        }
    }

    async fn dtls_run(&self, socket: UdpSocket, cert: String, key: String) {
        let mut server = Server::new(socket);
        match Self::build_openssl_context(cert, key) {
            Err(_) => {
                info!("Shutting down receiver task");
                self.shutdown_token.cancel();
            }

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
        let dispatcher_sender = self.dispatcher_sender.clone();
        let shutdown_token = self.shutdown_token.clone();
        let buffer_size = self.buffer_size;

        spawn(async move {
            let mut buf = vec![0u8; buffer_size];
            //Handle incoming UDP packets for each peer
            //session.read.is_err() => closed connection
            //We don't need to check shutdown_token.cancelled() using select!. In fact dispatcher_sender.send().is_err() => shutdown_token.cancelled()
            loop {
                select! {
                    _ = shutdown_token.cancelled() => break,
                    res = session.read(&mut buf) => match res {
                        Err(err) => {
                            error!("Peer connection closed. Reason: {}", err);
                            break;
                        },
                        Ok(len) => {
                            Self::send_to_dispatcher(&buf, len, session.peer(), &shutdown_token, &dispatcher_sender).await;
                        }
                    }
                }
            }
        });
    }

    async fn tcp_stream_run(&self, socket: TcpListener) {
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
            Self::common_handle_tcp_session(
                stream,
                peer,
                buffer_size,
                dispatcher_sender,
                shutdown_token,
            )
            .await;
        });
    }

    async fn common_handle_tcp_session<S>(
        mut stream: S,
        peer: SocketAddr,
        buffer_size: usize,
        dispatcher_sender: AsyncSender<DataPacket>,
        shutdown_token: CancellationToken,
    ) where
        S: AsyncRead + Unpin + Send + 'static,
    {
        let mut buf = vec![0; buffer_size];
        //Handle incoming TCP packets for each peer
        //session.read.is_err() => closed connection
        //We don't need to check shutdown_token.cancelled() using select!. In fact dispatcher_sender.send().is_err() => shutdown_token.cancelled()
        loop {
            select! {
                _ = shutdown_token.cancelled() => break,
                res = stream.read(&mut buf) => match res {
                    Err(err) => {
                        error!("Peer connection closed. Reason: {}", err);
                        break;
                    },
                    Ok(len) => match len {
                        //Probably the connection was closed by the peer. Dropping the stream
                        0 => return,
                        _ => {
                            Self::send_to_dispatcher(&buf, len, peer, &shutdown_token, &dispatcher_sender).await;
                        }
                    }
                }
            }
        }
    }

    async fn tls_stream_run(&self, socket: TcpListener, cert: String, key: String) {
        let cert = fs::read(Path::new(&cert));
        let key = fs::read(Path::new(&key));
        if cert.is_err() || key.is_err() {
            error!(
                "Tls configuration failed certs {}, keys {}",
                cert.is_err(),
                key.is_err()
            );
            self.shutdown_token.cancel();
            return;
        }
        let identity = Identity::from_pkcs8(&cert.unwrap(), &key.unwrap());

        if identity.is_err() {
            error!("Tls Identity fails");
            self.shutdown_token.cancel();
            return;
        }

        let acceptor = tokio_native_tls::native_tls::TlsAcceptor::new(identity.unwrap());
        if acceptor.is_err() {
            error!("Tls acceptor fails");
            self.shutdown_token.cancel();
            return;
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

    fn handle_tls_session(
        &self,
        stream: TcpStream,
        peer: SocketAddr,
        acceptor: tokio_native_tls::TlsAcceptor,
    ) {
        let dispatcher_sender = self.dispatcher_sender.clone();
        let shutdown_token = self.shutdown_token.clone();
        let buffer_size = self.buffer_size;

        spawn(async move {
            //Handle incoming TLS packets for each peer
            //session.read.is_err() => closed connection
            //We don't need to check shutdown_token.cancelled() using select!. In fact dispatcher_sender.send().is_err() => shutdown_token.cancelled()
            match acceptor.accept(stream).await {
                Err(err) => {
                    error!("Handshake failed. reason {}", err);
                }
                Ok(stream) => {
                    Self::common_handle_tcp_session(
                        stream,
                        peer,
                        buffer_size,
                        dispatcher_sender,
                        shutdown_token,
                    )
                    .await;
                }
            }
        });
    }

    fn build_openssl_context(cert: String, key: String) -> Result<SslContext> {
        let mut ctx_builder = SslContext::builder(SslMethod::dtls())?;

        ctx_builder.set_private_key_file(key, SslFiletype::PEM)?;
        ctx_builder.set_certificate_chain_file(cert)?;
        ctx_builder.check_private_key()?;
        Ok(ctx_builder.build())
    }

    fn build_udp_socket_reuse_addr_port(addr: &SocketAddr) -> Result<UdpSocket> {
        let address = (*addr).into();

        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&address)?;
        let std_sock: std::net::UdpSocket = socket.into();
        std_sock.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_sock)?;
        Ok(socket)
    }

    async fn run(self) {
        match &self.receiver_type {
            ReceiverType::Udp => {
                //Socket binding handling
                let socket = UdpSocket::bind(self.addr).await;
                if let Err(err) = socket {
                    error!("Socket binding failed. Reason: {}", err);
                    self.shutdown_token.cancel();
                    return;
                }

                let socket = socket.unwrap();
                match &self.tls_settings {
                    None => self.udp_run(socket).await,
                    Some((Some(cert), Some(key))) => {
                        self.dtls_run(socket, cert.to_owned(), key.to_owned()).await
                    }
                    Some((_, _)) => self.error_run(),
                }
            }
            ReceiverType::UdpConnected => {
                //Socket binding handling
                let socket = Self::build_udp_socket_reuse_addr_port(&self.addr);
                //let socket = UdpSocket::bind(self.addr).await;
                if socket.is_err() {
                    error!("Socket binding failed.");
                    self.shutdown_token.cancel();
                    return;
                }

                let socket = socket.unwrap();
                self.udp_connected_run(socket).await;
            }
            ReceiverType::Tcp => {
                //Socket binding handling
                let socket = TcpListener::bind(self.addr).await;
                if let Err(err) = socket {
                    error!("Socket binding failed. Reason: {}", err);
                    self.shutdown_token.cancel();
                    return;
                }

                let socket = socket.unwrap();
                match &self.tls_settings {
                    None => self.tcp_stream_run(socket).await,
                    Some((Some(cert), Some(key))) => {
                        self.tls_stream_run(socket, cert.to_owned(), key.to_owned())
                            .await
                    }
                    Some((_, _)) => self.error_run(),
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
