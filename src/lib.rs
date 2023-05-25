#![doc = include_str!("../README.md")]
use std::{net::SocketAddr, sync::Arc };
use statistics::StatisticData;
use tokio::sync::Notify;
use ustr::Ustr;
use coarsetime::Instant;

pub use strategies::CheckpointStrategy;
pub use strategies::PartitionStrategy;
pub use strategies::TransformStrategy;
pub use receiver::Receiver;
pub use strategies::PartitionStrategies;
pub use strategies::CheckpointStrategies;
pub use strategies::TransformerStrategies;

mod statistics;
mod dispatcher;
mod sender;
mod receiver;
mod strategies;

/// Main library module
pub mod forwarder;

type DataPacket = (Vec<u8>, (usize,SocketAddr), Instant);
type PartitionDetails = (Option<i32>, Ustr, Ustr);
type Ticket = Arc<Notify>;
type DataTransmitted = Option<StatisticData>;
type ForwarderReturn = Result<(),String>;
type TlsOption = (Option<String>, Option<String>);
