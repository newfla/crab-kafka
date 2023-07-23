#![doc = include_str!("../README.md")]
use coarsetime::Instant;
use statistics::StatisticData;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Notify;
use ustr::Ustr;

pub use receiver::Receiver;
pub use strategies::CheckpointStrategies;
pub use strategies::CheckpointStrategy;
pub use strategies::PartitionStrategies;
pub use strategies::PartitionStrategy;
pub use strategies::TransformStrategies;
pub use strategies::TransformStrategy;

mod dispatcher;
mod receiver;
mod sender;
mod statistics;
mod strategies;

/// Main library module
pub mod forwarder;

type DataPacket = (Vec<u8>, SocketAddr, Instant);
type PartitionDetails = (Option<i32>, Ustr, Ustr);
type Ticket = Arc<Notify>;
type DataTransmitted = Option<StatisticData>;
type ForwarderReturn = Result<(), String>;
type TlsOption = (Option<String>, Option<String>);
