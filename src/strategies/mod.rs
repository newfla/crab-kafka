use std::{
    net::SocketAddr,
    sync::atomic::{AtomicI32, Ordering},
};

use cached::proc_macro::cached;
use derive_new::new;
use fastrand::Rng;
use log::debug;
use ustr::ustr;

use crate::{DataPacket, PartitionDetails};

/// Hook to setup per packet kafka partition policy
pub trait PartitionStrategy {
    /// The returned value is interpreted as: ([`rdkafka::producer::future_producer::FutureRecord::partition`], [`rdkafka::producer::future_producer::FutureRecord::key`], order_key).
    ///
    ///  order_key can guarantee that packets from the same peer are sent to kafka respecting the reception order
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails;

    /// To store the partition number for the topic specified with [`crate::forwarder::ForwarderBuilder::kafka_settings`]
    fn set_num_partitions(&mut self, partitions: i32);
}

/// Facilities to distributes messages across topic partitions
///
/// Use the enum variants to construct the strategy
#[non_exhaustive]
#[derive(new)]
pub enum PartitionStrategies {
    /// Let the broker to decide the partition
    None,
    /// Assign a random partition for each packet
    Random {
        #[new(default)]
        rng: Rng,
        #[new(default)]
        num_partitions: i32,
    },
    /// Packets are distributed over partitions using a round robin schema
    RoundRobin {
        #[new(default)]
        start_partition: AtomicI32,
        #[new(default)]
        num_partitions: i32,
    },
    /// Packets coming from the same peer are guaranteed to be sent on the same partition.
    ///
    /// Partitions are assigned to peers using a round robin schema.
    StickyRoundRobin {
        #[new(default)]
        start_partition: AtomicI32,
        #[new(default)]
        num_partitions: i32,
    },
}

impl PartitionStrategy for PartitionStrategies {
    fn partition(&mut self, addr: &SocketAddr) -> PartitionDetails {
        match self {
            PartitionStrategies::None => none_partition(addr),
            PartitionStrategies::Random {
                rng,
                num_partitions,
            } => random_partition(addr, *num_partitions, rng),
            PartitionStrategies::RoundRobin {
                start_partition,
                num_partitions,
            } => round_robin_partition(addr, start_partition, *num_partitions),
            PartitionStrategies::StickyRoundRobin {
                start_partition,
                num_partitions,
            } => sticky_partition(addr, start_partition, *num_partitions),
        }
    }

    fn set_num_partitions(&mut self, partitions: i32) {
        match self {
            PartitionStrategies::None => (),
            PartitionStrategies::Random {
                rng: _,
                num_partitions,
            } => *num_partitions = partitions,

            PartitionStrategies::RoundRobin {
                start_partition,
                num_partitions,
            }
            | PartitionStrategies::StickyRoundRobin {
                start_partition,
                num_partitions,
            } => {
                *start_partition = AtomicI32::new(fastrand::i32(0..partitions));
                *num_partitions = partitions;
            }
        };
    }
}

#[cached(key = "SocketAddr", convert = r#"{ *addr }"#, sync_writes = true)]
fn none_partition(addr: &SocketAddr) -> PartitionDetails {
    let key = ustr(&(addr.to_string() + "|auto"));
    (None, key, key)
}

fn random_partition(addr: &SocketAddr, num_partitions: i32, rng: &mut Rng) -> PartitionDetails {
    let next = rng.i32(0..num_partitions);
    let addr_str = addr.to_string();
    let order_key = ustr(&addr_str);
    let key = ustr(&(addr_str + "|" + &next.to_string()));

    (Some(next), key, order_key)
}

fn round_robin_partition(
    addr: &SocketAddr,
    start_partition: &AtomicI32,
    num_partitions: i32,
) -> PartitionDetails {
    let next = start_partition.fetch_add(1, Ordering::Relaxed) % num_partitions;

    debug!("SockAddr: {} partition: {}", addr, next);

    let addr_str = addr.to_string();
    let order_key = ustr(&addr_str);
    let key = ustr(&(addr_str + "|" + &next.to_string()));

    (Some(next), key, order_key)
}

#[cached(key = "SocketAddr", convert = r#"{ *addr }"#, sync_writes = true)]
fn sticky_partition(
    addr: &SocketAddr,
    start_partition: &AtomicI32,
    num_partitions: i32,
) -> PartitionDetails {
    let next = start_partition.fetch_add(1, Ordering::Relaxed) % num_partitions;

    let key = ustr(&(addr.to_string() + "|" + &next.to_string()));
    let val = (Some(next), key, key);

    debug!("SockAddr: {} partition: {}", addr, next);

    val
}

/// Hook to setup per packet forward policy
pub trait CheckpointStrategy {
    /// Prevents packets to be forwarded in kafka
    /// data is composed as: (payload, source address)
    fn check(&self, data: (&DataPacket, &Option<i32>)) -> bool;
}

///Facilities to control the packets inflow to Kafka
#[non_exhaustive]
pub enum CheckpointStrategies {
    /// Forward every packet
    OpenDoors,
    /// Discard every packet
    ClosedDoors,
    /// Leave it to chance
    FlipCoin,
}

impl CheckpointStrategy for CheckpointStrategies {
    fn check(&self, _data: (&DataPacket, &Option<i32>)) -> bool {
        match self {
            CheckpointStrategies::OpenDoors => true,
            CheckpointStrategies::ClosedDoors => false,
            CheckpointStrategies::FlipCoin => fastrand::bool(),
        }
    }
}

/// Hook to modify tha packet payload before sending to Kafka
pub trait TransformStrategy {
    /// The returned value is used as payload for [`rdkafka::producer::future_producer::FutureRecord`]
    fn transform(&self, addr: &SocketAddr, payload: Vec<u8>, partition: &Option<i32>) -> Vec<u8>;
}

/// Facilities to alter the network payload before sending it to Kafka
#[non_exhaustive]
#[derive(Clone)]
pub enum TransformStrategies {
    /// Return payload without modification as [`Vec<u8>`]
    NoTransform,
}

impl TransformStrategy for TransformStrategies {
    fn transform(&self, _addr: &SocketAddr, payload: Vec<u8>, _partition: &Option<i32>) -> Vec<u8> {
        match self {
            &TransformStrategies::NoTransform => payload,
        }
    }
}
