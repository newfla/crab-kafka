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
    fn partition(&self, addr: &SocketAddr) -> PartitionDetails;

    /// To store the partition number for the topic specified with [`crate::forwarder::ForwarderBuilder::kafka_settings`]
    fn set_num_partitions(&mut self, partitions: i32);
}

/// Facilities to distributes messages across topic partions
///
/// Use the enum variants to construct the strategy
#[derive(new)]
pub enum PartitionStrategies {
    /// Let the broker to decide the partition
    None {
        #[new(default)]
        val: Option<PartitionStrategiesInternal>,
    },
    /// Assign a random partition for each packet
    Random {
        #[new(default)]
        val: Option<PartitionStrategiesInternal>,
    },
    /// Packets are distributed over partitions using a round robin schema
    RoundRobin {
        #[new(default)]
        val: Option<PartitionStrategiesInternal>,
    },
    /// Packets coming from the same peer are guaranted to be sent on the same partition.
    ///
    /// Partitions are assigned to peers using a round robin schema.
    StickyRoundRobin {
        #[new(default)]
        val: Option<PartitionStrategiesInternal>,
    },
}

impl PartitionStrategy for PartitionStrategies {
    fn partition(&self, addr: &SocketAddr) -> PartitionDetails {
        match self {
            PartitionStrategies::None { val } => val.as_ref().unwrap().partition(addr),
            PartitionStrategies::Random { val } => val.as_ref().unwrap().partition(addr),
            PartitionStrategies::RoundRobin { val } => val.as_ref().unwrap().partition(addr),
            PartitionStrategies::StickyRoundRobin { val } => val.as_ref().unwrap().partition(addr),
        }
    }

    fn set_num_partitions(&mut self, partitions: i32) {
        match self {
            PartitionStrategies::None { val } => val.insert(PartitionStrategiesInternal::None),
            PartitionStrategies::Random { val } => {
                val.insert(PartitionStrategiesInternal::new_random(partitions))
            }
            PartitionStrategies::RoundRobin { val } => {
                val.insert(PartitionStrategiesInternal::new_round_robin(partitions))
            }
            PartitionStrategies::StickyRoundRobin { val } => val.insert(
                PartitionStrategiesInternal::new_sticky_round_robin(partitions),
            ),
        };
    }
}

#[derive(new)]
pub enum PartitionStrategiesInternal {
    None,
    Random {
        #[new(default)]
        rng: Rng,
        num_partitions: i32,
    },
    RoundRobin {
        #[new(value = "AtomicI32::new(fastrand::i32(0..num_partitions))")]
        start_partition: AtomicI32,
        num_partitions: i32,
    },
    StickyRoundRobin {
        #[new(value = "AtomicI32::new(fastrand::i32(0..num_partitions))")]
        start_partition: AtomicI32,
        num_partitions: i32,
    },
}

impl PartitionStrategy for PartitionStrategiesInternal {
    fn partition(&self, addr: &SocketAddr) -> PartitionDetails {
        match self {
            PartitionStrategiesInternal::None => none_partition(addr),
            PartitionStrategiesInternal::Random {
                rng,
                num_partitions,
            } => random_partition(addr, *num_partitions, rng),
            PartitionStrategiesInternal::RoundRobin {
                start_partition,
                num_partitions,
            } => round_robin_partition(addr, start_partition, *num_partitions),
            PartitionStrategiesInternal::StickyRoundRobin {
                start_partition,
                num_partitions,
            } => sticky_partition(addr, start_partition, *num_partitions),
        }
    }

    fn set_num_partitions(&mut self, _partitions: i32) {}
}

#[cached(key = "SocketAddr", convert = r#"{ *addr }"#, sync_writes = true)]
fn none_partition(addr: &SocketAddr) -> PartitionDetails {
    let key = ustr(&(addr.to_string() + "|auto"));
    (None, key, key)
}

fn random_partition(addr: &SocketAddr, num_partitions: i32, rng: &Rng) -> PartitionDetails {
    let next = rng.i32(0..num_partitions);
    let addr_str = addr.to_string();
    let key = ustr(&(addr_str.clone() + "|" + &next.to_string()));
    let order_key = ustr(&addr_str);

    (Some(next), key, order_key)
}

fn round_robin_partition(
    addr: &SocketAddr,
    start_partition: &AtomicI32,
    num_partitions: i32,
) -> PartitionDetails {
    let next = start_partition.fetch_add(1, Ordering::SeqCst) % num_partitions;

    debug!("SockAddr: {} partition: {}", addr, next);

    let addr_str = addr.to_string();
    let key = ustr(&(addr_str.clone() + "|" + &next.to_string()));
    let order_key = ustr(&addr_str);

    (Some(next), key, order_key)
}

#[cached(key = "SocketAddr", convert = r#"{ *addr }"#, sync_writes = true)]
fn sticky_partition(
    addr: &SocketAddr,
    start_partition: &AtomicI32,
    num_partitions: i32,
) -> PartitionDetails {
    let next = start_partition.fetch_add(1, Ordering::SeqCst) % num_partitions;

    let key = ustr(&(addr.to_string() + "|" + &next.to_string()));
    let val = (Some(next), key, key);

    debug!("SockAddr: {} partition: {}", addr, next);

    val
}

/// Hook to setup per packet forward policy
pub trait CheckpointStrategy {
    /// Prevents packets to be forwarded in kafka
    /// data is composed as: (payload, (#valid bytes in payload, source address), recv_time)
    fn check(&self, data: (&DataPacket, &Option<i32>)) -> bool;
}

///Facilities to control the packets inflow to Kafka
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
    fn transform(&self, addr: &SocketAddr, payload: &[u8], partition: &Option<i32>) -> Vec<u8>;
}

/// Facilities to alter the network payload before sending it to Kafka
#[derive(Clone)]
pub enum TransformStrategies {
    /// Return payload without modification as [`std::vec::Vec<u8>`]
    NoTransform,
}

impl TransformStrategy for TransformStrategies {
    fn transform(&self, _addr: &SocketAddr, payload: &[u8], _partition: &Option<i32>) -> Vec<u8> {
        match self {
            TransformStrategies::NoTransform => payload.to_vec(),
        }
    }
}
