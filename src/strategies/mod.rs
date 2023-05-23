use std::{net::SocketAddr, sync::atomic::{AtomicI32, Ordering}};

use cached::proc_macro::cached;
use derive_new::new;
use fastrand::Rng;
use log::debug;
use ustr::ustr;

use crate::{PartitionDetails, DataPacket};

pub trait PartitionStrategy {
    fn partition(&self, addr: &SocketAddr) -> PartitionDetails;
    fn set_num_partitions(&mut self, partitions: i32);
}

pub enum CheckpointStrategies {
    OpenDoors,
    ClosedDoors,
    FlipCoin,
}

#[derive(new)]
pub enum PartitionStrategies {
    None {#[new(default)] val: Option<PartitionStrategiesInternal>},
    Random {#[new(default)] val: Option<PartitionStrategiesInternal>},
    RoundRobin {#[new(default)] val: Option<PartitionStrategiesInternal>},
    StickyRoundRobin {#[new(default)] val: Option<PartitionStrategiesInternal>},
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
            PartitionStrategies::Random { val } => val.insert(PartitionStrategiesInternal::new_random(partitions)),
            PartitionStrategies::RoundRobin { val } => val.insert(PartitionStrategiesInternal::new_round_robin(partitions)),
            PartitionStrategies::StickyRoundRobin { val } => val.insert(PartitionStrategiesInternal::new_sticky_round_robin(partitions)),
        };
    }
}

#[derive(new)]
pub enum PartitionStrategiesInternal {
    None,
    Random{#[new(default)]  rng: Rng, num_partitions: i32},
    RoundRobin{#[new(value = "AtomicI32::new(fastrand::i32(0..num_partitions))")] start_partition: AtomicI32, num_partitions: i32},
    StickyRoundRobin {#[new(value = "AtomicI32::new(fastrand::i32(0..num_partitions))")] start_partition: AtomicI32, num_partitions: i32}
}

impl PartitionStrategy for PartitionStrategiesInternal {
    fn partition(&self, addr: &SocketAddr) -> PartitionDetails {
        match self {
            PartitionStrategiesInternal::None => none_partition(addr),
            PartitionStrategiesInternal::Random { rng, num_partitions } => random_partition(addr, *num_partitions, rng),
            PartitionStrategiesInternal::RoundRobin { start_partition, num_partitions } => round_robin_partition(addr, start_partition, *num_partitions),
            PartitionStrategiesInternal::StickyRoundRobin { start_partition, num_partitions } => sticky_partition(addr, start_partition, *num_partitions),
        }
    }

    fn set_num_partitions(&mut self, _partitions: i32) {
        
    }
}

#[cached(key = "SocketAddr", convert = r#"{ *addr }"#, sync_writes = true)]
fn none_partition(addr: &SocketAddr) -> PartitionDetails {
    let key = ustr(&(addr.to_string()+"|auto"));
    (None, key,key)
}

fn random_partition(addr: &SocketAddr, num_partitions: i32, rng: &Rng) -> PartitionDetails {
    let next = rng.i32(0..num_partitions);
    let addr_str = addr.to_string();
    let key = ustr(&(addr_str.clone() + "|"+ &next.to_string()));
    let order_key = ustr(&addr_str);

    (Some(next),key,order_key)
}

fn round_robin_partition(addr: &SocketAddr, start_partition: &AtomicI32, num_partitions: i32) -> PartitionDetails {
    let next = start_partition.fetch_add(1, Ordering::SeqCst) % num_partitions;

    debug!("SockAddr: {} partition: {}",addr, next);

    let addr_str = addr.to_string();
    let key = ustr(&(addr_str.clone() + "|"+ &next.to_string()));
    let order_key = ustr(&addr_str);

    (Some(next),key,order_key)
}

#[cached(key = "SocketAddr", convert = r#"{ *addr }"#, sync_writes = true)]
fn sticky_partition(addr: &SocketAddr, start_partition: &AtomicI32, num_partitions: i32) -> PartitionDetails {
    
    let next = start_partition.fetch_add(1, Ordering::SeqCst) % num_partitions;

    let key = ustr(&(addr.to_string() +"|"+ &next.to_string()));
    let val = (Some(next),key,key);

    debug!("SockAddr: {} partition: {}",addr, next);

    val
}

pub trait CheckpointStrategy {
    fn check(&self, data: (&DataPacket,&Option<i32>)) -> bool;
}

impl CheckpointStrategy for CheckpointStrategies {
    fn check(&self, _data: (&DataPacket,&Option<i32>)) -> bool {
        match self {
            CheckpointStrategies::OpenDoors => true,
            CheckpointStrategies::ClosedDoors => false,
            CheckpointStrategies::FlipCoin => fastrand::bool()
        }
    }
}

pub trait TransformStrategy {
    fn transform(&self, addr: &SocketAddr, payload: &[u8], partition: &Option<i32>) -> Vec<u8>;
}

#[derive(Clone)]
pub enum TransformerStrategies {
    NoTransform
}

impl TransformStrategy for TransformerStrategies {
    fn transform(&self, _addr: &SocketAddr, payload: &[u8], _partition: &Option<i32>) -> Vec<u8> {
        match self {
            TransformerStrategies::NoTransform => payload.to_vec(),
        }
    }
}