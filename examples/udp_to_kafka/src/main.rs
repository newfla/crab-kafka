use std::{collections::HashMap, future::IntoFuture};

use mimalloc::MiMalloc;
use tokio::{
    runtime::{Builder, Runtime},
    select, signal,
};

use crab_kafka::{
    forwarder::{Forwarder, ForwarderBuilder, ForwarderShutdownHandle},
    CheckpointStrategies, CheckpointStrategy, PartitionStrategies, PartitionStrategy, Receiver,
    TransformStrategies, TransformStrategy,
};
use utilities::{
    env_var::{self, load_env_var, EnvVars},
    logger,
    logger::*,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    //Init logger
    logger!();

    //Load env variables
    let vars = load_env_var().unwrap();

    //Receiver component
    let receiver = receiver_builder_helper(&vars);

    //Checkpoint strategy
    let checkpoint_strategy = checkpoint_strategy_builder_helper(&vars);

    //Partition strategy
    let partition_strategy = partition_strategy_builder_helper(&vars);

    //Build kafka params map
    let kafka_map = kafka_params_builder_helper(&vars);

    //Instantiate a forwarder instance;
    let forwarder = ForwarderBuilder::default()
        .receiver(receiver)
        .checkpoint(checkpoint_strategy)
        .partition(partition_strategy)
        .transform(TransformStrategies::NoTransform)
        .kafka_settings(kafka_map)
        .topic(vars.kafka_topic.clone())
        .cache_size(vars.cache_size)
        .stats_interval(vars.stats_interval)
        .build()
        .unwrap();

    runtime_builder_helper(&vars).block_on(start_receiving(forwarder));
}

async fn start_receiving<C, P, T>(forwarder: Forwarder<C, P, T>)
where
    C: CheckpointStrategy + Send + 'static,
    P: PartitionStrategy + Send + 'static,
    T: TransformStrategy + Clone + Send + Sync + 'static,
{
    let forwarder_handle: ForwarderShutdownHandle = forwarder.shutdown_handle();

    select! {
        _ = signal::ctrl_c() => {
                info!("Received CTRL-C signal");
                forwarder_handle.shutdown();
        },

        _ = forwarder.into_future() => {
            info!("Shutting down manager task");
        }
    }
}

fn receiver_builder_helper(vars: &EnvVars) -> Receiver {
    let ip = vars.listen_ip.to_owned();
    let port = vars.listen_port.to_string();

    let buffer_size = vars.buffer_size;

    if !vars.use_dtls {
        Receiver::new_udp_framed(ip, port, buffer_size)
    } else {
        Receiver::new_dtls_stream(
            ip,
            port,
            buffer_size,
            (vars.server_cert.clone(), vars.server_key.clone()),
        )
    }
}

fn checkpoint_strategy_builder_helper(vars: &EnvVars) -> CheckpointStrategies {
    info!(
        "Selected Checkpoint Strategy: {}",
        vars.checkpoint_strategy()
    );

    match vars.checkpoint_strategy() {
        env_var::CheckpointStrategy::OpenDoors => CheckpointStrategies::OpenDoors,
        env_var::CheckpointStrategy::ClosedDoors => CheckpointStrategies::ClosedDoors,
        env_var::CheckpointStrategy::FlipCoin => CheckpointStrategies::FlipCoin,
    }
}

fn partition_strategy_builder_helper(vars: &EnvVars) -> PartitionStrategies {
    info!(
        "Selected Partition Strategy: {}",
        vars.kafka_partition_strategy()
    );

    match vars.kafka_partition_strategy() {
        env_var::PartitionStrategy::None => PartitionStrategies::new_none(),
        env_var::PartitionStrategy::Random => PartitionStrategies::new_random(),
        env_var::PartitionStrategy::RoundRobin => PartitionStrategies::new_round_robin(),
        env_var::PartitionStrategy::StickyRoundRobin => {
            PartitionStrategies::new_sticky_round_robin()
        }
    }
}

fn kafka_params_builder_helper(vars: &EnvVars) -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert(
        "bootstrap.servers".to_owned(),
        vars.kafka_brokers.to_owned(),
    );
    map.insert(
        "batch.num.messages".to_owned(),
        vars.kafka_batch_num_messages.to_string(),
    );
    map.insert(
        "queue.buffering.max.ms".to_owned(),
        vars.kafka_queue_buffering_max_ms.to_string(),
    );
    map.insert(
        "queue.buffering.max.messages".to_owned(),
        vars.kafka_queue_buffering_max_messages.to_string(),
    );
    map.insert(
        "queue.buffering.max.kbytes".to_owned(),
        vars.kafka_queue_buffering_max_kbytes.to_string(),
    );
    map.insert(
        "compression.codec".to_owned(),
        vars.kafka_compression_codec.to_string(),
    );
    map.insert(
        "request.required.acks".to_owned(),
        vars.kafka_request_required_acks.to_string(),
    );
    map.insert("retries".to_owned(), vars.kafka_retries.to_string());
    map
}

pub fn runtime_builder_helper(vars: &EnvVars) -> Runtime {
    let mut rt_builder = Builder::new_multi_thread();
    if vars.worker_threads > 0 {
        rt_builder.worker_threads(vars.worker_threads);
    }
    rt_builder.enable_all().build().unwrap()
}
