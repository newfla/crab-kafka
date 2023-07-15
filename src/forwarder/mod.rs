use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    ops::Index,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use derive_builder::Builder;
use kanal::{bounded_async, unbounded_async};
use rdkafka::{
    producer::{FutureProducer, Producer},
    ClientConfig,
};
use tokio::{
    sync::OnceCell,
    task::{AbortHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use ustr::ustr;

use crate::{
    dispatcher::DispatcherTaskBuilder,
    receiver::ReceiverTaskBuilder,
    sender::KafkaPacketSenderBuilder,
    statistics::StatisticsTaskBuilder,
    strategies::{CheckpointStrategy, PartitionStrategy},
    ForwarderReturn, Receiver, TransformStrategy,
};

type GlobalForwarder = Mutex<Vec<ForwarderShutdownHandle>>;
type AbortHandleForwarder = Arc<tokio::sync::Mutex<Vec<AbortHandle>>>;

static GLOBAL_HANDLE: OnceCell<GlobalForwarder> = OnceCell::const_new();

/// Send an orderly shutdown signal to all [`crate::forwarder::Forwarder`] instances
pub fn shutdown_all() {
    if !GLOBAL_HANDLE.initialized() {
        return;
    }

    for handle in GLOBAL_HANDLE.get().unwrap().lock().unwrap().iter() {
        handle.shutdown()
    }
}

/// Abort all instances of [`crate::forwarder::Forwarder`] without waiting their orderly shutdown
pub fn abort_all() {
    if !GLOBAL_HANDLE.initialized() {
        return;
    }

    GLOBAL_HANDLE.get().unwrap().lock().unwrap().clear();
}

/// Handle to order an orderly shutdown to the referenced [`crate::forwarder::Forwarder`] instance
#[derive(Default, Clone)]
pub struct ForwarderShutdownHandle {
    cancel_token: CancellationToken,
    abort_handle: AbortHandleForwarder,
}

impl ForwarderShutdownHandle {
    /// Send an orderly shutdown signal to the referenced [`crate::forwarder::Forwarder`] instance
    pub fn shutdown(&self) {
        self.cancel_token.cancel();
    }
}

impl Drop for ForwarderShutdownHandle {
    fn drop(&mut self) {
        self.shutdown();
        self.abort_handle
            .blocking_lock()
            .iter()
            .for_each(|handle| handle.abort())
    }
}

#[derive(Clone, Copy)]
struct ForwarderId {
    id: usize,
}

impl Default for ForwarderId {
    fn default() -> Self {
        if !GLOBAL_HANDLE.initialized() {
            let _ = GLOBAL_HANDLE.set(GlobalForwarder::default());
        }

        let mut data = GLOBAL_HANDLE.get().unwrap().lock().unwrap();
        let id = data.len();
        data.push(ForwarderShutdownHandle::default());
        Self { id }
    }
}

impl Index<ForwarderId> for Vec<ForwarderShutdownHandle> {
    type Output = ForwarderShutdownHandle;

    fn index(&self, index: ForwarderId) -> &Self::Output {
        &self[index.id]
    }
}

/// A single forwarder instance. Must be built using [`crate::forwarder::ForwarderBuilder`]
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct Forwarder<C, P, T>
where
    C: CheckpointStrategy,
    P: PartitionStrategy,
    T: TransformStrategy,
{
    receiver: Receiver,
    partition: P,
    checkpoint: C,
    transform: T,
    kafka_settings: HashMap<String, String>,
    topic: String,
    stats_interval: u64,
    cache_size: usize,
    #[builder(private, default)]
    id: ForwarderId,
}

impl<C, P, T> Forwarder<C, P, T>
where
    C: CheckpointStrategy + Send + 'static,
    P: PartitionStrategy + Send + 'static,
    T: TransformStrategy + Clone + Send + Sync + 'static,
{
    async fn run(mut self) -> ForwarderReturn {
        let producer = self.build_kafka_producer()?;
        let partitions_count = self.find_partition_number(&producer)? as i32;

        //Get handle
        let handle = self.shutdown_handle();

        //Communication channel between receiver and dispatcher tasks
        let (dispatcher_tx, dispatcher_rx) = bounded_async(self.cache_size);

        //Define channel to send statistics update
        let (stats_tx, stats_rx) = unbounded_async();

        let kafka_sender = KafkaPacketSenderBuilder::default()
            .producer(producer)
            .output_topic(ustr(&self.topic))
            .stats_tx(stats_tx)
            .transform_strategy(Arc::new(self.transform))
            .build()
            .map_err(|err| err.to_string())?;

        self.partition.set_num_partitions(partitions_count);

        //Istantiate tasks
        let stat_task = StatisticsTaskBuilder::default()
            .shutdown_token(handle.cancel_token.clone())
            .stats_rx(stats_rx)
            .timeout(self.stats_interval)
            .build()
            .map_err(|err| err.to_string())?;

        let receiver_task = ReceiverTaskBuilder::from(self.receiver)
            .shutdown_token(handle.cancel_token.clone())
            .dispatcher_sender(dispatcher_tx)
            .build()
            .map_err(|err| err.to_string())?;

        let dispatcher_task = DispatcherTaskBuilder::default()
            .shutdown_token(handle.cancel_token.clone())
            .dispatcher_receiver(dispatcher_rx)
            .checkpoint_strategy(self.checkpoint)
            .partition_strategy(self.partition)
            .kafka_sender(kafka_sender)
            .build()
            .map_err(|err| err.to_string())?;

        //Schedule task
        let mut task_set = JoinSet::new();
        let mut guard = handle.abort_handle.lock().await;
        guard.push(task_set.spawn(stat_task.into_future()));
        guard.push(task_set.spawn(dispatcher_task.into_future()));
        guard.push(task_set.spawn(receiver_task.into_future()));

        while task_set.join_next().await.is_some() {}

        Ok(())
    }

    /// Get the handle to order an orderly shutdown
    pub fn shutdown_handle(&self) -> ForwarderShutdownHandle {
        GLOBAL_HANDLE.get().unwrap().lock().unwrap()[self.id].clone()
    }

    fn build_kafka_producer(&self) -> Result<FutureProducer, String> {
        let mut client_config = ClientConfig::new();
        self.kafka_settings.iter().for_each(|(key, value)| {
            client_config.set(key, value);
        });
        client_config.create().map_err(|err| err.to_string())
    }

    fn find_partition_number(&self, producer: &FutureProducer) -> Result<usize, String> {
        let topic_name = self.topic.as_str();
        let timeout = Duration::from_secs(30);

        match producer.client().fetch_metadata(Some(topic_name), timeout) {
            Err(_) => Err("Failed to retrieve topic metadata".to_string()),
            Ok(metadata) => match metadata.topics().first() {
                None => Err("Topic".to_string() + topic_name + "not found"),
                Some(data) => {
                    if data.partitions().is_empty() {
                        Err("Topic has 0 partitions".to_string())
                    } else {
                        Ok(data.partitions().len())
                    }
                }
            },
        }
    }
}

impl<C, P, T> IntoFuture for Forwarder<C, P, T>
where
    C: CheckpointStrategy + Send + 'static,
    P: PartitionStrategy + Send + 'static,
    T: TransformStrategy + Clone + Send + Sync + 'static,
{
    type Output = ForwarderReturn;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}
