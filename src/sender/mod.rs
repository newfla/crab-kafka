use std::sync::Arc;

use coarsetime::Instant;
use derive_builder::Builder;
use kanal::AsyncSender;
use log::debug;
use nohash_hasher::IntMap;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::{spawn, sync::OnceCell};
use ustr::Ustr;

use crate::{
    statistics::StatisticData, DataPacket, DataTransmitted, PartitionDetails, Ticket,
    TransformStrategy,
};

static ONCE_PRODUCER: OnceCell<FutureProducer> = OnceCell::const_new();

#[derive(Builder)]
pub struct KafkaPacketSender<T>
where
    T: TransformStrategy,
{
    #[builder(setter(custom))]
    producer: &'static FutureProducer,
    #[builder(setter(custom))]
    output_topic: &'static str,
    #[builder(private, default = "IntMap::default()")]
    sender_tasks_map: IntMap<u64, Ticket>,
    stats_tx: AsyncSender<DataTransmitted>,
    transform_strategy: Arc<T>,
}
impl<T> KafkaPacketSenderBuilder<T>
where
    T: TransformStrategy,
{
    pub fn producer(&mut self, producer: FutureProducer) -> &mut Self {
        let _ = ONCE_PRODUCER.set(producer);
        self.producer = ONCE_PRODUCER.get();
        self
    }

    pub fn output_topic(&mut self, output_topic: Ustr) -> &mut Self {
        self.output_topic = Some(output_topic.as_str());
        self
    }
}

impl<T> KafkaPacketSender<T>
where
    T: TransformStrategy + Send + Sync + 'static,
{
    #[inline(always)]
    async fn send_stat(
        stats_tx: AsyncSender<DataTransmitted>,
        len: usize,
        recv_time: Instant,
        key: u64,
    ) {
        let stat = StatisticData::new(recv_time, Instant::now(), len, key);

        let _ = stats_tx.send(Some(stat)).await;
    }

    #[inline(always)]
    async fn send_data_loss(stats_tx: AsyncSender<DataTransmitted>) {
        let _ = stats_tx.send(None).await;
    }

    #[inline(always)]
    pub fn send_to_kafka(&mut self, packet: DataPacket, partition_detail: PartitionDetails) {
        let producer = self.producer;
        let output_topic = self.output_topic;
        let stats_tx = self.stats_tx.clone();
        let transform = self.transform_strategy.clone();

        let (payload, (len, addr), recv_time) = packet;
        let (partition, key, key_hash) = partition_detail;
        let key_hash = key_hash.precomputed_hash();

        //Notify for the next task
        let notify_next = Ticket::default();
        let notify_prev = match self.sender_tasks_map.insert(key_hash, notify_next.clone()) {
            Some(prev) => prev,
            None => {
                let fake_notify = Ticket::default();
                fake_notify.notify_one();
                fake_notify
            }
        };

        spawn(async move {
            unsafe {
                let payload =
                    transform.transform(&addr, payload.get_unchecked(..len), &partition_detail.0);

                let mut record = FutureRecord {
                    topic: output_topic,
                    partition,
                    payload: Some(&payload),
                    key: Some(key.as_str()),
                    timestamp: None,
                    headers: None,
                };

                debug!("{} bytes with key {} ready to be sent", payload.len(), key);
                notify_prev.notified().await;

                loop {
                    match producer.send_result(record) {
                        Ok(enqueuing_ok) => {
                            notify_next.notify_one();
                            match enqueuing_ok.await {
                                Ok(_) => Self::send_stat(stats_tx, len, recv_time, key_hash).await,
                                Err(_) => Self::send_data_loss(stats_tx).await,
                            }
                            break;
                        }
                        Err((_, rec)) => record = rec,
                    }
                }
            }
        });
    }
}
