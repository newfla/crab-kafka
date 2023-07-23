use std::{
    future::{Future, IntoFuture},
    pin::Pin,
};

use derive_builder::Builder;
use kanal::AsyncReceiver;
use log::info;
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::{
    sender::KafkaPacketSender,
    strategies::{CheckpointStrategy, PartitionStrategy},
    DataPacket, TransformStrategy,
};

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct DispatcherTask<C, P, T>
where
    C: CheckpointStrategy,
    P: PartitionStrategy,
    T: TransformStrategy + Send,
{
    shutdown_token: CancellationToken,
    dispatcher_receiver: AsyncReceiver<DataPacket>,
    checkpoint_strategy: C,
    partition_strategy: P,
    kafka_sender: KafkaPacketSender<T>,
}

impl<C, P, T> DispatcherTask<C, P, T>
where
    C: CheckpointStrategy,
    P: PartitionStrategy,
    T: TransformStrategy + Send + Sync + 'static,
{
    #[inline]
    async fn dispatch_packet(&mut self, packet: DataPacket) {
        let partition = self.partition_strategy.partition(&packet.1);
        if !self.checkpoint_strategy.check((&packet, &partition.0)) {
            return;
        }
        self.kafka_sender.send_to_kafka(packet, partition);
    }

    async fn run(mut self) {
        loop {
            select! {
                 _ = self.shutdown_token.cancelled() => {
                     info!("Shutting down dispatcher task");
                     break;
                 }

                 data = self.dispatcher_receiver.recv() => {
                     if let Ok(pkt) = data  {
                         DispatcherTask::dispatch_packet(&mut self, pkt).await;
                     }
                 }
            }
        }
    }
}

impl<C, P, T> IntoFuture for DispatcherTask<C, P, T>
where
    C: CheckpointStrategy + Send + 'static,
    P: PartitionStrategy + Send + 'static,
    T: TransformStrategy + Send + Sync + 'static,
{
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}
