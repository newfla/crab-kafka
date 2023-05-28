# crab_kafka

[![Crates.io][crates-badge]][crates-url]
[![Apache licensed][apache-badge]][apache-url]
[![Docs.rs][docs-badge]][docs-url]

[crates-badge]: https://img.shields.io/crates/v/crab-kafka.svg
[crates-url]: https://crates.io/crates/crab-kafka
[docs-badge]: https://docs.rs/crab-kafka/badge.svg
[docs-url]: https://docs.rs/crab-kafka/latest/
[apache-badge]: https://img.shields.io/badge/license-Apache2.0-blue.svg
[apache-url]: https://github.com/newfla/crab-kafka/blob/master/LICENSE

Forward <TCP|UDP> + TLS traffic to kafka.

Based on [tokio](https://github.com/tokio-rs/tokio) and [rust rdkafka](https://github.com/fede1024/rust-rdkafka)

## Basic Usage 
It's strongly encouraged the use of alternative allocator like [MiMalloc](https://crates.io/crates/mimalloc)

```no_run
use std::collections::HashMap;
use mimalloc::MiMalloc;
use crab_kafka::{forwarder::ForwarderBuilder,Receiver,PartitionStrategies,CheckpointStrategies,TransformStrategies};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(),String> {
    ForwarderBuilder::default()
    .receiver(Receiver::new_tcp_stream("127.0.0.1".to_owned(), "8888".to_owned(), 2000))
    .checkpoint(CheckpointStrategies::OpenDoors)
    .partition(PartitionStrategies::new_sticky_round_robin())
    .transform(TransformStrategies::NoTransform)
    .kafka_settings(HashMap::from([("bootstrap.servers".to_owned(),"broker:29091".to_owned())]))
    .topic("test_topic".to_owned())
    .cache_size(1000)
    .stats_interval(10)
    .build()
    .unwrap()
    .await
}
```
## Examples
- [UDP and DTLS example](https://github.com/newfla/crab-kafka/tree/main/examples/udp_to_kafka)