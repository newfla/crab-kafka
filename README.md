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

## Examples
- [UDP and DTLS example](https://github.com/newfla/crab-kafka/tree/main/examples/udp_to_kafka)