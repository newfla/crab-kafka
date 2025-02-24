use std::{
    fmt::Display,
    future::{Future, IntoFuture},
    pin::Pin,
};

use byte_unit::{Byte, UnitType};
use coarsetime::{Duration, Instant};
use derive_builder::Builder;
use derive_new::new;
use itertools::{
    Itertools,
    MinMaxResult::{MinMax, NoElements, OneElement},
};
use kanal::AsyncReceiver;
use log::info;
use nohash_hasher::IntSet;
use tokio::{select, time::interval_at};
use tokio_util::sync::CancellationToken;

use crate::DataTransmitted;

trait Stats {
    fn add_loss(&mut self);
    fn add_stat(&mut self, recv_time: Instant, send_time: Instant, size: usize, key: u64);
    fn calculate_and_reset(&mut self) -> Option<StatSummary>;
    fn calculate(&self) -> Option<StatSummary>;
    fn reset(&mut self);
}

#[derive(Debug, PartialEq)]
struct StatSummary {
    bandwidth: f32,
    min_latency: Duration,
    max_latency: Duration,
    average_latency: Duration,
    lost_packets: usize,
    unique_connections: usize,
}

impl Display for StatSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let min = self.min_latency.as_millis();
        let max = self.max_latency.as_millis();
        let average = self.average_latency.as_millis();

        let bandwidth = self.bandwidth * 8.;
        let bandwidth = Byte::from_f32(bandwidth)
            .unwrap_or_default()
            .get_appropriate_unit(UnitType::Decimal)
            .to_string();
        let bandwidth = &bandwidth[0..bandwidth.len() - 1];

        writeln!(
            f,
            "\nLost packets: {}\nUnique connections: {}\nBandwidth: {bandwidth}bit/s\nLatency: <min: {min}, max: {max}, average: {average}> ms",
            self.lost_packets, self.unique_connections
        )
    }
}

#[derive(Clone, new)]
pub struct StatsHolder {
    period: Duration,
    #[new(default)]
    stats_vec: Vec<StatElement>,
    #[new(default)]
    lost_packets: usize,
    #[new(default)]
    active_connections: IntSet<u64>,
}

impl Default for StatsHolder {
    fn default() -> Self {
        StatsHolder {
            period: Duration::new(10, 0),
            stats_vec: Vec::default(),
            lost_packets: usize::default(),
            active_connections: IntSet::default(),
        }
    }
}

impl Stats for StatsHolder {
    fn add_loss(&mut self) {
        self.lost_packets += 1;
    }

    fn add_stat(&mut self, recv_time: Instant, send_time: Instant, size: usize, key: u64) {
        let latency = send_time.duration_since(recv_time);

        self.stats_vec.push(StatElement { latency, size });

        self.active_connections.insert(key);
    }

    fn calculate_and_reset(&mut self) -> Option<StatSummary> {
        let res = self.calculate();
        self.reset();
        res
    }

    fn calculate(&self) -> Option<StatSummary> {
        if self.stats_vec.is_empty() {
            return None;
        }

        let latency = self.stats_vec.iter().map(|elem| elem.latency);

        let packet_processed = latency.len();

        let (min_latency, max_latency) = match latency.clone().minmax() {
            NoElements => (Duration::new(0, 0), Duration::new(0, 0)),
            OneElement(elem) => (elem, elem),
            MinMax(min, max) => (min, max),
        };

        let average_latency =
            latency.fold(Duration::new(0, 0), |acc, e| acc + e) / packet_processed as u32;

        let mut bandwidth = self.stats_vec.iter().map(|elem| elem.size).sum::<usize>() as f32;

        bandwidth /= self.period.as_secs() as f32;

        Some(StatSummary {
            bandwidth,
            min_latency,
            max_latency,
            average_latency,
            lost_packets: self.lost_packets,
            unique_connections: self.active_connections.len(),
        })
    }

    fn reset(&mut self) {
        self.stats_vec.clear();
        self.lost_packets = usize::default();
        self.active_connections.clear();
    }
}

#[derive(Copy, Clone)]
struct StatElement {
    latency: Duration,
    size: usize,
}

#[cfg(test)]
mod statistics_tests {
    use coarsetime::{Duration, Instant};

    use crate::statistics::*;

    #[test]
    fn test_stats_holder() {
        let mut stats: Box<dyn Stats> = Box::new(StatsHolder::default());
        let reference = Instant::now();

        stats.add_loss();

        stats.add_stat(reference, reference + Duration::new(2, 0), 128, 1);

        stats.add_stat(
            reference + Duration::new(3, 0),
            reference + Duration::new(4, 0),
            128,
            2,
        );

        stats.add_stat(
            reference + Duration::new(4, 0),
            reference + Duration::new(10, 0),
            256,
            1,
        );

        let stats_oracle = StatSummary {
            bandwidth: 512. / 10.,
            min_latency: Duration::new(1, 0),
            max_latency: Duration::new(6, 0),
            average_latency: Duration::new(3, 0),
            lost_packets: 1,
            unique_connections: 2,
        };

        let stats = stats.calculate_and_reset();
        assert_ne!(stats, None);

        let stats = stats.unwrap();
        assert_eq!(stats, stats_oracle);

        println!("{}", stats);
    }
}

#[derive(new)]
pub struct StatisticData {
    recv_time: Instant,
    send_time: Instant,
    size: usize,
    conn_key: u64,
}

#[derive(Builder)]
pub struct StatisticsTask {
    shutdown_token: CancellationToken,
    stats_rx: AsyncReceiver<DataTransmitted>,
    #[builder(setter(custom))]
    timeout: Duration,
    #[builder(private)]
    holder: StatsHolder,
}

impl StatisticsTaskBuilder {
    pub fn timeout(&mut self, stats_interval: u64) -> &mut Self {
        let timeout = Duration::new(stats_interval, 0);
        self.timeout = Some(timeout);
        self.holder = Some(StatsHolder::new(timeout));
        self
    }
}

impl StatisticsTask {
    async fn run(mut self) {
        //Arm the timer to produce statistics at regular intervals
        let start = tokio::time::Instant::now() + self.timeout.into();
        let mut timer = interval_at(start, self.timeout.into());

        loop {
            select! {
                _ = self.shutdown_token.cancelled() => {
                    info!("Shutting down statistics task");
                    break
                }
                _ = timer.tick() => {
                    match self.holder.calculate_and_reset() { Some(summary) => {
                        info!("{}",summary);
                    } _ => {
                        info!("No data in transit in the last {} seconds", self.timeout.as_secs());
                    }}
                }
                stat = self.stats_rx.recv() => {
                    if let Ok(data) = stat {
                        match data {
                            Some(msg) => self.holder.add_stat(msg.recv_time, msg.send_time, msg.size, msg.conn_key),
                            None => self.holder.add_loss(),
                        }
                    }
                }
            }
        }
    }
}

impl IntoFuture for StatisticsTask {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}
