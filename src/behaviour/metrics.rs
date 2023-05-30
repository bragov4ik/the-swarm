use std::time::Instant;

use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug, Clone)]
enum EventStatus {
    /// (start time)
    InProgress(Instant),
    NotStarted,
}

#[derive(Debug, Clone)]
pub struct PeriodicEvent {
    status: EventStatus,
    /// (event start time, duration of the event)
    event_timings: Vec<(Instant, std::time::Duration)>,
}

impl PeriodicEvent {
    pub fn new() -> Self {
        Self {
            status: EventStatus::NotStarted,
            event_timings: Vec::new(),
        }
    }

    /// Only one event in progress is supported. Each call to
    /// `record_event_start` should be followed by `record_event_end`,
    /// excessive calls are ignored
    pub fn record_start(&mut self) {
        if let EventStatus::InProgress(_) = self.status {
            return;
        }
        let start_time = Instant::now();
        self.status = EventStatus::InProgress(start_time);
    }

    /// Only one event in progress is supported. Each call to
    /// `record_event_start` should be followed by `record_event_end`,
    /// excessive calls are ignored
    pub fn record_end(&mut self) {
        let EventStatus::InProgress(start_time) = self.status else {
            return
        };
        let end_time = Instant::now();
        let duration = end_time - start_time;
        self.status = EventStatus::NotStarted;
        self.event_timings.push((start_time, duration));
    }

    pub fn get_data(&self) -> &Vec<(Instant, std::time::Duration)> {
        &self.event_timings
    }

    pub fn generate_data_for_step(&self) -> Vec<(f32, f32)> {
        let Some((t_start, _)) = self.event_timings.get(0) else {
            return vec![];
        };
        let data = self
            .event_timings
            .iter()
            .flat_map(|(t, y)| {
                let event_triggered = t.duration_since(*t_start);
                let event_finished = event_triggered + *y;
                vec![
                    (event_triggered.as_secs_f32(), 0f32),
                    (event_finished.as_secs_f32(), 1f32),
                ]
                .into_iter()
            })
            .collect();
        data
    }
}

/// A gauge is a metric that represents a single numerical
/// value that can arbitrarily go up and down.
/// (source: https://prometheus.io/docs/concepts/metric_types/#gauge)
#[derive(Debug, Clone)]
pub struct Gauge<V> {
    values: Vec<(Instant, V)>,
}

impl<V> Gauge<V>
where
    V: Clone + Default,
{
    pub fn new() -> Self {
        Self { values: vec![] }
    }

    /// Record new value of the gauge at current time
    pub fn record(&mut self, value: V) {
        let time = Instant::now();
        self.values.push((time, value));
    }

    pub fn get_data(&self) -> &Vec<(Instant, V)> {
        &self.values
    }

    pub fn generate_data_for_step(&self) -> Vec<(f32, V)> {
        let Some((t_start, _)) = self.values.get(0) else {
            return vec![];
        };
        let mut data: Vec<_> = self
            .values
            .windows(2)
            .flat_map(|pair| {
                let (_t1, v1) = &pair[0];
                let (t2, _v2) = &pair[1];
                let triggered = t2.duration_since(*t_start);
                vec![(triggered.as_secs_f32(), v1.clone())].into_iter()
            })
            .collect();
        data.insert(0, (0.0, V::default()));
        let now = Instant::now().duration_since(*t_start).as_secs_f32();
        data.push((now, self.values.last().unwrap().1.clone()));
        data
    }
}

#[derive(Debug, Clone)]
pub struct Metrics {
    pub sync: PeriodicEvent,
    pub consensus_queue_size: Gauge<usize>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            sync: PeriodicEvent::new(),
            consensus_queue_size: Gauge::new(),
        }
    }

    pub fn update_queue_size<T>(queue: &mpsc::Sender<T>, metric: &mut Gauge<usize>) {
        let total_capacity = queue.max_capacity();
        let free = queue.capacity();
        let Some(size) = total_capacity.checked_sub(free) else {
            warn!("Couldn't calculate queue capacity");
            return
        };
        metric.record(size);
    }
}
