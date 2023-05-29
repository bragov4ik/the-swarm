use std::time::Instant;

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
}

#[derive(Debug, Clone)]
pub struct Metrics {
    pub sync: PeriodicEvent,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            sync: PeriodicEvent::new(),
        }
    }
}
