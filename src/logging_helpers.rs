#[macro_export]
macro_rules! channel_log_send {
    ($chan_name:literal, $msg:expr) => {
        tracing::event!(target: crate::logging_helpers::Targets::ChannelSend.into_str(), tracing::Level::DEBUG, "Sent to {}: {}", $chan_name, $msg)
    };
    ($chan_name:literal, $lvl:expr, $msg:expr) => {
        tracing::event!(target: crate::logging_helpers::Targets::ChannelSend.into_str(), $lvl, "Sent to {}: {}", $chan_name, $msg)
    };
}

#[macro_export]
macro_rules! channel_log_recv {
    ($chan_name:literal, $msg:expr) => {
        tracing::event!(target: crate::logging_helpers::Targets::ChannelRecv.into_str(), tracing::Level::DEBUG, "Received from {}: {}", $chan_name, $msg)
    };
    ($chan_name:literal, $lvl:expr, $msg:expr) => {
        tracing::event!(target: crate::logging_helpers::Targets::ChannelRecv.into_str(), $lvl, "Received from {}: {}", $chan_name, $msg)
    };
}

pub enum Targets {
    ChannelSend,
    ChannelRecv,
    StorageInitialization,
    DataDistribution,
    ProgramExecution,
    DataRecollection,
    Synchronization,
}

impl Targets {
    pub const fn into_str(self) -> &'static str {
        match self {
            Targets::ChannelSend => "channel_send",
            Targets::ChannelRecv => "channel_recv",
            Targets::StorageInitialization => "storage_init",
            Targets::DataDistribution => "data_distr",
            Targets::ProgramExecution => "program_exec",
            Targets::DataRecollection => "data_recollect",
            Targets::Synchronization => "sync",
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        channel_log_send!("aboba.input", "init_storage");
        channel_log_recv!("aboba.input", "init_storage");
    }
}
