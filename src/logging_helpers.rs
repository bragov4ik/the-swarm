#[macro_export]
macro_rules! channel_log_send {
    ($chan_name:literal, $msg:expr) => {
        tracing::event!(target: "channel_send", tracing::Level::DEBUG, "Sent to {}: {}", $chan_name, $msg);
    };
    ($chan_name:literal, $lvl:expr, $msg:expr) => {
        tracing::event!(target: "channel_send", $lvl, "Sent to {}: {}", $chan_name, $msg);
    };
}

#[macro_export]
macro_rules! channel_log_recv {
    ($chan_name:literal, $msg:expr) => {
        tracing::event!(target: "channel_recv", tracing::Level::DEBUG, "Received from {}: {}", $chan_name, $msg);
    };
    ($chan_name:literal, $lvl:expr, $msg:expr) => {
        tracing::event!(target: "channel_recv", $lvl, "Received from {}: {}", $chan_name, $msg);
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        channel_log_send!("aboba.input", "init_storage");
    }
}
