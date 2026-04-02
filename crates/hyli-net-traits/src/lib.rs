pub use hyli_net_macros::TcpMessageLabel;

/// Returns the message label used by TCP metrics/logging.
pub trait TcpMessageLabel {
    fn message_label(&self) -> &'static str;
}

impl TcpMessageLabel for Vec<u8> {
    fn message_label(&self) -> &'static str {
        "bytes"
    }
}

impl TcpMessageLabel for String {
    fn message_label(&self) -> &'static str {
        "string"
    }
}
