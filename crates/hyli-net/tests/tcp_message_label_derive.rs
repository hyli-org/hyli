#![allow(dead_code)]

use hyli_net::tcp::TcpMessageLabel;

#[derive(TcpMessageLabel)]
struct MyStruct {
    _id: u64,
}

#[derive(TcpMessageLabel)]
enum MyEnum {
    Unit,
    Tuple(()),
    Struct { _name: String },
}

#[derive(TcpMessageLabel)]
#[tcp_label(prefix = "Net")]
struct PrefixedStruct;

#[derive(TcpMessageLabel)]
#[tcp_label(label = "Request")]
struct LabeledStruct;

#[derive(TcpMessageLabel)]
#[tcp_label(prefix = "Net")]
enum PrefixedEnum {
    Ping,
    #[tcp_label(suffix = "Payload")]
    Data(Vec<u8>),
}

#[derive(TcpMessageLabel)]
#[tcp_label(label = "WireMsg", prefix = "Ignored")]
enum LabeledEnum {
    Ping,
    #[tcp_label(label = "CustomPong")]
    Pong,
}

#[test]
fn derive_tcp_message_label_struct_and_enum() {
    let s = MyStruct { _id: 7 };
    assert_eq!(s.message_label(), "MyStruct");

    assert_eq!(MyEnum::Unit.message_label(), "MyEnum::Unit");
    assert_eq!(MyEnum::Tuple(()).message_label(), "MyEnum::Tuple");
    assert_eq!(
        MyEnum::Struct {
            _name: "n".to_string()
        }
        .message_label(),
        "MyEnum::Struct"
    );
}

#[test]
fn derive_tcp_message_label_with_overrides() {
    assert_eq!(PrefixedStruct.message_label(), "Net::PrefixedStruct");
    assert_eq!(LabeledStruct.message_label(), "Request");

    assert_eq!(
        PrefixedEnum::Ping.message_label(),
        "Net::PrefixedEnum::Ping"
    );
    assert_eq!(
        PrefixedEnum::Data(vec![1, 2, 3]).message_label(),
        "Net::PrefixedEnum::Payload"
    );

    // Type label wins over prefix.
    assert_eq!(LabeledEnum::Ping.message_label(), "WireMsg::Ping");
    // Variant label is a full override.
    assert_eq!(LabeledEnum::Pong.message_label(), "CustomPong");
}
