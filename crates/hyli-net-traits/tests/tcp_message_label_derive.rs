#![allow(dead_code)]

use hyli_net_traits::TcpMessageLabel;

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
