use std::collections::BTreeMap;

use whdr_ext_dev::handle_dev_dispatch;
use whdr_proto::SrvMsg;

#[test]
fn dev_extension_echoes_body_and_emits_dev_event() {
    let body = b"hello";
    let (reply, events) = handle_dev_dispatch(SrvMsg::Dispatch {
        req_id: uuid::Uuid::nil(),
        method: "POST".to_string(),
        path: "/dev".to_string(),
        query: None,
        headers: BTreeMap::new(),
        body_b64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, body),
        secret: None,
    })
    .unwrap();

    assert_eq!(reply.status, 200);
    assert_eq!(reply.body, "hello");
    assert_eq!(events[0].channel, "dev.echo");
}
