use std::{collections::HashMap, fmt};

use serde::de;
use serde::Deserializer;
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Debug, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum MessageType {
    Update = 0,
    Query = 1,
    Response = 2,
    Keepalive = 3,
}

#[derive(Debug)]
pub enum Message {
    Update {
        stream: String,
        tip: String,
        model: Option<String>,
    },
    Query {
        id: String,
        stream: String,
    },
    Response {
        id: String,
        tips: HashMap<String, String>,
    },
    Keepalive {
        ts: i64,
        ver: String,
    },
}

// We need manually implement deserialize for Message until this feature is implemented
// https://github.com/serde-rs/serde/issues/745
// PR implementing the feature https://github.com/serde-rs/serde/pull/2056
impl<'de> de::Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> de::Visitor<'de> for MessageVisitor {
            type Value = Message;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("message type")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                if let Some(key) = map.next_key::<String>()? {
                    if key != "typ" {
                        return Err(de::Error::missing_field("typ"));
                    }
                    let typ: MessageType = map.next_value()?;
                    match typ {
                        MessageType::Update => {
                            let mut stream: Option<String> = None;
                            let mut tip: Option<String> = None;
                            let mut model: Option<String> = None;
                            loop {
                                match map.next_key::<&str>()? {
                                    Some("stream") => {
                                        stream = Some(map.next_value()?);
                                    }
                                    Some("tip") => {
                                        tip = Some(map.next_value()?);
                                    }
                                    Some("model") => {
                                        model = Some(map.next_value()?);
                                    }
                                    // Explicitly ignore the doc field
                                    Some("doc") => {
                                        map.next_value::<&str>()?;
                                    }
                                    // Error on unknown fields
                                    Some(k) => {
                                        return Err(de::Error::unknown_field(
                                            k,
                                            &["stream", "tip", "model", "doc"],
                                        ));
                                    }
                                    None => {
                                        // We are done, validate we got all fields and return
                                        if let Some(stream) = stream {
                                            if let Some(tip) = tip {
                                                return Ok(Message::Update { stream, tip, model });
                                            } else {
                                                return Err(de::Error::missing_field("tip"));
                                            }
                                        } else {
                                            return Err(de::Error::missing_field("stream"));
                                        }
                                    }
                                }
                            }
                        }
                        MessageType::Query => {
                            let mut id: Option<String> = None;
                            let mut stream: Option<String> = None;
                            loop {
                                match map.next_key::<&str>()? {
                                    Some("id") => {
                                        id = Some(map.next_value()?);
                                    }
                                    Some("stream") => {
                                        stream = Some(map.next_value()?);
                                    }
                                    // Explicitly ignore the doc field
                                    Some("doc") => {
                                        map.next_value::<&str>()?;
                                    }
                                    // Error on unknown fields
                                    Some(k) => {
                                        return Err(de::Error::unknown_field(
                                            k,
                                            &["id", "stream", "doc"],
                                        ));
                                    }
                                    None => {
                                        // We are done, validate we got all fields and return
                                        if let Some(stream) = stream {
                                            if let Some(id) = id {
                                                return Ok(Message::Query { stream, id });
                                            } else {
                                                return Err(de::Error::missing_field("id"));
                                            }
                                        } else {
                                            return Err(de::Error::missing_field("stream"));
                                        }
                                    }
                                }
                            }
                        }
                        MessageType::Response => {
                            let mut id: Option<String> = None;
                            let mut tips: Option<HashMap<String, String>> = None;
                            loop {
                                match map.next_key::<&str>()? {
                                    Some("id") => {
                                        id = Some(map.next_value()?);
                                    }
                                    Some("tips") => {
                                        tips = Some(map.next_value()?);
                                    }
                                    // Error on unknown fields
                                    Some(k) => {
                                        return Err(de::Error::unknown_field(k, &["id", "tips"]));
                                    }
                                    None => {
                                        // We are done, validate we got all fields and return
                                        if let Some(id) = id {
                                            if let Some(tips) = tips {
                                                return Ok(Message::Response { id, tips });
                                            } else {
                                                return Err(de::Error::missing_field("tips"));
                                            }
                                        } else {
                                            return Err(de::Error::missing_field("id"));
                                        }
                                    }
                                }
                            }
                        }
                        MessageType::Keepalive => {
                            let mut ts: Option<i64> = None;
                            let mut ver: Option<String> = None;
                            loop {
                                match map.next_key::<&str>()? {
                                    Some("ts") => {
                                        ts = Some(map.next_value()?);
                                    }
                                    Some("ver") => {
                                        ver = Some(map.next_value()?);
                                    }
                                    // Error on unknown fields
                                    Some(k) => {
                                        return Err(de::Error::unknown_field(k, &["ts", "ver"]));
                                    }
                                    None => {
                                        // We are done, validate we got all fields and return
                                        if let Some(ts) = ts {
                                            if let Some(ver) = ver {
                                                return Ok(Message::Keepalive { ts, ver });
                                            } else {
                                                return Err(de::Error::missing_field("ver"));
                                            }
                                        } else {
                                            return Err(de::Error::missing_field("ts"));
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    Err(de::Error::missing_field("typ"))
                }
            }
        }

        deserializer.deserialize_map(MessageVisitor {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::{expect, Expect};

    fn deserialize(json: &str, expect: Expect) {
        let msg: Message = serde_json::from_str(json).unwrap();
        expect.assert_debug_eq(&msg);
    }

    #[test]
    fn test_de_update() {
        // Test with minimum required fields
        deserialize(
            r#"{
    "typ":0,
    "stream": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
    "tip": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39"
}"#,
            expect![[r#"
                Update {
                    stream: "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
                    tip: "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
                    model: None,
                }
            "#]],
        );
        // Test with maximum fields
        deserialize(
            r#"{
    "typ":0,
    "stream": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
    "doc": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
    "model": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
    "tip": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39"
}"#,
            expect![[r#"
                Update {
                    stream: "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
                    tip: "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
                    model: Some(
                        "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
                    ),
                }
            "#]],
        );
    }

    #[test]
    fn test_de_query() {
        // Test with minimum required fields
        deserialize(
            r#"{
    "typ":1,
    "id":"EiDJzhPfx5LNWKc2G49O42NvgfXb76bhKwTTdXapSujoGg",
    "stream": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39"
}"#,
            expect![[r#"
                Query {
                    id: "EiDJzhPfx5LNWKc2G49O42NvgfXb76bhKwTTdXapSujoGg",
                    stream: "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
                }
            "#]],
        );
        // Test with maximum fields
        deserialize(
            r#"{
    "typ":1,
    "id":"EiDJzhPfx5LNWKc2G49O42NvgfXb76bhKwTTdXapSujoGg",
    "stream": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
    "doc":"kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39"
}"#,
            expect![[r#"
                Query {
                    id: "EiDJzhPfx5LNWKc2G49O42NvgfXb76bhKwTTdXapSujoGg",
                    stream: "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
                }
            "#]],
        );
    }
    #[test]
    fn test_de_response() {
        // Test with minimum required fields
        deserialize(
            r#"{
    "typ":2,
    "id":"EiDJzhPfx5LNWKc2G49O42NvgfXb76bhKwTTdXapSujoGg",
    "tips": {"kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39":
    "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39"}
}"#,
            expect![[r#"
                Response {
                    id: "EiDJzhPfx5LNWKc2G49O42NvgfXb76bhKwTTdXapSujoGg",
                    tips: {
                        "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39": "kjzl6cwe1jw145as2el62s5k2n5xwij7snqu1mngluhpr05xy5wylfswe1zvq39",
                    },
                }
            "#]],
        );
    }
    #[test]
    fn test_de_keepalive() {
        deserialize(
            r#"{
    "typ":3,
    "ts": 5,
    "ver": "2.23.0"
}"#,
            expect![[r#"
                Keepalive {
                    ts: 5,
                    ver: "2.23.0",
                }
            "#]],
        );
    }
}
