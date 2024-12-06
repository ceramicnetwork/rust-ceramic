#[macro_export]
macro_rules! actor_message {
    (
        $enum_name:ident,
        $(
            notify $notify_variant_name:ident => $notify_message_type:ty,
        )*
        $(
            send $send_variant_name:ident => $send_message_type:ty,
        )*
    ) => {
        enum $enum_name {
            $(
                $notify_variant_name($notify_message_type),
            )*
            $(
                $send_variant_name(
                    $send_message_type,
                    tokio::sync::oneshot::Sender<<$send_message_type as crate::Message>::Result>,
                ),
            )*
        }
        impl $enum_name {
            async fn run<A>(mut actor: A )
                where A: Actor<Envelope = $enum_name> $(+ Handler<$notify_message_type>)* $(+ Handler<$send_message_type>)*
            {
                let mut receiver = actor.receiver();
                while let Some(traced_msg) = receiver.recv().await {
                    match traced_msg.msg {
                        $(
                            $enum_name::$notify_variant_name(m) => {
                                actor.handle(m).instrument(traced_msg.span).await;
                            }
                        )*
                        $(
                            $enum_name::$send_variant_name(m, respond_to) => {
                                respond_to
                                    .send(actor.handle(m).instrument(traced_msg.span).await)
                                    .unwrap();
                            }
                        )*
                    }
                }
            }
        }
        $(
        impl std::convert::From<$notify_message_type> for $enum_name {
            fn from(value: $notify_message_type) -> Self {
                Self::$notify_variant_name(value)
            }
        }
        )*
        $(
        impl std::convert::From<($send_message_type, tokio::sync::oneshot::Sender<<$send_message_type as crate::Message>::Result>)> for $enum_name {
            fn from(value: ($send_message_type, tokio::sync::oneshot::Sender<<$send_message_type as crate::Message>::Result>)) -> Self {
                Self::$send_variant_name(value.0, value.1)
            }
        }
        )*
    };
}
