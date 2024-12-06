#[macro_export]
macro_rules! actor_envelope {
    (
        $enum_name:ident,
        $(
            $variant_name:ident => $message_type:ty,
        )*
    ) => {
        /// Wrapping envelope for all messages of the actor.
        pub enum $enum_name {
            $(
                $variant_name(
                    $message_type,
                    tokio::sync::oneshot::Sender<<$message_type as $crate::Message>::Result>,
                ),
            )*
        }
        impl $enum_name {
            /// Runs the actor handling messages as they arive.
            pub async fn run<A>(mut actor: A )
                where A: $crate::Actor<Envelope = $enum_name> $(+ $crate::Handler<$message_type>)* $(+ $crate::Handler<$message_type>)*
            {
                let mut receiver = actor.receiver();
                while let Some(delivery) = receiver.recv().await {
                    match delivery {
                        $crate::DeliverOp::Send(traced) => {
                            match traced.value {
                                $(
                                    $enum_name::$variant_name(m, respond_to) => {
                                        respond_to
                                            .send(actor.handle(m).instrument(traced.span).await)
                                            .unwrap();
                                    }
                                )*
                            }
                        }
                        $crate::DeliverOp::Notify(traced) => {
                            match traced.value {
                                $(
                                    $enum_name::$variant_name(m, _respond_to) => {
                                        actor.handle(m).instrument(traced.span).await;
                                    }
                                )*
                            }
                        }
                    }
                }
            }
        }
        $(
        impl std::convert::From<($message_type, tokio::sync::oneshot::Sender<<$message_type as $crate::Message>::Result>)> for $enum_name {
            fn from(value: ($message_type, tokio::sync::oneshot::Sender<<$message_type as $crate::Message>::Result>)) -> Self {
                Self::$variant_name(value.0, value.1)
            }
        }
        )*
    };
}
