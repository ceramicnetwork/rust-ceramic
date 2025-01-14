/// Constructs an envelope enumeration that contains all messages for an actor.
///
/// The first identifier is the name of the enum.
/// The second identifier is the name of a trait specific to the actor.
/// The remaining pairs are the variants of the envelope indicating the messages the actor handles.
///
/// The constructed trait is a union of the [`crate::Handler`] traits for each message along with the [`crate::Actor`] trait.
#[macro_export]
macro_rules! actor_envelope {
    (
        $enum_name:ident,
        $trait_name:ident,
        $(
            $variant_name:ident => $message_type:ty,
        )*
    ) => {
        /// Wrapping envelope for all messages of the actor.
        pub enum $enum_name {
            $(
                #[doc = stringify!($variant_name)]
                #[doc = " contains messages of type [`"]
                #[doc = stringify!($message_type)]
                #[doc = "`]."]
                $variant_name(
                    $message_type,
                    tokio::sync::oneshot::Sender<<$message_type as $crate::Message>::Result>,
                ),
            )*
        }
        impl ::std::fmt::Debug for $enum_name
        where
            $(
                $message_type: std::fmt::Debug,
            )*
        {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                match self {
                    $(
                        $enum_name::$variant_name(msg, _) => f.debug_tuple(stringify!($variant_name))
                            .field(msg)
                            .field(&"_")
                            .finish(),
                    )*
                }
            }
        }
        #[doc = std::stringify!($trait_name)]
        #[doc = " is an [`crate::Actor`] and [`crate::Handler`] for each message type in the actor envelope "]
        #[doc = stringify!($enum_name)]
        #[doc = "."]
        pub trait $trait_name : $crate::Actor<Envelope = $enum_name> $( + $crate::Handler<$message_type> )* { }
        impl $enum_name {
            /// Runs the actor handling messages as they arrive.
            pub async fn run<A>(mut actor: A, mut receiver: $crate::Receiver<A::Envelope>, mut shutdown: impl ::std::future::Future<Output=()> + ::std::marker::Send + 'static)
                where A: $trait_name
            {
                let mut shutdown = Box::pin(shutdown);
                loop {
                    let delivery = tokio::select!{
                        // Check if we should shutdown, this is a graceful shutdown as we do not cancel
                        // any existing handle futures.
                        //
                        // This is an intentional design decision to avoid having to deal with cancel
                        // safety in handle implementations.
                        _ = &mut shutdown => {break;}
                        Some(delivery) = receiver.recv() => {delivery}
                    };
                    match delivery {
                        $crate::DeliverOp::Send(traced) => {
                            match traced.value {
                                $(
                                    $enum_name::$variant_name(m, respond_to) => {
                                        if respond_to.send($crate::tracing::Instrument::instrument(
                                            actor.handle(m),
                                            traced.span,
                                        ).await).is_err() {
                                            ::tracing::warn!("failed to send message to actor");
                                        }
                                    }
                                )*
                            }
                        }
                        $crate::DeliverOp::Notify(traced) => {
                            match traced.value {
                                $(
                                    $enum_name::$variant_name(m, _respond_to) => {
                                        // When notifying we do not care about the response, drop
                                        // it.
                                        let _ = $crate::tracing::Instrument::instrument(
                                            actor.handle(m),
                                            traced.span,
                                        ).await;
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
        impl std::convert::TryFrom<$enum_name> for $message_type {
            type Error = ();
            /// Extracts the message from the envelope.
            ///
            /// # Panics
            /// Panics if the envelope does not contain a message of the expected type.
            fn try_from(value: $enum_name) -> ::std::result::Result<Self,Self::Error> {
                match value {
                    $enum_name::$variant_name(msg, _) => Ok(msg),
                    _ => Err(())
                }
            }
        }
        )*
    };
}