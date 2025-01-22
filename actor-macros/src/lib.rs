use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, DeriveInput, GenericParam, Lit};
/// Derive the `Actor` trait for the struct.
/// The struct must be a field struct, not a tuple struct.
///
/// The struct may have generic parameters, in which case the derived `ActorHandle` will have the
/// same generic parameters.
///
/// Lifetimes on the struct are not supported.
///
/// The `actor` attribute may be added with the following optional key value pairs:
///     - envelope: The name of the envelope type.
///     - handle: The name of the derived `ActorHandle` implementation.
///     - actor_trait: The name of the actor specific trait. This is the same as the second
///     argument to the actor_envelope! macro.
///     - recorder_trait: The name of the recorder trait. This is the thrid arugment to the
///     actor_envelope! macro.
///
/// # Example
/// ```
/// # use ceramic_actor::{Handler, Message};
/// use ceramic_actor::{Actor, actor_envelope};
///
/// #[derive(Actor)]
/// #[actor(envelope = "PlayerEnv", handle = "PlayerH", actor_trait = "PlayerI", recorder_trait = "PlayerR")]
/// pub struct Player { }
///
/// actor_envelope!{
///     PlayerEnv,
///     PlayerI,
///     PlayerR,
///     Score => ScoreMessage,
/// }
///
/// # #[async_trait::async_trait]
/// # impl Handler<ScoreMessage> for Player {
/// #     async fn handle(&mut self, message: ScoreMessage) -> <ScoreMessage as Message>::Result {
/// #       todo!()
/// #     }
/// # }
///
/// # #[derive(Debug)]
/// # struct ScoreMessage { }
/// # impl Message for ScoreMessage {
/// #     type Result = ();
/// # }
/// ```
#[proc_macro_derive(Actor, attributes(actor))]
pub fn actor(item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as DeriveInput);
    // Extract struct name
    let struct_name = item.ident;

    let Config {
        actor_trait,
        recorder_trait,
        envelope_name,
        handle_name,
    } = Config::from_attributes(&struct_name, &item.attrs);

    let generics = item.generics;
    let generic_types: Vec<_> = generics
        .params
        .iter()
        .filter_map(|param| match param {
            GenericParam::Type(type_param) => Some(&type_param.ident),
            _ => None,
        })
        .collect();
    let phantom_fields = generic_types.iter().map(|ty| {
        let name = syn::Ident::new(&format!("__{}", ty).to_lowercase(), ty.span());
        quote! {
            #name: std::marker::PhantomData<#ty>
        }
    });
    let phantom_values: Vec<_> = generic_types
        .iter()
        .map(|ty| {
            let name = syn::Ident::new(&format!("__{}", ty).to_lowercase(), ty.span());
            quote! {
                #name: Default::default()
            }
        })
        .collect();

    // Generate the implementation
    let expanded = quote! {
        impl #generics ceramic_actor::Actor for #struct_name < #(#generic_types,)*> {
            type Envelope = #envelope_name;
        }
        impl #generics #actor_trait for #struct_name < #(#generic_types,)*> { }

        impl #generics #struct_name < #(#generic_types,)*> {
            /// Start the actor returning a handle that can be easily cloned and shared.
            /// The actor stops once all handles are dropped.
            pub fn spawn(
                size: usize,
                actor: impl #actor_trait,
                recorder: impl #recorder_trait,
                shutdown: impl ::std::future::Future<Output=()> + ::std::marker::Send + 'static) -> (#handle_name < #(#generic_types,)*>, tokio::task::JoinHandle<()>,
            ) {
                let (sender, receiver) = ceramic_actor::channel(size);
                let task_handle = tokio::spawn(async move { #envelope_name::run(actor, receiver, shutdown).await });

                (
                    #handle_name {
                        sender,
                        recorder: ::std::sync::Arc::new(recorder),
                        #(#phantom_values,)*
                    },
                    task_handle,
                )
            }
        }

        #[doc = concat!("Handle for [`", stringify!(#actor_trait), "`].")]
        #[derive(Debug)]
        pub struct #handle_name #generics {
            sender: ceramic_actor::Sender<#envelope_name>,
            recorder: ::std::sync::Arc<dyn #recorder_trait>,
            #(#phantom_fields,)*
        }
        impl #generics ::core::clone::Clone for #handle_name < #(#generic_types,)*> {
            fn clone(&self) -> Self {
                Self{
                    sender: self.sender.clone(),
                    recorder: self.recorder.clone(),
                    #(#phantom_values,)*
                }
            }
        }

        #[async_trait::async_trait]
        impl #generics ceramic_actor::ActorHandle for #handle_name < #(#generic_types,)*> {
            type Actor = #struct_name < #(#generic_types,)*>;
            fn sender(&self) -> ceramic_actor::Sender<<#struct_name < #(#generic_types,)*> as ceramic_actor::Actor>::Envelope> {
                self.sender.clone()
            }
        }
        impl #handle_name {
            /// Notify the actor of the message. Do not wait for the response.
            /// Record the messsage event using the recorder provided to the handler at spawn.
            pub async fn notify<Msg>(&self, msg: Msg) -> ::std::result::Result<(), ::ceramic_actor::Error<Msg>>
            where
                Msg: ::ceramic_actor::Message
                    + ::std::convert::TryFrom<#envelope_name>
                    + ::std::fmt::Debug
                    + ::std::marker::Send
                    + 'static,
                <Msg as ::std::convert::TryFrom<#envelope_name>>::Error: ::std::fmt::Debug,
                #envelope_name:
                    ::std::convert::From<(Msg, ::tokio::sync::oneshot::Sender<Msg::Result>)>,
                dyn #recorder_trait: ::ceramic_metrics::Recorder<::ceramic_actor::MessageEvent<Msg>> + Send + 'static,
            {
                ::ceramic_actor::ActorHandle::notify(self, msg, self.recorder.clone()).await
            }
            /// Send a message to the actor waiting for the response.
            /// Record the messsage event using the recorder provided to the handler at spawn.
            pub async fn send<Msg>(&self, msg: Msg) -> ::std::result::Result<Msg::Result, ::ceramic_actor::Error<Msg>>
            where
                Msg: ::ceramic_actor::Message
                    + ::std::convert::TryFrom<#envelope_name>
                    + ::std::fmt::Debug
                    + ::std::marker::Send
                    + 'static,
                <Msg as ::std::convert::TryFrom<#envelope_name>>::Error: ::std::fmt::Debug,
                #envelope_name:
                    ::std::convert::From<(Msg, ::tokio::sync::oneshot::Sender<Msg::Result>)>,
                dyn #recorder_trait: ::ceramic_metrics::Recorder<::ceramic_actor::MessageEvent<Msg>> + Send + 'static,
            {
                ::ceramic_actor::ActorHandle::send(self, msg, self.recorder.clone()).await
            }
        }

    };

    TokenStream::from(expanded)
}

struct Config {
    actor_trait: syn::Ident,
    recorder_trait: syn::Ident,
    envelope_name: syn::Ident,
    handle_name: syn::Ident,
}

impl Config {
    fn from_attributes(struct_name: &syn::Ident, attrs: &[Attribute]) -> Self {
        let mut actor_trait = syn::Ident::new(&format!("{}Actor", struct_name), struct_name.span());
        let mut recorder_trait =
            syn::Ident::new(&format!("{}Recorder", struct_name), struct_name.span());
        let mut envelope_name =
            syn::Ident::new(&format!("{}Envelope", struct_name), struct_name.span());
        let mut handle_name =
            syn::Ident::new(&format!("{}Handle", struct_name), struct_name.span());
        for attr in attrs {
            if attr.path().is_ident("actor") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("envelope") {
                        let value: Lit = meta.value()?.parse()?;
                        if let Lit::Str(lit_str) = value {
                            envelope_name = syn::Ident::new(&lit_str.value(), lit_str.span())
                        }
                    } else if meta.path.is_ident("handle") {
                        let value: Lit = meta.value()?.parse()?;
                        if let Lit::Str(lit_str) = value {
                            handle_name = syn::Ident::new(&lit_str.value(), lit_str.span())
                        }
                    } else if meta.path.is_ident("actor_trait") {
                        let value: Lit = meta.value()?.parse()?;
                        if let Lit::Str(lit_str) = value {
                            actor_trait = syn::Ident::new(&lit_str.value(), lit_str.span())
                        }
                    } else if meta.path.is_ident("recorder_trait") {
                        let value: Lit = meta.value()?.parse()?;
                        if let Lit::Str(lit_str) = value {
                            recorder_trait = syn::Ident::new(&lit_str.value(), lit_str.span())
                        }
                    }
                    Ok(())
                })
                .expect("should be able to parse attributes");
            }
        }
        Self {
            actor_trait,
            recorder_trait,
            envelope_name,
            handle_name,
        }
    }
}
