use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Fields, GenericParam, Lit};

#[proc_macro_derive(Actor, attributes(actor))]
pub fn actor(item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as DeriveInput);
    // Extract struct name
    let struct_name = item.ident;

    let Config {
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
    // Ensure the input is a struct and extract its fields
    let fields = if let Data::Struct(data_struct) = item.data {
        match data_struct.fields {
            Fields::Named(named_fields) => named_fields.named,
            _ => panic!("#[actor] only supports structs with named fields."),
        }
    } else {
        panic!("#[actor] can only be used with structs.");
    };

    // Extract field names
    let field_names: Vec<_> = fields.iter().map(|field| &field.ident).collect();

    // Generate the implementation
    let expanded = quote! {
        impl #generics ceramic_actor::Actor for #struct_name < #(#generic_types,)*> {
            type Envelope = #envelope_name;
        }

        impl #generics #struct_name < #(#generic_types,)*> {
            /// Start the actor returning a handle that can be easily cloned and shared.
            /// The actor stops once all handles are dropped.
            pub fn spawn(size: usize, #fields) -> #handle_name < #(#generic_types,)*> {
                let (sender, receiver) = ceramic_actor::channel(size);
                let actor = #struct_name {
                    #(#field_names,)*
                };
                tokio::spawn(async move { <#struct_name  < #(#generic_types,)*> as ceramic_actor::Actor>::Envelope::run(actor, receiver).await });

                #handle_name {
                    sender,
                    #(#phantom_values,)*
                }
            }
        }

        pub struct #handle_name #generics {
            sender: ceramic_actor::Sender<<#struct_name  < #(#generic_types,)*> as ceramic_actor::Actor>::Envelope>,
            #(#phantom_fields,)*
        }
        impl #generics ::core::clone::Clone for #handle_name < #(#generic_types,)*> {
            fn clone(&self) -> Self {
                Self{
                    sender:self.sender.clone(),
                    #(#phantom_values,)*
                }
            }
        }

        #[async_trait::async_trait]
        impl #generics ceramic_actor::ActorHandle<#struct_name < #(#generic_types,)*>> for #handle_name < #(#generic_types,)*> {
            fn sender(&self) -> ceramic_actor::Sender<<#struct_name < #(#generic_types,)*> as ceramic_actor::Actor>::Envelope> {
                self.sender.clone()
            }
        }

    };

    TokenStream::from(expanded)
}

struct Config {
    envelope_name: syn::Ident,
    handle_name: syn::Ident,
}

impl Config {
    fn from_attributes(struct_name: &syn::Ident, attrs: &[Attribute]) -> Self {
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
                    }
                    Ok(())
                })
                .expect("should be able to parse attributes");
            }
        }
        Self {
            envelope_name,
            handle_name,
        }
    }
}
