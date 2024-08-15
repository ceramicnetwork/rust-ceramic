use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(SerdeIpld)]
pub fn derive_serde_ipld(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let name = &ast.ident;

    let expanded = quote! {
            // impl MacroTrait for #name // should we make a trait?
            impl #name {
                /// serde serialize to dag-json
                pub fn to_json(&self) -> Result<String, Box<dyn std::error::Error>> {
                    let json_bytes = serde_ipld_dagjson::to_vec(self)?;
                    String::from_utf8(json_bytes).map_err(Into::into)
                }

                /// serde serialize to dag-cbor
                pub fn to_cbor(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
                    serde_ipld_dagcbor::to_vec(self).map_err(Into::into)
                }

                /// CID of serde serialize to dag-cbor
                pub fn to_cid(&self) -> Result<Cid, Box<dyn std::error::Error>> {
                    let cbor_bytes = self.to_cbor()?;
    use multihash_codetable::{Code, MultihashDigest};
                    let hash = multihash_codetable::Code::Sha2_256.digest(&cbor_bytes);
                    Ok(Cid::new_v1(0x71, hash))
                }
            }
        };
    expanded.into()
}
