use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[derive(Copy, Clone)]
struct Symbol(&'static str);

const SERDE: Symbol = Symbol("serde");
const SKIP: Symbol = Symbol("skip");

impl PartialEq<Symbol> for syn::Path {
    fn eq(&self, word: &Symbol) -> bool {
        self.is_ident(word.0)
    }
}

enum Data {
    Struct { name: syn::Ident, fields: Vec<syn::Field> },
    Enum { name: syn::Ident, variants: Vec<syn::Variant> },
}

fn preprocess(input: DeriveInput) -> Data {
    match input.data {
        syn::Data::Struct(strukt) => {
            let name = input.ident;
            let fields = strukt.fields;
            let fields = match fields {
                syn::Fields::Named(fields) => fields.named,
                syn::Fields::Unnamed(fields) => fields.unnamed,
                syn::Fields::Unit => todo!("serialize unit struct"),
            };
            let fields = fields.into_iter().filter(|f| filter_skip(&f.attrs)).collect::<Vec<_>>();
            Data::Struct { name, fields }
        }
        syn::Data::Enum(data) => {
            let name = input.ident;
            let variants = data.variants.into_iter().filter(|v| filter_skip(&v.attrs)).collect();
            Data::Enum { name, variants }
        }
        syn::Data::Union(_) => panic!("Unions are not supported"),
    }
}

fn filter_skip(attrs: &[syn::Attribute]) -> bool {
    !attrs.iter().flat_map(get_serde_meta_items).any(|meta| {
        if let syn::NestedMeta::Meta(syn::Meta::Path(path)) = meta { path == SKIP } else { false }
    })
}

fn get_serde_meta_items(attr: &syn::Attribute) -> Vec<syn::NestedMeta> {
    if attr.path != SERDE {
        return Vec::new();
    }

    match attr.parse_meta() {
        Ok(syn::Meta::List(meta)) => meta.nested.into_iter().collect(),
        Ok(_other) => panic!("expected #[serde(...)]"),
        Err(err) => panic!("error parsing serde attribute: {err}"),
    }
}

#[proc_macro_derive(Serialize, attributes(serde))]
pub fn derive_serialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match preprocess(input) {
        Data::Struct { name, fields } => {
            let field_name = fields.iter().map(|f| f.ident.as_ref().unwrap());
            let ty = fields.iter().map(|field| &field.ty);
            quote! {
                impl ::nsql_serde::Serialize for #name {
                    async fn serialize(&self, ser: &mut dyn ::nsql_serde::Serializer<'_>) -> ::std::result::Result<(), Self::Error> {
                        use ::nsql_serde::Serialize as _;
                        #(
                            <#ty as ::nsql_serde::Serialize>::serialize(&self.#field_name, ser).await?;
                        )*
                        Ok(())
                    }
                }
            }.into()
        }
        Data::Enum { name, variants } => {
            let variant_name = variants.iter().map(|v| &v.ident);
            quote! {
                impl ::nsql_serde::Serialize for #name {
                    async fn serialize(&self, serializer: &mut dyn ::nsql_serde::Serializer<'_>) -> ::std::result::Result<(), Self::Error> {
                        use ::nsql_serde::Serialize as _;
                        match self {
                            #(
                                Self::#variant_name(x) => ::nsql_serde::Serialize::serialize(x, serializer).await?,
                            )*
                            _ => (),
                        }
                        Ok(())
                    }
                }
            }.into()
        }
    }
}

#[proc_macro_derive(Deserialize, attributes(serde))]
pub fn derive_deserialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match preprocess(input) {
        Data::Struct { name, fields } => {
            let field_name = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect::<Vec<_>>();
            let ty = fields.iter().map(|field| &field.ty);
            quote! {
                impl ::nsql_serde::Deserialize for #name {
                    async fn deserialize(de: &mut dyn ::nsql_serde::Deserializer<'_>) -> ::std::result::Result<Self, Self::Error> {
                        use ::nsql_serde::Deserialize as _;
                        #(
                            let #field_name = <#ty as ::nsql_serde::Deserialize>::deserialize(de).await?;
                        )*
                        Ok(Self {
                            #(#field_name),*
                        })
                    }
                }
            }
        }
        Data::Enum { .. } => panic!("Enums are not supported"),
    }
    .into()
}
