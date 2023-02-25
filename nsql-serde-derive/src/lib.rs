use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

struct Data {}

fn preprocess(
    input: DeriveInput,
) -> (syn::Ident, syn::punctuated::Punctuated<syn::Field, syn::token::Comma>) {
    match input.data {
        syn::Data::Struct(strukt) => {
            let name = input.ident;
            let fields = strukt.fields;
            let fields = match fields {
                syn::Fields::Named(fields) => fields.named,
                syn::Fields::Unnamed(fields) => fields.unnamed,
                syn::Fields::Unit => todo!("serialize unit struct"),
            };
            (name, fields)
        }
        syn::Data::Enum(_) => unimplemented!(),
        syn::Data::Union(_) => panic!("Unions are not supported"),
    }
}

#[proc_macro_derive(Serialize)]
pub fn derive_serialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let (name, fields) = preprocess(input);
    let field_name = fields.iter().map(|f| f.ident.as_ref().unwrap());
    quote! {
        impl ::nsql_serde::Serialize for #name {
            async fn serialize(&self, serializer: &mut dyn ::nsql_serde::Serializer<'_>) -> ::std::result::Result<(), Self::Error> {
                use ::nsql_serde::Serialize as _;
                #(
                    self.#field_name.serialize(serializer).await?;
                )*
                Ok(())
            }
        }
    }
    .into()
}

#[proc_macro_derive(SerializeSync)]
pub fn derive_serialize_sync(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let (name, fields) = preprocess(input);
    let field_name = fields.iter().map(|f| f.ident.as_ref().unwrap());

    quote! {
        impl ::nsql_serde::SerializeSync for #name {
            fn serialize_sync(&self, serializer: &mut dyn ::nsql_serde::BufMut) {
                use ::nsql_serde::SerializeSync as _;
                #(
                    self.#field_name.serialize_sync(serializer);
                )*
            }
        }
    }
    .into()
}

#[proc_macro_derive(Deserialize)]
pub fn derive_deserialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let (name, fields) = preprocess(input);
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
    .into()
}

#[proc_macro_derive(DeserializeSync)]
pub fn derive_deserialize_sync(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let (name, fields) = preprocess(input);
    let field_name = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect::<Vec<_>>();
    let ty = fields.iter().map(|field| &field.ty);

    quote! {
        impl ::nsql_serde::DeserializeSync for #name {
            fn deserialize_sync(de: &mut dyn ::nsql_serde::Buf) -> Self {
                use ::nsql_serde::DeserializeSync as _;
                #(
                    let #field_name = <#ty as ::nsql_serde::DeserializeSync>::deserialize_sync(de);
                )*
                Self {
                    #(#field_name),*
                }
            }
        }
    }
    .into()
}
