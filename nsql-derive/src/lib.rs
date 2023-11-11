use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_derive(FromTuple)]
pub fn derive_from_tuple(input: TokenStream) -> TokenStream {
    let syn::DeriveInput { ident, data, .. } = syn::parse_macro_input!(input as syn::DeriveInput);

    let s = match data {
        syn::Data::Struct(s) => s,
        syn::Data::Enum(_) | syn::Data::Union(_) => panic!(),
    };

    let fields = match s.fields {
        syn::Fields::Named(fields) => fields.named,
        syn::Fields::Unnamed(_) | syn::Fields::Unit => panic!(),
    };

    let field_name = fields.iter().map(|field| &field.ident);

    quote! {
        impl ::nsql_storage::tuple::FromTuple for #ident {
            #[inline]
            fn from_values(mut values: impl Iterator<Item = ::nsql_storage::value::Value>) -> Result<Self, ::nsql_storage::tuple::FromFlatTupleError> {
                Ok(Self {
                    #(
                        #field_name: values.next().ok_or(::nsql_storage::tuple::FromFlatTupleError::NotEnoughValues)?.cast()?,
                    )*
                })
            }
        }
    }.into()
}

#[proc_macro_derive(IntoTuple)]
pub fn derive_into_tuple(input: TokenStream) -> TokenStream {
    let syn::DeriveInput { ident, data, .. } = syn::parse_macro_input!(input as syn::DeriveInput);

    let s = match data {
        syn::Data::Struct(s) => s,
        syn::Data::Enum(_) | syn::Data::Union(_) => panic!(),
    };

    let fields = match s.fields {
        syn::Fields::Named(fields) => fields.named,
        syn::Fields::Unnamed(_) | syn::Fields::Unit => panic!(),
    };

    let field_name = fields.iter().map(|field| &field.ident);

    quote! {
        impl ::nsql_storage::tuple::IntoFlatTuple for #ident {
            #[inline]
            fn into_tuple(self) -> ::nsql_storage::tuple::FlatTuple {
                ::nsql_storage::tuple::FlatTuple::from([
                    #(
                        self.#field_name.into(),
                    )*
                ])
            }
        }
    }
    .into()
}
