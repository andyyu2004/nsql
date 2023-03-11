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

struct Data {
    name: syn::Ident,
    kind: DataKind,
    generics: syn::Generics,
}

enum DataKind {
    Struct { fields: Vec<syn::Field> },
    Enum { variants: Vec<syn::Variant> },
}

fn preprocess(input: DeriveInput) -> Data {
    let kind = match input.data {
        syn::Data::Struct(strukt) => {
            let fields = strukt.fields;
            let fields = match fields {
                syn::Fields::Named(fields) => fields.named,
                syn::Fields::Unnamed(fields) => fields.unnamed,
                syn::Fields::Unit => todo!("serialize unit struct"),
            };
            let fields = fields.into_iter().filter(|f| filter_skip(&f.attrs)).collect::<Vec<_>>();
            DataKind::Struct { fields }
        }
        syn::Data::Enum(data) => {
            let variants = data.variants.into_iter().filter(|v| filter_skip(&v.attrs)).collect();
            DataKind::Enum { variants }
        }
        syn::Data::Union(_) => panic!("Unions are not supported"),
    };

    Data { name: input.ident, kind, generics: input.generics }
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
    let mut data = preprocess(input);

    let type_params = data.generics.type_params();
    let extended_predicates = syn::parse::<syn::WhereClause>(
        quote! {
            where #(#type_params: ::nsql_serde::Serialize),*
        }
        .into(),
    )
    .expect("should always be parseable")
    .predicates;
    data.generics.make_where_clause().predicates.extend(extended_predicates);

    let (impl_generics, ty_generics, where_clause) = data.generics.split_for_impl();

    let name = data.name;
    let body = match data.kind {
        DataKind::Struct { fields } => {
            let field_name = fields.iter().map(|f| f.ident.as_ref().unwrap());
            let ty = fields.iter().map(|field| &field.ty);
            quote! {
                use ::nsql_serde::Serialize as _;
                #(
                    <#ty as ::nsql_serde::Serialize>::serialize(&self.#field_name, ser).await?;
                )*
                Ok(())
            }
        }
        DataKind::Enum { variants } => {
            let variant_name = variants.iter().map(|v| &v.ident);
            quote! {
                use ::nsql_serde::Serialize as _;
                match self {
                    #(
                        Self::#variant_name(x) => ::nsql_serde::Serialize::serialize(x, ser).await?,
                    )*
                    _ => (),
                }
                Ok(())
            }
        }
    };

    quote! {
        impl #impl_generics ::nsql_serde::Serialize for #name #ty_generics #where_clause {
            async fn serialize(&self, ser: &mut dyn ::nsql_serde::Serializer) -> ::nsql_serde::Result<()> {
                #body
            }
        }
    }.into()
}

#[proc_macro_derive(Deserialize, attributes(serde))]
pub fn derive_deserialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let mut data = preprocess(input);
    let name = data.name;

    let type_params = data.generics.type_params();
    let extended_predicates = syn::parse::<syn::WhereClause>(
        quote! {
        where #(#type_params: ::nsql_serde::Deserialize),*
        }
        .into(),
    )
    .expect("should always be parseable")
    .predicates;
    data.generics.make_where_clause().predicates.extend(extended_predicates);
    let (impl_generics, ty_generics, where_clause) = data.generics.split_for_impl();

    match data.kind {
        DataKind::Struct {  fields } => {
            let field_name = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect::<Vec<_>>();
            let ty = fields.iter().map(|field| &field.ty);
            quote! {
                impl #impl_generics ::nsql_serde::Deserialize for #name #ty_generics #where_clause {
                    async fn deserialize(de: &mut dyn ::nsql_serde::Deserializer<'_>) -> ::nsql_serde::Result<Self> {
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
        DataKind::Enum { .. } => panic!("Enums are not implemented"),
    }
    .into()
}
