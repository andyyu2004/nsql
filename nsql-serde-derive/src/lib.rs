use quote::quote;

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

fn preprocess(input: syn::DeriveInput) -> Data {
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

#[proc_macro_derive(StreamSerialize, attributes(serde))]
pub fn derive_serialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input);
    let mut data = preprocess(input);

    let type_params = data.generics.type_params();
    let extended_predicates = syn::parse::<syn::WhereClause>(
        quote! {
            where #(#type_params: ::nsql_serde::StreamSerialize),*
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
                use ::nsql_serde::StreamSerialize as _;
                #(
                    <#ty as ::nsql_serde::StreamSerialize>::serialize(&self.#field_name, ser).await?;
                )*
                Ok(())
            }
        }
        DataKind::Enum { variants } => {
            let variant_name = variants.iter().map(|v| &v.ident);
            quote! {
                use ::nsql_serde::StreamSerialize as _;
                match self {
                    #(
                        Self::#variant_name(x) => ::nsql_serde::StreamSerialize::serialize(x, ser).await?,
                    )*
                    _ => (),
                }
                Ok(())
            }
        }
    };

    quote! {
        impl #impl_generics ::nsql_serde::StreamSerialize for #name #ty_generics #where_clause {
            async fn serialize<S: ::nsql_serde::StreamSerializer>(&self, ser: &mut S) -> ::nsql_serde::Result<()> {
                #body
            }
        }
    }.into()
}

#[proc_macro_derive(StreamDeserialize, attributes(serde))]
pub fn derive_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let mut data = preprocess(input);
    let name = data.name;

    let type_params = data.generics.type_params();
    let extended_predicates = syn::parse::<syn::WhereClause>(
        quote! {
        where #(#type_params: ::nsql_serde::StreamDeserialize),*
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
                impl #impl_generics ::nsql_serde::StreamDeserialize for #name #ty_generics #where_clause {
                    async fn deserialize<D: ::nsql_serde::StreamDeserializer>(de: &mut D) -> ::nsql_serde::Result<Self> {
                        use ::nsql_serde::StreamDeserialize as _;
                        #(
                            let #field_name = <#ty as ::nsql_serde::StreamDeserialize>::deserialize(de).await?;
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

// FIXME remove this?
#[proc_macro_derive(SerializeSized, attributes(serde))]
pub fn derive_serialize_sized(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let serialize_impl = proc_macro2::TokenStream::from(derive_serialize(input.clone()));

    let input = syn::parse_macro_input!(input as syn::DeriveInput);
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
            let ty = fields.iter().map(|field| &field.ty);
            quote! {
                const SERIALIZED_SIZE: u16 =  0 #(+ <#ty as ::nsql_serde::SerializeSized>::SERIALIZED_SIZE)*;
            }
        }
        DataKind::Enum { .. } => todo!(),
    };

    quote! {
        #serialize_impl

        impl #impl_generics ::nsql_serde::SerializeSized for #name #ty_generics #where_clause {
            #body
        }
    }
    .into()
}
