use proc_macro::TokenStream;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Error, Fields, LitStr};

fn tcp_message_label_trait_path() -> proc_macro2::TokenStream {
    match crate_name("hyli-net-traits") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_) => match crate_name("hyli-net") {
            Ok(FoundCrate::Itself) => quote!(crate::tcp),
            Ok(FoundCrate::Name(name)) => {
                let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
                quote!(::#ident::tcp)
            }
            Err(_) => quote!(::hyli_net_traits),
        },
    }
}

#[derive(Default)]
struct TcpLabelOpts {
    prefix: Option<LitStr>,
    label: Option<LitStr>,
    suffix: Option<LitStr>,
}

fn parse_tcp_label_opts(
    attrs: &[syn::Attribute],
    allow_prefix: bool,
    allow_label: bool,
    allow_suffix: bool,
) -> syn::Result<TcpLabelOpts> {
    let mut opts = TcpLabelOpts::default();

    for attr in attrs {
        if !attr.path().is_ident("tcp_label") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                if !allow_prefix {
                    return Err(meta.error("`prefix` is not allowed here"));
                }
                let value: LitStr = meta.value()?.parse()?;
                if opts.prefix.replace(value).is_some() {
                    return Err(meta.error("duplicate `prefix`"));
                }
                return Ok(());
            }

            if meta.path.is_ident("label") {
                if !allow_label {
                    return Err(meta.error("`label` is not allowed here"));
                }
                let value: LitStr = meta.value()?.parse()?;
                if opts.label.replace(value).is_some() {
                    return Err(meta.error("duplicate `label`"));
                }
                return Ok(());
            }

            if meta.path.is_ident("suffix") {
                if !allow_suffix {
                    return Err(meta.error("`suffix` is not allowed here"));
                }
                let value: LitStr = meta.value()?.parse()?;
                if opts.suffix.replace(value).is_some() {
                    return Err(meta.error("duplicate `suffix`"));
                }
                return Ok(());
            }

            Err(meta.error("unknown tcp_label option: expected `prefix`, `label`, or `suffix`"))
        })?;
    }

    Ok(opts)
}

#[proc_macro_derive(TcpMessageLabel, attributes(tcp_label))]
pub fn derive_tcp_message_label(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = input.ident;
    let trait_path = tcp_message_label_trait_path();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let type_opts = match parse_tcp_label_opts(&input.attrs, true, true, false) {
        Ok(opts) => opts,
        Err(err) => return err.to_compile_error().into(),
    };

    let body = match input.data {
        Data::Struct(_) => match (type_opts.label.as_ref(), type_opts.prefix.as_ref()) {
            (Some(label), _) => quote! { #label },
            (None, Some(prefix)) => quote! { concat!(#prefix, "::", stringify!(#name)) },
            (None, None) => quote! { stringify!(#name) },
        },
        Data::Enum(data_enum) => {
            let base_label = match (type_opts.label.as_ref(), type_opts.prefix.as_ref()) {
                (Some(label), _) => quote! { #label },
                (None, Some(prefix)) => quote! { concat!(#prefix, "::", stringify!(#name)) },
                (None, None) => quote! { stringify!(#name) },
            };

            let mut arms = Vec::with_capacity(data_enum.variants.len());
            for variant in &data_enum.variants {
                let v_ident = &variant.ident;
                let variant_opts = match parse_tcp_label_opts(&variant.attrs, false, true, true) {
                    Ok(opts) => opts,
                    Err(err) => return err.to_compile_error().into(),
                };

                if variant_opts.label.is_some() && variant_opts.suffix.is_some() {
                    return Error::new_spanned(
                        variant,
                        "only one of `label` or `suffix` can be set on a variant",
                    )
                    .to_compile_error()
                    .into();
                }

                let pat = match &variant.fields {
                    Fields::Unit => quote! { Self::#v_ident },
                    Fields::Unnamed(_) => quote! { Self::#v_ident(..) },
                    Fields::Named(_) => quote! { Self::#v_ident { .. } },
                };

                let label_expr = if let Some(label) = variant_opts.label.as_ref() {
                    quote! { #label }
                } else if let Some(suffix) = variant_opts.suffix.as_ref() {
                    quote! { concat!(#base_label, "::", #suffix) }
                } else {
                    quote! { concat!(#base_label, "::", stringify!(#v_ident)) }
                };

                arms.push(quote! {
                    #pat => #label_expr
                });
            }

            quote! {
                match self {
                    #( #arms, )*
                }
            }
        }
        Data::Union(_) => {
            return syn::Error::new_spanned(name, "TcpMessageLabel derive does not support unions")
                .to_compile_error()
                .into();
        }
    };

    quote! {
        impl #impl_generics #trait_path::TcpMessageLabel for #name #ty_generics #where_clause {
            fn message_label(&self) -> &'static str {
                #body
            }
        }
    }
    .into()
}
