use proc_macro::TokenStream;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

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

#[proc_macro_derive(TcpMessageLabel)]
pub fn derive_tcp_message_label(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = input.ident;
    let trait_path = tcp_message_label_trait_path();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let body = match input.data {
        Data::Struct(_) => quote! { stringify!(#name) },
        Data::Enum(data_enum) => {
            let mut arms = Vec::with_capacity(data_enum.variants.len());
            for variant in &data_enum.variants {
                let v_ident = &variant.ident;
                let pat = match &variant.fields {
                    Fields::Unit => quote! { Self::#v_ident },
                    Fields::Unnamed(_) => quote! { Self::#v_ident(..) },
                    Fields::Named(_) => quote! { Self::#v_ident { .. } },
                };

                arms.push(quote! {
                    #pat => concat!(stringify!(#name), "::", stringify!(#v_ident))
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
