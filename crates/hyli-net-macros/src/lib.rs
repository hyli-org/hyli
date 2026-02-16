use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::Parser, parse_macro_input, parse_quote, punctuated::Punctuated, GenericParam, ImplItem,
    ImplItemFn, ItemImpl, Meta, Token, Type, TypeParamBound, WherePredicate,
};

fn has_method(item: &ItemImpl, name: &str) -> bool {
    item.items.iter().any(|it| {
        if let ImplItem::Fn(f) = it {
            f.sig.ident == name
        } else {
            false
        }
    })
}

fn parse_mode_and_event_out(attr: TokenStream) -> syn::Result<(String, Option<Type>)> {
    let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
    let metas = parser.parse2(attr.into())?;
    let mut mode: Option<String> = None;
    let mut event_out: Option<Type> = None;

    for meta in metas {
        match meta {
            Meta::Path(path) => {
                let Some(seg) = path.segments.last() else {
                    continue;
                };
                let ident = seg.ident.to_string();
                if ident == "inbound" || ident == "outbound_tick" || ident == "all" {
                    mode = Some(ident);
                } else {
                    return Err(syn::Error::new_spanned(
                        path,
                        "unknown mode: expected `inbound`, `outbound_tick`, or `all`",
                    ));
                }
            }
            Meta::List(list) if list.path.is_ident("event_out") => {
                event_out = Some(syn::parse2::<Type>(list.tokens)?);
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "invalid attribute args: expected `inbound`, `outbound_tick`, or `all`, optionally `event_out(Type)`",
                ));
            }
        }
    }

    let Some(mode) = mode else {
        return Err(syn::Error::new_spanned(
            quote!(tcp_middleware),
            "missing mode: use `inbound`, `outbound_tick`, or `all`",
        ));
    };

    Ok((mode, event_out))
}

fn add_req_res_bounds(generics: &mut syn::Generics, req: &syn::Ident, res: &syn::Ident) {
    let where_clause = generics.make_where_clause();
    where_clause.predicates.push(parse_quote!(
        #req: borsh::BorshSerialize
            + borsh::BorshDeserialize
            + std::fmt::Debug
            + Send
            + crate::tcp::TcpMessageLabel
            + 'static
    ));
    where_clause.predicates.push(parse_quote!(
        #res: borsh::BorshSerialize + borsh::BorshDeserialize + std::fmt::Debug + crate::tcp::TcpMessageLabel
    ));
}

fn has_type_param(generics: &syn::Generics, name: &str) -> bool {
    generics.params.iter().any(|p| match p {
        GenericParam::Type(t) => t.ident == name,
        _ => false,
    })
}

fn has_declared_bound_for_type(item_impl: &ItemImpl, name: &str) -> bool {
    // Generic parameter bound: `impl<T: Bound> ...`
    let in_params = item_impl.generics.params.iter().any(|p| match p {
        GenericParam::Type(t) if t.ident == name => !t.bounds.is_empty(),
        _ => false,
    });
    if in_params {
        return true;
    }

    // Where-clause bound: `where T: Bound`
    let Some(where_clause) = &item_impl.generics.where_clause else {
        return false;
    };

    where_clause.predicates.iter().any(|pred| match pred {
        WherePredicate::Type(ty_pred) => {
            let is_target_type = if let Type::Path(type_path) = &ty_pred.bounded_ty {
                type_path.qself.is_none()
                    && type_path.path.segments.len() == 1
                    && type_path.path.segments[0].ident == name
            } else {
                false
            };
            let has_any_bound = ty_pred
                .bounds
                .iter()
                .any(|b| matches!(b, TypeParamBound::Trait(_) | TypeParamBound::Lifetime(_)));
            is_target_type && has_any_bound
        }
        _ => false,
    })
}

fn maybe_add_default_impl_req_bound(item_impl: &mut ItemImpl, req: &syn::Ident) {
    if has_declared_bound_for_type(item_impl, &req.to_string()) {
        return;
    }
    let where_clause = item_impl.generics.make_where_clause();
    where_clause.predicates.push(parse_quote!(
        #req: crate::tcp::middleware::TcpReqBound
    ));
}

fn maybe_add_default_impl_res_bound(item_impl: &mut ItemImpl, res: &syn::Ident) {
    if has_declared_bound_for_type(item_impl, &res.to_string()) {
        return;
    }
    let where_clause = item_impl.generics.make_where_clause();
    where_clause.predicates.push(parse_quote!(
        #res: crate::tcp::middleware::TcpResBound
    ));
}

fn method_has_type_param(method: &ImplItemFn, name: &str) -> bool {
    method.sig.generics.params.iter().any(|p| match p {
        GenericParam::Type(t) => t.ident == name,
        _ => false,
    })
}

fn add_req_bound_if_present(method: &mut ImplItemFn, req: &syn::Ident) {
    if !method_has_type_param(method, &req.to_string()) {
        return;
    }
    let where_clause = method.sig.generics.make_where_clause();
    where_clause.predicates.push(parse_quote!(
        #req: borsh::BorshSerialize
            + borsh::BorshDeserialize
            + std::fmt::Debug
            + Send
            + crate::tcp::TcpMessageLabel
            + 'static
    ));
}

fn add_res_bound_if_present(method: &mut ImplItemFn, res: &syn::Ident) {
    if !method_has_type_param(method, &res.to_string()) {
        return;
    }
    let where_clause = method.sig.generics.make_where_clause();
    where_clause.predicates.push(parse_quote!(
        #res: borsh::BorshSerialize + borsh::BorshDeserialize + std::fmt::Debug + crate::tcp::TcpMessageLabel
    ));
}

fn add_s_bound_if_present(
    method: &mut ImplItemFn,
    s: &syn::Ident,
    req: &syn::Ident,
    res: &syn::Ident,
) {
    if !method_has_type_param(method, &s.to_string()) {
        return;
    }
    let where_clause = method.sig.generics.make_where_clause();
    where_clause.predicates.push(parse_quote!(
        #s: crate::tcp::middleware::TcpServerLike<#req, #res, EventOut = crate::tcp::TcpEvent<#req>>
    ));
}

fn inject_hook_bounds(
    item_impl: &mut ItemImpl,
    mode: &str,
    req_ident: &syn::Ident,
    res_ident: &syn::Ident,
) {
    let s_ident = format_ident!("S");
    for item in &mut item_impl.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };

        let name = method.sig.ident.to_string();
        if !matches!(name.as_str(), "inbound" | "outbound_error" | "tick") {
            continue;
        }

        match mode {
            "inbound" => {
                add_req_bound_if_present(method, req_ident);
                add_res_bound_if_present(method, res_ident);
                add_s_bound_if_present(method, &s_ident, req_ident, res_ident);
            }
            "outbound_tick" => {
                add_req_bound_if_present(method, req_ident);
                add_s_bound_if_present(method, &s_ident, req_ident, res_ident);
            }
            "all" => {
                add_s_bound_if_present(method, &s_ident, req_ident, res_ident);
            }
            _ => {}
        }
    }
}

#[proc_macro_attribute]
pub fn tcp_middleware(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut item_impl = parse_macro_input!(item as ItemImpl);
    if item_impl.trait_.is_some() {
        return syn::Error::new_spanned(
            &item_impl,
            "#[tcp_middleware(...)] must be used on an inherent impl block",
        )
        .to_compile_error()
        .into();
    }

    let (mode, event_out) = match parse_mode_and_event_out(attr) {
        Ok(parsed) => parsed,
        Err(err) => return err.to_compile_error().into(),
    };

    let self_ty = item_impl.self_ty.clone();
    let req_ident = format_ident!("__TcpReq");
    let res_ident = format_ident!("__TcpRes");

    match mode.as_str() {
        "inbound" => {
            let req_method_ident = format_ident!("Req");
            let res_method_ident = format_ident!("Res");
            inject_hook_bounds(
                &mut item_impl,
                "inbound",
                &req_method_ident,
                &res_method_ident,
            );

            let Some(event_out) = event_out else {
                return syn::Error::new_spanned(
                    &item_impl,
                    "missing `event_out(Type)` for inbound mode, e.g. #[tcp_middleware(inbound, event_out(TcpEvent<__TcpReq>))]",
                )
                .to_compile_error()
                .into();
            };

            let mut in_g = item_impl.generics.clone();
            in_g.params.push(parse_quote!(#req_ident));
            in_g.params.push(parse_quote!(#res_ident));
            add_req_res_bounds(&mut in_g, &req_ident, &res_ident);
            let (in_impl_g, _, in_where) = in_g.split_for_impl();

            let mut out_g = item_impl.generics.clone();
            out_g.params.push(parse_quote!(#req_ident));
            out_g.params.push(parse_quote!(#res_ident));
            add_req_res_bounds(&mut out_g, &req_ident, &res_ident);
            let (out_impl_g, _, out_where) = out_g.split_for_impl();

            let mut tick_g = item_impl.generics.clone();
            tick_g.params.push(parse_quote!(#req_ident));
            tick_g.params.push(parse_quote!(#res_ident));
            add_req_res_bounds(&mut tick_g, &req_ident, &res_ident);
            let (tick_impl_g, _, tick_where) = tick_g.split_for_impl();

            quote! {
                #item_impl

                impl #in_impl_g crate::tcp::middleware::TcpInboundMiddleware<#req_ident, #res_ident> for #self_ty #in_where {
                    type EventOut = #event_out;

                    fn on_event<S>(&mut self, server: &mut S, event: crate::tcp::TcpEvent<#req_ident>) -> Option<Self::EventOut>
                    where
                        S: crate::tcp::middleware::TcpServerLike<#req_ident, #res_ident, EventOut = crate::tcp::TcpEvent<#req_ident>>,
                    {
                        let mut cx = crate::tcp::middleware::InboundCx::<#req_ident, #res_ident, S>::new(server);
                        self.inbound(&mut cx, event)
                    }
                }

                impl #out_impl_g crate::tcp::middleware::TcpOutboundMiddleware<#req_ident, #res_ident> for #self_ty #out_where {}

                impl #tick_impl_g crate::tcp::middleware::TcpTickMiddleware<#req_ident, #res_ident> for #self_ty #tick_where {}
            }
            .into()
        }
        "outbound_tick" => {
            if !has_type_param(&item_impl.generics, "Res") {
                return syn::Error::new_spanned(
                    &item_impl,
                    "`outbound_tick`/`all` mode requires a `Res` type parameter on the impl (e.g. impl<Res> Type<Res>)",
                )
                .to_compile_error()
                .into();
            }

            let res_self_ident = format_ident!("Res");
            maybe_add_default_impl_res_bound(&mut item_impl, &res_self_ident);
            let req_method_ident = format_ident!("Req");
            inject_hook_bounds(
                &mut item_impl,
                "outbound_tick",
                &req_method_ident,
                &res_self_ident,
            );
            let has_outbound = has_method(&item_impl, "outbound_error");
            let has_tick = has_method(&item_impl, "tick");
            let has_next_wakeup = has_method(&item_impl, "next_wakeup");

            let mut in_g = item_impl.generics.clone();
            in_g.params.push(parse_quote!(#req_ident));
            add_req_res_bounds(&mut in_g, &req_ident, &res_self_ident);
            let (in_impl_g, _, in_where) = in_g.split_for_impl();

            let mut out_g = item_impl.generics.clone();
            out_g.params.push(parse_quote!(#req_ident));
            add_req_res_bounds(&mut out_g, &req_ident, &res_self_ident);
            let (out_impl_g, _, out_where) = out_g.split_for_impl();

            let mut tick_g = item_impl.generics.clone();
            tick_g.params.push(parse_quote!(#req_ident));
            add_req_res_bounds(&mut tick_g, &req_ident, &res_self_ident);
            let (tick_impl_g, _, tick_where) = tick_g.split_for_impl();

            let outbound_body = if has_outbound {
                quote! { self.outbound_error(&mut cx, ctx) }
            } else {
                quote! { crate::tcp::middleware::SendErrorOutcome::Unhandled(anyhow::anyhow!(ctx.error.to_string())) }
            };
            let tick_body = if has_tick {
                quote! { self.tick(&mut cx) }
            } else {
                quote! {}
            };
            let next_wakeup_body = if has_next_wakeup {
                quote! { self.next_wakeup() }
            } else {
                quote! { None }
            };

            quote! {
                #item_impl

                impl #in_impl_g crate::tcp::middleware::TcpInboundMiddleware<#req_ident, #res_self_ident> for #self_ty #in_where {
                    type EventOut = crate::tcp::TcpEvent<#req_ident>;

                    fn on_event<S>(&mut self, server: &mut S, event: crate::tcp::TcpEvent<#req_ident>) -> Option<Self::EventOut>
                    where
                        S: crate::tcp::middleware::TcpServerLike<#req_ident, #res_self_ident, EventOut = crate::tcp::TcpEvent<#req_ident>>,
                    {
                        Some(event)
                    }
                }

                impl #out_impl_g crate::tcp::middleware::TcpOutboundMiddleware<#req_ident, #res_self_ident> for #self_ty #out_where {
                    fn on_send_error<S>(&mut self, server: &mut S, ctx: &crate::tcp::middleware::SendErrorContext<#res_self_ident>) -> crate::tcp::middleware::SendErrorOutcome
                    where
                        S: crate::tcp::middleware::TcpServerLike<#req_ident, #res_self_ident, EventOut = crate::tcp::TcpEvent<#req_ident>>,
                    {
                        let mut cx = crate::tcp::middleware::OutboundCx::<#req_ident, #res_self_ident, S>::new(server);
                        #outbound_body
                    }
                }

                impl #tick_impl_g crate::tcp::middleware::TcpTickMiddleware<#req_ident, #res_self_ident> for #self_ty #tick_where {
                    fn on_tick<S>(&mut self, server: &mut S)
                    where
                        S: crate::tcp::middleware::TcpServerLike<#req_ident, #res_self_ident, EventOut = crate::tcp::TcpEvent<#req_ident>>,
                    {
                        let mut cx = crate::tcp::middleware::TickCx::<#req_ident, #res_self_ident, S>::new(server);
                        #tick_body
                    }

                    fn next_wakeup(&self) -> Option<tokio::time::Instant> {
                        #next_wakeup_body
                    }
                }
            }
            .into()
        }
        "all" => {
            if !has_type_param(&item_impl.generics, "Req") || !has_type_param(&item_impl.generics, "Res") {
                return syn::Error::new_spanned(
                    &item_impl,
                    "`all` mode requires `Req` and `Res` type parameters on the impl (e.g. impl<Req, Res> Type<Req, Res>)",
                )
                .to_compile_error()
                .into();
            }

            let Some(event_out_ty) = event_out else {
                return syn::Error::new_spanned(
                    &item_impl,
                    "missing `event_out(Type)` for all mode, e.g. #[tcp_middleware(all, event_out(TcpInboundMessage<Req>))]",
                )
                .to_compile_error()
                .into();
            };

            let has_inbound = has_method(&item_impl, "inbound");
            let has_outbound = has_method(&item_impl, "outbound_error");
            let has_tick = has_method(&item_impl, "tick");
            let has_next_wakeup = has_method(&item_impl, "next_wakeup");

            let req_self_ident = format_ident!("Req");
            let res_self_ident = format_ident!("Res");
            maybe_add_default_impl_req_bound(&mut item_impl, &req_self_ident);
            maybe_add_default_impl_res_bound(&mut item_impl, &res_self_ident);
            inject_hook_bounds(&mut item_impl, "all", &req_self_ident, &res_self_ident);

            let mut all_g = item_impl.generics.clone();
            add_req_res_bounds(&mut all_g, &req_self_ident, &res_self_ident);
            let (all_impl_g, _, all_where) = all_g.split_for_impl();

            let inbound_body = if has_inbound {
                quote! {
                    let mut cx = crate::tcp::middleware::InboundCx::<Req, Res, S>::new(server);
                    self.inbound(&mut cx, event)
                }
            } else {
                quote! { Some(event) }
            };
            let outbound_body = if has_outbound {
                quote! { self.outbound_error(&mut cx, ctx) }
            } else {
                quote! { crate::tcp::middleware::SendErrorOutcome::Unhandled(anyhow::anyhow!(ctx.error.to_string())) }
            };
            let tick_body = if has_tick {
                quote! { self.tick(&mut cx) }
            } else {
                quote! {}
            };
            let next_wakeup_body = if has_next_wakeup {
                quote! { self.next_wakeup() }
            } else {
                quote! { None }
            };

            quote! {
                #item_impl

                impl #all_impl_g crate::tcp::middleware::TcpInboundMiddleware<Req, Res> for #self_ty #all_where {
                    type EventOut = #event_out_ty;

                    fn on_event<S>(&mut self, server: &mut S, event: crate::tcp::TcpEvent<Req>) -> Option<Self::EventOut>
                    where
                        S: crate::tcp::middleware::TcpServerLike<Req, Res, EventOut = crate::tcp::TcpEvent<Req>>,
                    {
                        #inbound_body
                    }
                }

                impl #all_impl_g crate::tcp::middleware::TcpOutboundMiddleware<Req, Res> for #self_ty #all_where {
                    fn on_send_error<S>(&mut self, server: &mut S, ctx: &crate::tcp::middleware::SendErrorContext<Res>) -> crate::tcp::middleware::SendErrorOutcome
                    where
                        S: crate::tcp::middleware::TcpServerLike<Req, Res, EventOut = crate::tcp::TcpEvent<Req>>,
                    {
                        let mut cx = crate::tcp::middleware::OutboundCx::<Req, Res, S>::new(server);
                        #outbound_body
                    }
                }

                impl #all_impl_g crate::tcp::middleware::TcpTickMiddleware<Req, Res> for #self_ty #all_where {
                    fn on_tick<S>(&mut self, server: &mut S)
                    where
                        S: crate::tcp::middleware::TcpServerLike<Req, Res, EventOut = crate::tcp::TcpEvent<Req>>,
                    {
                        let mut cx = crate::tcp::middleware::TickCx::<Req, Res, S>::new(server);
                        #tick_body
                    }

                    fn next_wakeup(&self) -> Option<tokio::time::Instant> {
                        #next_wakeup_body
                    }
                }
            }
            .into()
        }
        _ => syn::Error::new_spanned(
            &item_impl,
            format!("unknown tcp_middleware mode `{mode}`: expected `inbound`, `outbound_tick`, or `all`"),
        )
        .to_compile_error()
        .into(),
    }
}
