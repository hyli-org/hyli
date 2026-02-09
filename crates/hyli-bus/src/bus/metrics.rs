use std::{
    any::{TypeId, type_name},
    collections::HashMap,
};

use hyli_turmoil_shims::global_meter_or_panic;
use opentelemetry::{KeyValue, metrics::Counter};
use quote::ToTokens;
use syn::{Type, parse_str};

#[derive(Debug, Clone)]
pub struct BusMetrics {
    labels: HashMap<(TypeId, TypeId), [KeyValue; 2]>,
    send: Counter<u64>,
    receive: Counter<u64>,
    client_name: String,
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
impl BusMetrics {
    pub fn global() -> BusMetrics {
        let my_meter = global_meter_or_panic();

        BusMetrics {
            labels: HashMap::new(),
            send: my_meter.u64_counter("bus_send").build(),
            receive: my_meter.u64_counter("bus_receive").build(),
            client_name: String::new(),
        }
    }

    // Fonction pour simplifier le nom de type en utilisant `syn`
    fn simplify_type_name(type_name: &str) -> String {
        // Tente de parser `type_name` en tant que Type
        let parsed_type: Type = parse_str(type_name).expect("Erreur lors du parsing du type");

        // Fonction auxiliaire pour extraire les segments de base sans le chemin complet
        fn simplify_type(ty: &Type) -> String {
            match ty {
                Type::Path(type_path) => {
                    // Prend le dernier segment du chemin (nom de base du type)
                    let last_segment = type_path.path.segments.last().unwrap();
                    let ident = &last_segment.ident;

                    // Si le type a des arguments (ex. `Type<Arg1, Arg2>`), on les simplifie également
                    if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                        let args_str = args
                            .args
                            .iter()
                            .map(|arg| match arg {
                                syn::GenericArgument::Type(inner_ty) => simplify_type(inner_ty),
                                _ => arg.to_token_stream().to_string(),
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{ident}<{args_str}>")
                    } else {
                        ident.to_string()
                    }
                }
                _ => ty.to_token_stream().to_string(),
            }
        }

        simplify_type(&parsed_type)
    }

    fn get_key<Msg: 'static, Client: 'static>(&self) -> (TypeId, TypeId) {
        (TypeId::of::<Msg>(), TypeId::of::<Client>())
    }

    pub fn send<Msg: 'static, Client: 'static>(&mut self) {
        let key = self.get_key::<Msg, Client>();
        self.labels.entry(key).or_insert_with(|| {
            [
                KeyValue::new("msg", BusMetrics::simplify_type_name(type_name::<Msg>())),
                KeyValue::new("client_id", self.client_name.clone()),
            ]
        });
        self.send.add(1, self.labels.get(&key).unwrap());
    }

    pub fn receive<Msg: 'static, Client: 'static>(&mut self) {
        let key = self.get_key::<Msg, Client>();
        self.labels.entry(key).or_insert_with(|| {
            [
                KeyValue::new("msg", BusMetrics::simplify_type_name(type_name::<Msg>())),
                KeyValue::new("client_id", self.client_name.clone()),
            ]
        });
        self.receive.add(1, self.labels.get(&key).unwrap());
    }

    pub fn simplified_name<T>() -> String {
        BusMetrics::simplify_type_name(type_name::<T>())
    }

    pub fn with_client_name(mut self, client_name: String) -> Self {
        self.client_name = client_name;
        self
    }
}
