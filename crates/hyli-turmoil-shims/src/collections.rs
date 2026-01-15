// Stable map ordering in turmoil tests, HashMap in production.
#[cfg(feature = "turmoil")]
pub type StableMap<K, V> = std::collections::BTreeMap<K, V>;

#[cfg(not(feature = "turmoil"))]
pub type StableMap<K, V> = std::collections::HashMap<K, V>;
