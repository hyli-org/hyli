use crate::base64_field;
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "full", derive(Serialize, Deserialize, utoipa::ToSchema))]
pub struct CatchupStoreResponse {
    #[cfg_attr(feature = "full", serde(with = "base64_field"))]
    pub node_state_store: Vec<u8>,
    #[cfg_attr(feature = "full", serde(with = "base64_field"))]
    pub consensus_store: Vec<u8>,
}
