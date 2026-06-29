//! Node metadata the sidecar reports on behalf of its node.
//!
//! Static facts about the node that the control plane needs but that the node
//! itself shouldn't have to care about — most importantly its **failure domain**
//! (region/AZ/rack), which the brain uses to spread a shard's replicas so a
//! single rack or zone loss can't take out a quorum.

use serde::Serialize;

/// Identity + placement metadata for the local node.
#[derive(Debug, Clone, Serialize)]
pub struct NodeMeta {
    pub node_id: String,
    /// Address peers/clients reach the node at (what the brain advertises).
    pub address: String,
    pub region: Option<String>,
    pub availability_zone: Option<String>,
    pub rack: Option<String>,
    pub version: Option<String>,
}

impl NodeMeta {
    pub fn from_env() -> Self {
        NodeMeta {
            node_id: std::env::var("FIDUCIA_NODE_ID").unwrap_or_else(|_| "node-a".to_string()),
            address: std::env::var("FIDUCIA_NODE_ADDRESS")
                .unwrap_or_else(|_| "http://localhost:8090".to_string()),
            region: std::env::var("FIDUCIA_REGION").ok(),
            availability_zone: std::env::var("FIDUCIA_AZ").ok(),
            rack: std::env::var("FIDUCIA_RACK").ok(),
            version: std::env::var("FIDUCIA_NODE_VERSION").ok(),
        }
    }

    /// The single failure-domain label the brain spreads replicas across.
    ///
    /// The brain spreads on **one** label, so it must be the *primary* domain to
    /// keep distinct — the **region** (cluster), which is what makes the
    /// cross-cluster "one replica per cluster, survive losing a whole cluster"
    /// guarantee hold. (Appending the AZ here would let two replicas land in two
    /// AZs of the *same* region, breaking that.) AZ/rack still travel in the
    /// metadata for observability. Empty = unknown (treated as its own domain).
    pub fn failure_domain(&self) -> String {
        self.region.clone().unwrap_or_default()
    }
}
