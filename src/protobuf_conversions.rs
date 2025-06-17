use crate::protocol;
use crate::pekko_protobuf::*;

// Conversion helpers from our internal types to Pekko protobuf types

impl From<protocol::ActorPath> for Address {
    fn from(path: protocol::ActorPath) -> Self {
        Self {
            protocol: path.protocol.to_string(),
            system: path.system,
            hostname: path.host,
            port: path.port as u32,
        }
    }
}

impl From<protocol::UniqueAddress> for UniqueAddress {
    fn from(unique_addr: protocol::UniqueAddress) -> Self {
        Self {
            address: unique_addr.address.into(),
            uid: unique_addr.uid,
        }
    }
}

impl From<Address> for protocol::ActorPath {
    fn from(addr: Address) -> Self {
        Self {
            protocol: crate::protocol::PEKKO, // Always pekko protocol in our implementation
            system: addr.system,
            host: addr.hostname,
            port: addr.port as u16,
            path: "system".to_string(), // Default path for address conversion
        }
    }
}

impl From<UniqueAddress> for protocol::UniqueAddress {
    fn from(unique_addr: UniqueAddress) -> Self {
        Self {
            address: protocol::ActorPath::from(unique_addr.address),
            uid: unique_addr.uid,
        }
    }
}

