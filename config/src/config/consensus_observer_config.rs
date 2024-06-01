// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsensusObserverConfig {
    /// Whether the consensus observer is enabled
    pub consensus_observer_enabled: bool,
    /// Whether the consensus publisher is enabled
    pub consensus_publisher_enabled: bool,
    /// Maximum number of pending network messages
    pub max_network_channel_size: u64,
}

impl ConsensusObserverConfig {
    /// Returns true iff the observer or publisher is enabled
    pub fn is_enabled(&self) -> bool {
        self.consensus_observer_enabled || self.consensus_publisher_enabled
    }
}
