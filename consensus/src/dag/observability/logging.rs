// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_consensus_types::common::{Author, Round};
use aptos_logger::Schema;
use serde::Serialize;

#[derive(Schema)]
pub struct LogSchema {
    event: LogEvent,
    remote_peer: Option<Author>,
    round: Option<Round>,
}

#[derive(Serialize)]
pub enum LogEvent {
    EpochStart,
    ModeTransition,
    BroadcastNode,
    ReceiveNode,
    Vote,
    ReceiveVote,
    BroadcastCertifiedNode,
    ReceiveCertifiedNode,
    ReceiveAck,
    OrderedAnchor,
    NewRound,
    FetchNodes,
    ReceiveFetchNodes,
    ActiveMode,
    SyncMode,
    SyncOutcome,
    Shutdown,
    FetchPayload,
}

impl LogSchema {
    pub fn new(event: LogEvent) -> Self {
        Self {
            event,
            remote_peer: None,
            round: None,
        }
    }
}
