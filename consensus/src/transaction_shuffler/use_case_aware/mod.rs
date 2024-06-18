// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::transaction_shuffler::{use_case_aware::types::UseCaseKey, TransactionShuffler};
use aptos_types::transaction::SignedTransaction;
use iterator::ShuffledTransactionIterator;

pub(crate) mod iterator;
pub(crate) mod types;
pub(crate) mod utils;

pub(crate) mod delayed_queue;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Default)]
pub(crate) struct Config {
    sender_spread_factor: usize,
    platform_use_case_spread_factor: usize,
    user_use_case_spread_factor: usize,
}

impl Config {
    pub(crate) fn sender_spread_factor(&self) -> usize {
        self.sender_spread_factor
    }

    pub(crate) fn use_case_spread_factor(&self, use_case_key: &UseCaseKey) -> usize {
        match use_case_key {
            UseCaseKey::Platform => self.platform_use_case_spread_factor,
            _ => self.user_use_case_spread_factor,
        }
    }
}

pub struct UseCaseAwareShuffler {
    config: Config,
}

impl TransactionShuffler for UseCaseAwareShuffler {
    fn shuffle(&self, txns: Vec<SignedTransaction>) -> Vec<SignedTransaction> {
        ShuffledTransactionIterator::new(self.config.clone())
            .extended_with(txns)
            .collect()
    }
}
