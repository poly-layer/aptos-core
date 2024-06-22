// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::transaction_shuffler::use_case_aware::{
    delayed_queue::DelayedQueue,
    types::{InputIdx, OutputIdx, UseCaseAwareTransaction},
    Config,
};
use std::{collections::VecDeque, fmt::Debug};

#[derive(Debug)]
pub(super) struct ShuffledTransactionIterator<Txn> {
    input_queue: VecDeque<Txn>,
    delayed_queue: DelayedQueue<Txn>,
    input_idx: InputIdx,
    output_idx: OutputIdx,
}

impl<Txn> ShuffledTransactionIterator<Txn>
where
    Txn: UseCaseAwareTransaction + Debug,
{
    pub(super) fn new(config: Config) -> Self {
        Self {
            input_queue: VecDeque::new(),
            delayed_queue: DelayedQueue::new(config),
            input_idx: 0,
            output_idx: 0,
        }
    }

    pub(super) fn extended_with(mut self, txns: impl IntoIterator<Item = Txn>) -> Self {
        self.input_queue.extend(txns);
        self
    }

    pub(super) fn select_next_txn(&mut self) -> Option<Txn> {
        let ret = self.select_next_txn_inner();
        if ret.is_some() {
            self.output_idx += 1;
        }
        ret
    }

    pub(super) fn select_next_txn_inner(&mut self) -> Option<Txn> {
        // println!("\n### {}: selection started.\n", self.output_idx);
        // println!("Starting with state:");
        // println!("{:#?}\n", self);

        self.delayed_queue.bump_output_idx(self.output_idx);
        // println!("After bumping the output idx:");
        // println!("{:#?}\n", self);

        // 1. if anything delayed became ready, return it
        if let Some(txn) = self.delayed_queue.pop_head(true) {
            // println!(
            //     "--- {}: Selected {:?} from the delayed queue",
            //     self.output_idx, txn
            // );
            return Some(txn);
        }

        // 2. Otherwise, seek in the input queue for something that shouldn't be delayed due to either
        // the sender or the use case.
        while let Some(txn) = self.input_queue.pop_front() {
            let input_idx = self.input_idx;
            // println!(
            //     "--- {}: examining {:?} from the input queue. input_idx: {input_idx}",
            //     self.output_idx, txn
            // );
            self.input_idx += 1;

            if let Some(txn) = self.delayed_queue.add_or_return(input_idx, txn) {
                // println!(
                //     "--- {}: Selected {:?} from the input queue",
                //     self.output_idx, txn
                // );
                return Some(txn);
            }
        }

        // 3. If nothing is ready, return the next eligible from the delay queue
        self.delayed_queue.pop_head(false)
        // println!(
        //     "--- {}: force select head {:?} from the delay queue",
        //     self.output_idx, ret
        // );
    }
}

impl<Txn> Iterator for ShuffledTransactionIterator<Txn>
where
    Txn: UseCaseAwareTransaction + Debug,
{
    type Item = Txn;

    fn next(&mut self) -> Option<Self::Item> {
        self.select_next_txn()
    }
}
