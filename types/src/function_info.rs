// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use move_core_types::account_address::AccountAddress;
use move_core_types::value::{MoveStruct, MoveValue};
use crate::move_utils::as_move_value::AsMoveValue;

/// Reflection of aptos_framework::function_info::FunctionInfo
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct FunctionInfo {
    pub module_address: AccountAddress,
    pub module_name: String,
    pub function_name: String,
}

impl FunctionInfo {
    pub fn new(module_address: AccountAddress, module_name: String, function_name: String) -> Self {
        Self {
            module_address,
            module_name,
            function_name,
        }
    }
}

impl AsMoveValue for FunctionInfo {
    fn as_move_value(&self) -> MoveValue {
        MoveValue::Struct(MoveStruct::Runtime(vec![
            MoveValue::Address(self.module_address),
            self.module_name.as_move_value(),
            self.function_name.as_move_value(),
        ]))
    }
}
