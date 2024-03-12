// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use aptos_native_interface::{RawSafeNative, SafeNativeBuilder};
use move_vm_runtime::native_functions::NativeFunction;

/***************************************************************************************************
 * module
 *
 **************************************************************************************************/
pub fn make_all(
    builder: &SafeNativeBuilder,
) -> impl Iterator<Item = (String, NativeFunction)> + '_ {
    let natives = [
        (
            "dispatchable_authenticate",
            super::dispatchable_fungible_asset::native_dispatch as RawSafeNative,
        ),
        (
            "dispatchable_verify_pay_master",
            super::dispatchable_fungible_asset::native_dispatch as RawSafeNative,
        ),
    ];

    builder.make_named_natives(natives)
}
