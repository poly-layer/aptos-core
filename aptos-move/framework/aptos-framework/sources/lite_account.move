module aptos_framework::lite_account {
    use std::bcs;
    use std::error;
    use std::signer;
    use std::string::{Self, String};
    use std::vector;
    use aptos_framework::account;
    use aptos_framework::create_signer;
    use aptos_framework::event;
    use aptos_framework::function_info::{Self, FunctionInfo};
    use aptos_framework::object;
    use aptos_framework::object::ObjectCore;

    friend aptos_framework::aptos_account;
    friend aptos_framework::resource_account;
    friend aptos_framework::transaction_validation;
    #[test_only]
    friend aptos_framework::lite_account_tests;

    const EACCOUNT_EXISTENCE: u64 = 1;
    const ECANNOT_RESERVED_ADDRESS: u64 = 2;
    const ESEQUENCE_NUMBER_OVERFLOW: u64 = 3;
    const EMALFORMED_AUTHENTICATION_KEY: u64 = 4;
    const ENATIVE_AUTHENTICATOR_IS_NOT_USED: u64 = 5;
    const ECUSTOMIZED_AUTHENTICATOR_IS_NOT_USED: u64 = 6;
    const EAUTH_FUNCTION_SIGNATURE_MISMATCH: u64 = 7;
    const ENOT_OWNER: u64 = 8;

    const MAX_U64: u128 = 18446744073709551615;

    #[event]
    struct UpdateNativeAuthenticator has store, drop {
        account: address,
        key: vector<u8>,
    }

    #[event]
    struct UpdateDispatchableAuthenticator has store, drop {
        account: address,
        auth: FunctionInfo
    }

    #[resource_group(scope = address)]
    /// A shared resource group for storing new account resources together in storage.
    struct LiteAccountGroup {}

    #[resource_group_member(group = aptos_framework::lite_account::LiteAccountGroup)]
    /// Resource representing an account object.
    struct Account has key {
        sequence_number: u64,
    }

    #[resource_group_member(group = aptos_framework::lite_account::LiteAccountGroup)]
    /// The native authenticator where the key is used for authenticator verification in native code.
    struct NativeAuthenticator has key, copy, drop {
        key: vector<u8>,
    }

    #[resource_group_member(group = aptos_framework::lite_account::LiteAccountGroup)]
    /// The dispatchable authenticator that defines how to authenticates this account in the specified module.
    /// An integral part of Account Abstraction.
    struct DispatchableAuthenticator has key, copy, drop {
        auth: FunctionInfo
    }

    /// Update native authenticator, FKA account rotation.
    /// Note: it is a private entry function that can only be called directly from transaction.
    entry fun update_native_authenticator(
        account: &signer,
        key: vector<u8>,
    ) acquires DispatchableAuthenticator, NativeAuthenticator {
        update_native_authenticator_impl(account, key);
    }

    /// Update dispatchable authenticator, FKA account rotation.
    /// Note: it is a private entry function that can only be called directly from transaction.
    entry fun update_dispatchable_authenticator(
        account: &signer,
        module_address: address,
        module_name: String,
        function_name: String,
    ) acquires DispatchableAuthenticator, NativeAuthenticator {
        update_dispatchable_authenticator_impl(
            account,
            function_info::new_function_info_from_address(module_address, module_name, function_name)
        );
    }

    public(friend) fun update_native_authenticator_impl(
        account: &signer,
        new_auth_key: vector<u8>,
    ) acquires DispatchableAuthenticator, NativeAuthenticator {
        let account_address = signer::address_of(account);
        assert!(exists_at(account_address), error::not_found(EACCOUNT_EXISTENCE));
        assert!(
            vector::length(&new_auth_key) == 32,
            error::invalid_argument(EMALFORMED_AUTHENTICATION_KEY)
        );
        if (exists<DispatchableAuthenticator>(account_address)) {
            move_from<DispatchableAuthenticator>(account_address);
        };

        if (new_auth_key == bcs::to_bytes(&account_address)) {
            if (exists<NativeAuthenticator>(account_address)) {
                move_from<NativeAuthenticator>(account_address);
            };
            event::emit(UpdateNativeAuthenticator { account: account_address, key: new_auth_key })
        } else if (exists<NativeAuthenticator>(account_address)) {
            let current = &mut borrow_global_mut<NativeAuthenticator>(account_address).key;
            if (*current != new_auth_key) {
                *current = new_auth_key;
                event::emit(UpdateNativeAuthenticator { account: account_address, key: new_auth_key })
            };
        } else {
            move_to(account, NativeAuthenticator { key: new_auth_key });
            event::emit(UpdateNativeAuthenticator { account: account_address, key: new_auth_key })
        }
    }

    public(friend) fun update_dispatchable_authenticator_impl(
        account: &signer,
        auth: FunctionInfo,
    ) acquires DispatchableAuthenticator, NativeAuthenticator {
        let account_address = signer::address_of(account);
        assert!(exists_at(account_address), error::not_found(EACCOUNT_EXISTENCE));
        if (exists<NativeAuthenticator>(account_address)) {
            move_from<NativeAuthenticator>(account_address);
        };
        let dispatcher_auth_function_info = function_info::new_function_info_from_address(
            @aptos_framework,
            string::utf8(b"lite_account"),
            string::utf8(b"dispatchable_authenticate"),
        );
        assert!(
            function_info::check_dispatch_type_compatibility(&dispatcher_auth_function_info, &auth),
            error::invalid_argument(EAUTH_FUNCTION_SIGNATURE_MISMATCH)
        );
        if (exists<DispatchableAuthenticator>(account_address)) {
            let current = &mut borrow_global_mut<DispatchableAuthenticator>(account_address).auth;
            if (*current != auth) {
                *current = auth;
                event::emit(UpdateDispatchableAuthenticator { account: account_address, auth });
            }
        } else {
            move_to(account, DispatchableAuthenticator { auth });
            event::emit(UpdateDispatchableAuthenticator { account: account_address, auth });
        }
    }

    /// Publishes a lite `Account` resource under `new_address`. A ConstructorRef representing `new_address`
    /// is returned. This way, the caller of this function can publish additional resources under
    /// `new_address`.
    public(friend) fun create_account_resource(new_address: address): signer {
        // there cannot be an Account resource under new_addr already.
        assert!(!account_resource_exists_at(new_address), error::already_exists(EACCOUNT_EXISTENCE));

        // NOTE: @core_resources gets created via a `create_account` call, so we do not include it below.
        assert!(
            new_address != @vm_reserved && new_address != @aptos_framework && new_address != @aptos_token,
            error::invalid_argument(ECANNOT_RESERVED_ADDRESS)
        );
        create_account_unchecked(new_address)
    }

    fun create_account_unchecked(new_address: address): signer {
        let new_account = create_signer::create_signer(new_address);
        move_to(
            &new_account,
            Account {
                sequence_number: 0,
            }
        );
        move_to(&new_account,
            NativeAuthenticator {
                key: bcs::to_bytes(&new_address)
            }
        );
        new_account
    }

    #[view]
    public fun exists_at(addr: address): bool {
        account_resource_exists_at(addr) || (!account::exists_at(addr) && !object::object_exists<ObjectCore>(addr))
    }

    #[view]
    public fun account_resource_exists_at(addr: address): bool {
        exists<Account>(addr)
    }

    #[view]
    public fun using_dispatchable_authenticator(addr: address): bool {
        exists<DispatchableAuthenticator>(addr)
    }

    #[view]
    public fun get_sequence_number(addr: address): u64 acquires Account {
        assert!(exists_at(addr), error::not_found(EACCOUNT_EXISTENCE));
        if (account_resource_exists_at(addr)) {
            borrow_global<Account>(addr).sequence_number
        } else {
            0
        }
    }

    #[view]
    public fun native_authenticator(addr: address): vector<u8> acquires NativeAuthenticator {
        assert!(!using_dispatchable_authenticator(addr), error::not_found(ENATIVE_AUTHENTICATOR_IS_NOT_USED));
        if (exists<NativeAuthenticator>(addr)) {
            borrow_global<NativeAuthenticator>(addr).key
        } else {
            bcs::to_bytes(&addr)
        }
    }

    #[view]
    public fun dispatchable_authenticator(addr: address): FunctionInfo acquires DispatchableAuthenticator {
        assert!(using_dispatchable_authenticator(addr), error::not_found(ECUSTOMIZED_AUTHENTICATOR_IS_NOT_USED));
        borrow_global<DispatchableAuthenticator>(addr).auth
    }

    // Only called by transaction_validation.move in apilogue for sequential transactions.
    public(friend) fun increment_sequence_number(addr: address) acquires Account {
        if (!account_resource_exists_at(addr)) {
            create_account_resource(addr);
        };
        let sequence_number = &mut borrow_global_mut<Account>(addr).sequence_number;

        assert!(
            (*sequence_number as u128) < MAX_U64,
            error::out_of_range(ESEQUENCE_NUMBER_OVERFLOW)
        );
        *sequence_number = *sequence_number + 1;
    }

    native fun dispatchable_authenticate(
        signature: vector<u8>,
        function: &FunctionInfo
    );

    #[test_only]
    public fun create_account_for_test(new_address: address): signer {
        create_account_unchecked(new_address)
    }

    #[test(aaron = @0xcafe)]
    entry fun test_empty_account(aaron: &signer) acquires NativeAuthenticator, Account, DispatchableAuthenticator {
        let addr = signer::address_of(aaron);
        assert!(exists_at(addr), 0);
        assert!(!account_resource_exists_at(addr), 0);
        assert!(!using_dispatchable_authenticator(addr), 0);
        assert!(native_authenticator(addr) == bcs::to_bytes(&addr), 0);
        assert!(get_sequence_number(addr) == 0, 0);
        assert!(native_authenticator(addr) == bcs::to_bytes(&addr), 0);
        update_native_authenticator(aaron, bcs::to_bytes(&@0x1));
        assert!(native_authenticator(addr) == bcs::to_bytes(&@0x1), 0);
    }

    #[test(bob = @0xb0b)]
    entry fun test_account_basics(
        bob: &signer,
    ) acquires Account, DispatchableAuthenticator, NativeAuthenticator {
        let bob_addr = signer::address_of(bob);
        create_account_for_test(bob_addr);
        assert!(exists_at(bob_addr), 0);
        assert!(!using_dispatchable_authenticator(bob_addr), 0);
        assert!(get_sequence_number(bob_addr) == 0, 0);

        increment_sequence_number(bob_addr);
        assert!(get_sequence_number(bob_addr) == 1, 0);
        update_dispatchable_authenticator(
            bob,
            @aptos_framework,
            string::utf8(b"lite_account_tests"),
            string::utf8(b"test_auth")
        );
    }
}
