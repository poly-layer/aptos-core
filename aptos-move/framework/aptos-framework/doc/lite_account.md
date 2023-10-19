
<a id="0x1_lite_account"></a>

# Module `0x1::lite_account`



-  [Struct `UpdateNativeAuthenticator`](#0x1_lite_account_UpdateNativeAuthenticator)
-  [Struct `UpdateDispatchableAuthenticator`](#0x1_lite_account_UpdateDispatchableAuthenticator)
-  [Struct `LiteAccountGroup`](#0x1_lite_account_LiteAccountGroup)
-  [Resource `Account`](#0x1_lite_account_Account)
-  [Resource `NativeAuthenticator`](#0x1_lite_account_NativeAuthenticator)
-  [Resource `DispatchableAuthenticator`](#0x1_lite_account_DispatchableAuthenticator)
-  [Constants](#@Constants_0)
-  [Function `update_native_authenticator`](#0x1_lite_account_update_native_authenticator)
-  [Function `update_dispatchable_authenticator`](#0x1_lite_account_update_dispatchable_authenticator)
-  [Function `update_native_authenticator_impl`](#0x1_lite_account_update_native_authenticator_impl)
-  [Function `update_dispatchable_authenticator_impl`](#0x1_lite_account_update_dispatchable_authenticator_impl)
-  [Function `create_account_resource`](#0x1_lite_account_create_account_resource)
-  [Function `create_account_unchecked`](#0x1_lite_account_create_account_unchecked)
-  [Function `exists_at`](#0x1_lite_account_exists_at)
-  [Function `account_resource_exists_at`](#0x1_lite_account_account_resource_exists_at)
-  [Function `using_dispatchable_authenticator`](#0x1_lite_account_using_dispatchable_authenticator)
-  [Function `get_sequence_number`](#0x1_lite_account_get_sequence_number)
-  [Function `native_authenticator`](#0x1_lite_account_native_authenticator)
-  [Function `dispatchable_authenticator`](#0x1_lite_account_dispatchable_authenticator)
-  [Function `increment_sequence_number`](#0x1_lite_account_increment_sequence_number)
-  [Function `dispatchable_authenticate`](#0x1_lite_account_dispatchable_authenticate)


<pre><code><b>use</b> <a href="account.md#0x1_account">0x1::account</a>;
<b>use</b> <a href="../../aptos-stdlib/../move-stdlib/doc/bcs.md#0x1_bcs">0x1::bcs</a>;
<b>use</b> <a href="create_signer.md#0x1_create_signer">0x1::create_signer</a>;
<b>use</b> <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error">0x1::error</a>;
<b>use</b> <a href="event.md#0x1_event">0x1::event</a>;
<b>use</b> <a href="function_info.md#0x1_function_info">0x1::function_info</a>;
<b>use</b> <a href="object.md#0x1_object">0x1::object</a>;
<b>use</b> <a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">0x1::signer</a>;
<b>use</b> <a href="../../aptos-stdlib/../move-stdlib/doc/string.md#0x1_string">0x1::string</a>;
</code></pre>



<a id="0x1_lite_account_UpdateNativeAuthenticator"></a>

## Struct `UpdateNativeAuthenticator`



<pre><code>#[<a href="event.md#0x1_event">event</a>]
<b>struct</b> <a href="lite_account.md#0x1_lite_account_UpdateNativeAuthenticator">UpdateNativeAuthenticator</a> <b>has</b> drop, store
</code></pre>



<details>
<summary>Fields</summary>


<dl>
<dt>
<code><a href="account.md#0x1_account">account</a>: <b>address</b></code>
</dt>
<dd>

</dd>
<dt>
<code>key: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;</code>
</dt>
<dd>

</dd>
</dl>


</details>

<a id="0x1_lite_account_UpdateDispatchableAuthenticator"></a>

## Struct `UpdateDispatchableAuthenticator`



<pre><code>#[<a href="event.md#0x1_event">event</a>]
<b>struct</b> <a href="lite_account.md#0x1_lite_account_UpdateDispatchableAuthenticator">UpdateDispatchableAuthenticator</a> <b>has</b> drop, store
</code></pre>



<details>
<summary>Fields</summary>


<dl>
<dt>
<code><a href="account.md#0x1_account">account</a>: <b>address</b></code>
</dt>
<dd>

</dd>
<dt>
<code>auth: <a href="function_info.md#0x1_function_info_FunctionInfo">function_info::FunctionInfo</a></code>
</dt>
<dd>

</dd>
</dl>


</details>

<a id="0x1_lite_account_LiteAccountGroup"></a>

## Struct `LiteAccountGroup`

A shared resource group for storing new account resources together in storage.


<pre><code>#[resource_group(#[scope = <b>address</b>])]
<b>struct</b> <a href="lite_account.md#0x1_lite_account_LiteAccountGroup">LiteAccountGroup</a>
</code></pre>



<details>
<summary>Fields</summary>


<dl>
<dt>
<code>dummy_field: bool</code>
</dt>
<dd>

</dd>
</dl>


</details>

<a id="0x1_lite_account_Account"></a>

## Resource `Account`

Resource representing an account object.


<pre><code>#[resource_group_member(#[group = <a href="lite_account.md#0x1_lite_account_LiteAccountGroup">0x1::lite_account::LiteAccountGroup</a>])]
<b>struct</b> <a href="lite_account.md#0x1_lite_account_Account">Account</a> <b>has</b> key
</code></pre>



<details>
<summary>Fields</summary>


<dl>
<dt>
<code>sequence_number: u64</code>
</dt>
<dd>

</dd>
</dl>


</details>

<a id="0x1_lite_account_NativeAuthenticator"></a>

## Resource `NativeAuthenticator`

The native authenticator where the key is used for authenticator verification in native code.


<pre><code>#[resource_group_member(#[group = <a href="lite_account.md#0x1_lite_account_LiteAccountGroup">0x1::lite_account::LiteAccountGroup</a>])]
<b>struct</b> <a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a> <b>has</b> <b>copy</b>, drop, key
</code></pre>



<details>
<summary>Fields</summary>


<dl>
<dt>
<code>key: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;</code>
</dt>
<dd>

</dd>
</dl>


</details>

<a id="0x1_lite_account_DispatchableAuthenticator"></a>

## Resource `DispatchableAuthenticator`

The dispatchable authenticator that defines how to authenticates this account in the specified module.
An integral part of Account Abstraction.


<pre><code>#[resource_group_member(#[group = <a href="lite_account.md#0x1_lite_account_LiteAccountGroup">0x1::lite_account::LiteAccountGroup</a>])]
<b>struct</b> <a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a> <b>has</b> <b>copy</b>, drop, key
</code></pre>



<details>
<summary>Fields</summary>


<dl>
<dt>
<code>auth: <a href="function_info.md#0x1_function_info_FunctionInfo">function_info::FunctionInfo</a></code>
</dt>
<dd>

</dd>
</dl>


</details>

<a id="@Constants_0"></a>

## Constants


<a id="0x1_lite_account_MAX_U64"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_MAX_U64">MAX_U64</a>: u128 = 18446744073709551615;
</code></pre>



<a id="0x1_lite_account_ECANNOT_RESERVED_ADDRESS"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_ECANNOT_RESERVED_ADDRESS">ECANNOT_RESERVED_ADDRESS</a>: u64 = 2;
</code></pre>



<a id="0x1_lite_account_EMALFORMED_AUTHENTICATION_KEY"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_EMALFORMED_AUTHENTICATION_KEY">EMALFORMED_AUTHENTICATION_KEY</a>: u64 = 4;
</code></pre>



<a id="0x1_lite_account_EACCOUNT_EXISTENCE"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_EACCOUNT_EXISTENCE">EACCOUNT_EXISTENCE</a>: u64 = 1;
</code></pre>



<a id="0x1_lite_account_EAUTH_FUNCTION_SIGNATURE_MISMATCH"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_EAUTH_FUNCTION_SIGNATURE_MISMATCH">EAUTH_FUNCTION_SIGNATURE_MISMATCH</a>: u64 = 7;
</code></pre>



<a id="0x1_lite_account_ECUSTOMIZED_AUTHENTICATOR_IS_NOT_USED"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_ECUSTOMIZED_AUTHENTICATOR_IS_NOT_USED">ECUSTOMIZED_AUTHENTICATOR_IS_NOT_USED</a>: u64 = 6;
</code></pre>



<a id="0x1_lite_account_ENATIVE_AUTHENTICATOR_IS_NOT_USED"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_ENATIVE_AUTHENTICATOR_IS_NOT_USED">ENATIVE_AUTHENTICATOR_IS_NOT_USED</a>: u64 = 5;
</code></pre>



<a id="0x1_lite_account_ENOT_OWNER"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_ENOT_OWNER">ENOT_OWNER</a>: u64 = 8;
</code></pre>



<a id="0x1_lite_account_ESEQUENCE_NUMBER_OVERFLOW"></a>



<pre><code><b>const</b> <a href="lite_account.md#0x1_lite_account_ESEQUENCE_NUMBER_OVERFLOW">ESEQUENCE_NUMBER_OVERFLOW</a>: u64 = 3;
</code></pre>



<a id="0x1_lite_account_update_native_authenticator"></a>

## Function `update_native_authenticator`

Update native authenticator, FKA account rotation.
Note: it is a private entry function that can only be called directly from transaction.


<pre><code>entry <b>fun</b> <a href="lite_account.md#0x1_lite_account_update_native_authenticator">update_native_authenticator</a>(<a href="account.md#0x1_account">account</a>: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>, key: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code>entry <b>fun</b> <a href="lite_account.md#0x1_lite_account_update_native_authenticator">update_native_authenticator</a>(
    <a href="account.md#0x1_account">account</a>: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>,
    key: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;,
) <b>acquires</b> <a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>, <a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a> {
    <a href="lite_account.md#0x1_lite_account_update_native_authenticator_impl">update_native_authenticator_impl</a>(<a href="account.md#0x1_account">account</a>, key);
}
</code></pre>



</details>

<a id="0x1_lite_account_update_dispatchable_authenticator"></a>

## Function `update_dispatchable_authenticator`

Update dispatchable authenticator, FKA account rotation.
Note: it is a private entry function that can only be called directly from transaction.


<pre><code>entry <b>fun</b> <a href="lite_account.md#0x1_lite_account_update_dispatchable_authenticator">update_dispatchable_authenticator</a>(<a href="account.md#0x1_account">account</a>: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>, module_address: <b>address</b>, module_name: <a href="../../aptos-stdlib/../move-stdlib/doc/string.md#0x1_string_String">string::String</a>, function_name: <a href="../../aptos-stdlib/../move-stdlib/doc/string.md#0x1_string_String">string::String</a>)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code>entry <b>fun</b> <a href="lite_account.md#0x1_lite_account_update_dispatchable_authenticator">update_dispatchable_authenticator</a>(
    <a href="account.md#0x1_account">account</a>: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>,
    module_address: <b>address</b>,
    module_name: String,
    function_name: String,
) <b>acquires</b> <a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>, <a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a> {
    <a href="lite_account.md#0x1_lite_account_update_dispatchable_authenticator_impl">update_dispatchable_authenticator_impl</a>(
        <a href="account.md#0x1_account">account</a>,
        <a href="function_info.md#0x1_function_info_new_function_info_from_address">function_info::new_function_info_from_address</a>(module_address, module_name, function_name)
    );
}
</code></pre>



</details>

<a id="0x1_lite_account_update_native_authenticator_impl"></a>

## Function `update_native_authenticator_impl`



<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="lite_account.md#0x1_lite_account_update_native_authenticator_impl">update_native_authenticator_impl</a>(<a href="account.md#0x1_account">account</a>: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>, new_auth_key: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="lite_account.md#0x1_lite_account_update_native_authenticator_impl">update_native_authenticator_impl</a>(
    <a href="account.md#0x1_account">account</a>: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>,
    new_auth_key: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;,
) <b>acquires</b> <a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>, <a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a> {
    <b>let</b> account_address = <a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer_address_of">signer::address_of</a>(<a href="account.md#0x1_account">account</a>);
    <b>assert</b>!(<a href="lite_account.md#0x1_lite_account_exists_at">exists_at</a>(account_address), <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_not_found">error::not_found</a>(<a href="lite_account.md#0x1_lite_account_EACCOUNT_EXISTENCE">EACCOUNT_EXISTENCE</a>));
    <b>assert</b>!(
        <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector_length">vector::length</a>(&new_auth_key) == 32,
        <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_invalid_argument">error::invalid_argument</a>(<a href="lite_account.md#0x1_lite_account_EMALFORMED_AUTHENTICATION_KEY">EMALFORMED_AUTHENTICATION_KEY</a>)
    );
    <b>if</b> (<b>exists</b>&lt;<a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>&gt;(account_address)) {
        <b>move_from</b>&lt;<a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>&gt;(account_address);
    };

    <b>if</b> (new_auth_key == <a href="../../aptos-stdlib/../move-stdlib/doc/bcs.md#0x1_bcs_to_bytes">bcs::to_bytes</a>(&account_address)) {
        <b>if</b> (<b>exists</b>&lt;<a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a>&gt;(account_address)) {
            <b>move_from</b>&lt;<a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a>&gt;(account_address);
        };
        <a href="event.md#0x1_event_emit">event::emit</a>(<a href="lite_account.md#0x1_lite_account_UpdateNativeAuthenticator">UpdateNativeAuthenticator</a> { <a href="account.md#0x1_account">account</a>: account_address, key: new_auth_key })
    } <b>else</b> <b>if</b> (<b>exists</b>&lt;<a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a>&gt;(account_address)) {
        <b>let</b> current = &<b>mut</b> <b>borrow_global_mut</b>&lt;<a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a>&gt;(account_address).key;
        <b>if</b> (*current != new_auth_key) {
            *current = new_auth_key;
            <a href="event.md#0x1_event_emit">event::emit</a>(<a href="lite_account.md#0x1_lite_account_UpdateNativeAuthenticator">UpdateNativeAuthenticator</a> { <a href="account.md#0x1_account">account</a>: account_address, key: new_auth_key })
        };
    } <b>else</b> {
        <b>move_to</b>(<a href="account.md#0x1_account">account</a>, <a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a> { key: new_auth_key });
        <a href="event.md#0x1_event_emit">event::emit</a>(<a href="lite_account.md#0x1_lite_account_UpdateNativeAuthenticator">UpdateNativeAuthenticator</a> { <a href="account.md#0x1_account">account</a>: account_address, key: new_auth_key })
    }
}
</code></pre>



</details>

<a id="0x1_lite_account_update_dispatchable_authenticator_impl"></a>

## Function `update_dispatchable_authenticator_impl`



<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="lite_account.md#0x1_lite_account_update_dispatchable_authenticator_impl">update_dispatchable_authenticator_impl</a>(<a href="account.md#0x1_account">account</a>: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>, auth: <a href="function_info.md#0x1_function_info_FunctionInfo">function_info::FunctionInfo</a>)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="lite_account.md#0x1_lite_account_update_dispatchable_authenticator_impl">update_dispatchable_authenticator_impl</a>(
    <a href="account.md#0x1_account">account</a>: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>,
    auth: FunctionInfo,
) <b>acquires</b> <a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>, <a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a> {
    <b>let</b> account_address = <a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer_address_of">signer::address_of</a>(<a href="account.md#0x1_account">account</a>);
    <b>assert</b>!(<a href="lite_account.md#0x1_lite_account_exists_at">exists_at</a>(account_address), <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_not_found">error::not_found</a>(<a href="lite_account.md#0x1_lite_account_EACCOUNT_EXISTENCE">EACCOUNT_EXISTENCE</a>));
    <b>if</b> (<b>exists</b>&lt;<a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a>&gt;(account_address)) {
        <b>move_from</b>&lt;<a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a>&gt;(account_address);
    };
    <b>let</b> dispatcher_auth_function_info = <a href="function_info.md#0x1_function_info_new_function_info_from_address">function_info::new_function_info_from_address</a>(
        @aptos_framework,
        <a href="../../aptos-stdlib/../move-stdlib/doc/string.md#0x1_string_utf8">string::utf8</a>(b"<a href="lite_account.md#0x1_lite_account">lite_account</a>"),
        <a href="../../aptos-stdlib/../move-stdlib/doc/string.md#0x1_string_utf8">string::utf8</a>(b"dispatchable_authenticate"),
    );
    <b>assert</b>!(
        <a href="function_info.md#0x1_function_info_check_dispatch_type_compatibility">function_info::check_dispatch_type_compatibility</a>(&dispatcher_auth_function_info, &auth),
        <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_invalid_argument">error::invalid_argument</a>(<a href="lite_account.md#0x1_lite_account_EAUTH_FUNCTION_SIGNATURE_MISMATCH">EAUTH_FUNCTION_SIGNATURE_MISMATCH</a>)
    );
    <b>if</b> (<b>exists</b>&lt;<a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>&gt;(account_address)) {
        <b>let</b> current = &<b>mut</b> <b>borrow_global_mut</b>&lt;<a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>&gt;(account_address).auth;
        <b>if</b> (*current != auth) {
            *current = auth;
            <a href="event.md#0x1_event_emit">event::emit</a>(<a href="lite_account.md#0x1_lite_account_UpdateDispatchableAuthenticator">UpdateDispatchableAuthenticator</a> { <a href="account.md#0x1_account">account</a>: account_address, auth });
        }
    } <b>else</b> {
        <b>move_to</b>(<a href="account.md#0x1_account">account</a>, <a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a> { auth });
        <a href="event.md#0x1_event_emit">event::emit</a>(<a href="lite_account.md#0x1_lite_account_UpdateDispatchableAuthenticator">UpdateDispatchableAuthenticator</a> { <a href="account.md#0x1_account">account</a>: account_address, auth });
    }
}
</code></pre>



</details>

<a id="0x1_lite_account_create_account_resource"></a>

## Function `create_account_resource`

Publishes a lite <code><a href="lite_account.md#0x1_lite_account_Account">Account</a></code> resource under <code>new_address</code>. A ConstructorRef representing <code>new_address</code>
is returned. This way, the caller of this function can publish additional resources under
<code>new_address</code>.


<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="lite_account.md#0x1_lite_account_create_account_resource">create_account_resource</a>(new_address: <b>address</b>): <a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="lite_account.md#0x1_lite_account_create_account_resource">create_account_resource</a>(new_address: <b>address</b>): <a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a> {
    // there cannot be an <a href="lite_account.md#0x1_lite_account_Account">Account</a> resource under new_addr already.
    <b>assert</b>!(!<a href="lite_account.md#0x1_lite_account_account_resource_exists_at">account_resource_exists_at</a>(new_address), <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_already_exists">error::already_exists</a>(<a href="lite_account.md#0x1_lite_account_EACCOUNT_EXISTENCE">EACCOUNT_EXISTENCE</a>));

    // NOTE: @core_resources gets created via a `create_account` call, so we do not <b>include</b> it below.
    <b>assert</b>!(
        new_address != @vm_reserved && new_address != @aptos_framework && new_address != @aptos_token,
        <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_invalid_argument">error::invalid_argument</a>(<a href="lite_account.md#0x1_lite_account_ECANNOT_RESERVED_ADDRESS">ECANNOT_RESERVED_ADDRESS</a>)
    );
    <a href="lite_account.md#0x1_lite_account_create_account_unchecked">create_account_unchecked</a>(new_address)
}
</code></pre>



</details>

<a id="0x1_lite_account_create_account_unchecked"></a>

## Function `create_account_unchecked`



<pre><code><b>fun</b> <a href="lite_account.md#0x1_lite_account_create_account_unchecked">create_account_unchecked</a>(new_address: <b>address</b>): <a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>fun</b> <a href="lite_account.md#0x1_lite_account_create_account_unchecked">create_account_unchecked</a>(new_address: <b>address</b>): <a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a> {
    <b>let</b> new_account = <a href="create_signer.md#0x1_create_signer_create_signer">create_signer::create_signer</a>(new_address);
    <b>move_to</b>(
        &new_account,
        <a href="lite_account.md#0x1_lite_account_Account">Account</a> {
            sequence_number: 0,
        }
    );
    <b>move_to</b>(&new_account,
        <a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a> {
            key: <a href="../../aptos-stdlib/../move-stdlib/doc/bcs.md#0x1_bcs_to_bytes">bcs::to_bytes</a>(&new_address)
        }
    );
    new_account
}
</code></pre>



</details>

<a id="0x1_lite_account_exists_at"></a>

## Function `exists_at`



<pre><code>#[view]
<b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_exists_at">exists_at</a>(addr: <b>address</b>): bool
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_exists_at">exists_at</a>(addr: <b>address</b>): bool {
    <a href="lite_account.md#0x1_lite_account_account_resource_exists_at">account_resource_exists_at</a>(addr) || (!<a href="account.md#0x1_account_exists_at">account::exists_at</a>(addr) && !<a href="object.md#0x1_object_object_exists">object::object_exists</a>&lt;ObjectCore&gt;(addr))
}
</code></pre>



</details>

<a id="0x1_lite_account_account_resource_exists_at"></a>

## Function `account_resource_exists_at`



<pre><code>#[view]
<b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_account_resource_exists_at">account_resource_exists_at</a>(addr: <b>address</b>): bool
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_account_resource_exists_at">account_resource_exists_at</a>(addr: <b>address</b>): bool {
    <b>exists</b>&lt;<a href="lite_account.md#0x1_lite_account_Account">Account</a>&gt;(addr)
}
</code></pre>



</details>

<a id="0x1_lite_account_using_dispatchable_authenticator"></a>

## Function `using_dispatchable_authenticator`



<pre><code>#[view]
<b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_using_dispatchable_authenticator">using_dispatchable_authenticator</a>(addr: <b>address</b>): bool
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_using_dispatchable_authenticator">using_dispatchable_authenticator</a>(addr: <b>address</b>): bool {
    <b>exists</b>&lt;<a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>&gt;(addr)
}
</code></pre>



</details>

<a id="0x1_lite_account_get_sequence_number"></a>

## Function `get_sequence_number`



<pre><code>#[view]
<b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_get_sequence_number">get_sequence_number</a>(addr: <b>address</b>): u64
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_get_sequence_number">get_sequence_number</a>(addr: <b>address</b>): u64 <b>acquires</b> <a href="lite_account.md#0x1_lite_account_Account">Account</a> {
    <b>assert</b>!(<a href="lite_account.md#0x1_lite_account_exists_at">exists_at</a>(addr), <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_not_found">error::not_found</a>(<a href="lite_account.md#0x1_lite_account_EACCOUNT_EXISTENCE">EACCOUNT_EXISTENCE</a>));
    <b>if</b> (<a href="lite_account.md#0x1_lite_account_account_resource_exists_at">account_resource_exists_at</a>(addr)) {
        <b>borrow_global</b>&lt;<a href="lite_account.md#0x1_lite_account_Account">Account</a>&gt;(addr).sequence_number
    } <b>else</b> {
        0
    }
}
</code></pre>



</details>

<a id="0x1_lite_account_native_authenticator"></a>

## Function `native_authenticator`



<pre><code>#[view]
<b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_native_authenticator">native_authenticator</a>(addr: <b>address</b>): <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_native_authenticator">native_authenticator</a>(addr: <b>address</b>): <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt; <b>acquires</b> <a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a> {
    <b>assert</b>!(!<a href="lite_account.md#0x1_lite_account_using_dispatchable_authenticator">using_dispatchable_authenticator</a>(addr), <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_not_found">error::not_found</a>(<a href="lite_account.md#0x1_lite_account_ENATIVE_AUTHENTICATOR_IS_NOT_USED">ENATIVE_AUTHENTICATOR_IS_NOT_USED</a>));
    <b>if</b> (<b>exists</b>&lt;<a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a>&gt;(addr)) {
        <b>borrow_global</b>&lt;<a href="lite_account.md#0x1_lite_account_NativeAuthenticator">NativeAuthenticator</a>&gt;(addr).key
    } <b>else</b> {
        <a href="../../aptos-stdlib/../move-stdlib/doc/bcs.md#0x1_bcs_to_bytes">bcs::to_bytes</a>(&addr)
    }
}
</code></pre>



</details>

<a id="0x1_lite_account_dispatchable_authenticator"></a>

## Function `dispatchable_authenticator`



<pre><code>#[view]
<b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_dispatchable_authenticator">dispatchable_authenticator</a>(addr: <b>address</b>): <a href="function_info.md#0x1_function_info_FunctionInfo">function_info::FunctionInfo</a>
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_dispatchable_authenticator">dispatchable_authenticator</a>(addr: <b>address</b>): FunctionInfo <b>acquires</b> <a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a> {
    <b>assert</b>!(<a href="lite_account.md#0x1_lite_account_using_dispatchable_authenticator">using_dispatchable_authenticator</a>(addr), <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_not_found">error::not_found</a>(<a href="lite_account.md#0x1_lite_account_ECUSTOMIZED_AUTHENTICATOR_IS_NOT_USED">ECUSTOMIZED_AUTHENTICATOR_IS_NOT_USED</a>));
    <b>borrow_global</b>&lt;<a href="lite_account.md#0x1_lite_account_DispatchableAuthenticator">DispatchableAuthenticator</a>&gt;(addr).auth
}
</code></pre>



</details>

<a id="0x1_lite_account_increment_sequence_number"></a>

## Function `increment_sequence_number`



<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="lite_account.md#0x1_lite_account_increment_sequence_number">increment_sequence_number</a>(addr: <b>address</b>)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="lite_account.md#0x1_lite_account_increment_sequence_number">increment_sequence_number</a>(addr: <b>address</b>) <b>acquires</b> <a href="lite_account.md#0x1_lite_account_Account">Account</a> {
    <b>if</b> (!<a href="lite_account.md#0x1_lite_account_account_resource_exists_at">account_resource_exists_at</a>(addr)) {
        <a href="lite_account.md#0x1_lite_account_create_account_resource">create_account_resource</a>(addr);
    };
    <b>let</b> sequence_number = &<b>mut</b> <b>borrow_global_mut</b>&lt;<a href="lite_account.md#0x1_lite_account_Account">Account</a>&gt;(addr).sequence_number;

    <b>assert</b>!(
        (*sequence_number <b>as</b> u128) &lt; <a href="lite_account.md#0x1_lite_account_MAX_U64">MAX_U64</a>,
        <a href="../../aptos-stdlib/../move-stdlib/doc/error.md#0x1_error_out_of_range">error::out_of_range</a>(<a href="lite_account.md#0x1_lite_account_ESEQUENCE_NUMBER_OVERFLOW">ESEQUENCE_NUMBER_OVERFLOW</a>)
    );
    *sequence_number = *sequence_number + 1;
}
</code></pre>



</details>

<a id="0x1_lite_account_dispatchable_authenticate"></a>

## Function `dispatchable_authenticate`



<pre><code><b>fun</b> <a href="lite_account.md#0x1_lite_account_dispatchable_authenticate">dispatchable_authenticate</a>(signature: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;, function: &<a href="function_info.md#0x1_function_info_FunctionInfo">function_info::FunctionInfo</a>)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>native</b> <b>fun</b> <a href="lite_account.md#0x1_lite_account_dispatchable_authenticate">dispatchable_authenticate</a>(
    signature: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u8&gt;,
    function: &FunctionInfo
);
</code></pre>



</details>


[move-book]: https://aptos.dev/move/book/SUMMARY
