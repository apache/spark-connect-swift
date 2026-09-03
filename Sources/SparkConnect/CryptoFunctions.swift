//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

// MARK: - Crypto functions
//
// These functions only build SQL expressions that the server evaluates; no encryption or
// authentication is performed by this client. A key passed as a literal becomes part of the
// query plan, so it can show up in server logs, the Spark UI, and plan output such as
// `DataFrame/explain(_:)`. Read keys from a column of an access-controlled source instead of
// embedding them in the plan whenever that matters.

/// Returns an encrypted value of `input` using AES in the given `mode` with the specified
/// `padding`. Key lengths of 16, 24 and 32 bytes are supported. Supported combinations of
/// (`mode`, `padding`) are ('ECB', 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS').
///
/// The `key` becomes part of the query plan when it is a literal, which exposes it to server
/// logs and plan output.
/// - Parameters:
///   - input: A binary ``Column`` to encrypt.
///   - key: A binary ``Column`` holding the passphrase used to encrypt the data.
///   - mode: A string ``Column`` selecting the block cipher mode. Valid values: `ECB`, `GCM`,
///     `CBC`. Defaults to `GCM`.
///   - padding: A string ``Column`` specifying how to pad messages whose length is not a
///     multiple of the block size. Valid values: `PKCS`, `NONE`, `DEFAULT`. `DEFAULT` means
///     `PKCS` for `ECB`, `NONE` for `GCM` and `PKCS` for `CBC`. Defaults to `DEFAULT`.
///   - iv: A binary ``Column`` holding the initialization vector. Only supported for `CBC` and
///     `GCM` modes, and must be 16 bytes for `CBC` and 12 bytes for `GCM`. Defaults to an empty
///     string, which makes the server generate a random vector and prepend it to the output.
///   - aad: A binary ``Column`` holding additional authenticated data. Only supported for `GCM`
///     mode, and the identical value must be given to ``aes_decrypt(_:_:mode:padding:aad:)``.
///     Defaults to an empty string.
/// - Returns: A ``Column`` that evaluates to a binary.
public func aes_encrypt(
  _ input: Column, _ key: Column, mode: Column = lit("GCM"), padding: Column = lit("DEFAULT"),
  iv: Column = lit(""), aad: Column = lit("")
) -> Column {
  return fn("aes_encrypt", input, key, mode, padding, iv, aad)
}

/// Returns a decrypted value of `input` using AES in the given `mode` with the specified
/// `padding`. Key lengths of 16, 24 and 32 bytes are supported. Supported combinations of
/// (`mode`, `padding`) are ('ECB', 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS').
///
/// The `key` becomes part of the query plan when it is a literal, which exposes it to server
/// logs and plan output.
/// - Parameters:
///   - input: A binary ``Column`` to decrypt.
///   - key: A binary ``Column`` holding the passphrase used to decrypt the data.
///   - mode: A string ``Column`` selecting the block cipher mode. Valid values: `ECB`, `GCM`,
///     `CBC`. Defaults to `GCM`.
///   - padding: A string ``Column`` specifying how to pad messages whose length is not a
///     multiple of the block size. Valid values: `PKCS`, `NONE`, `DEFAULT`. `DEFAULT` means
///     `PKCS` for `ECB`, `NONE` for `GCM` and `PKCS` for `CBC`. Defaults to `DEFAULT`.
///   - aad: A binary ``Column`` holding additional authenticated data. Only supported for `GCM`
///     mode, and must be the same value given to ``aes_encrypt(_:_:mode:padding:iv:aad:)``.
///     Defaults to an empty string.
/// - Returns: A ``Column`` that evaluates to a binary.
public func aes_decrypt(
  _ input: Column, _ key: Column, mode: Column = lit("GCM"), padding: Column = lit("DEFAULT"),
  aad: Column = lit("")
) -> Column {
  return fn("aes_decrypt", input, key, mode, padding, aad)
}

/// This is a special version of ``aes_decrypt(_:_:mode:padding:aad:)`` that returns `NULL`
/// instead of raising an error when decryption fails.
///
/// The `key` becomes part of the query plan when it is a literal, which exposes it to server
/// logs and plan output.
/// - Parameters:
///   - input: A binary ``Column`` to decrypt.
///   - key: A binary ``Column`` holding the passphrase used to decrypt the data.
///   - mode: A string ``Column`` selecting the block cipher mode. Valid values: `ECB`, `GCM`,
///     `CBC`. Defaults to `GCM`.
///   - padding: A string ``Column`` specifying how to pad messages whose length is not a
///     multiple of the block size. Valid values: `PKCS`, `NONE`, `DEFAULT`. `DEFAULT` means
///     `PKCS` for `ECB`, `NONE` for `GCM` and `PKCS` for `CBC`. Defaults to `DEFAULT`.
///   - aad: A binary ``Column`` holding additional authenticated data. Only supported for `GCM`
///     mode, and must be the same value given to ``aes_encrypt(_:_:mode:padding:iv:aad:)``.
///     Defaults to an empty string.
/// - Returns: A ``Column`` that evaluates to a binary.
public func try_aes_decrypt(
  _ input: Column, _ key: Column, mode: Column = lit("GCM"), padding: Column = lit("DEFAULT"),
  aad: Column = lit("")
) -> Column {
  return fn("try_aes_decrypt", input, key, mode, padding, aad)
}

/// Returns the keyed-hash message authentication code (HMAC) of `message` using `key` and
/// SHA-256. To use a different algorithm, call ``hmac(_:_:_:)``.
/// This requires a Spark 4.3.0+ server.
///
/// The result is raw MAC bytes; wrap it with ``hex(_:)`` or ``base64(_:)`` for a textual value.
/// The `key` becomes part of the query plan when it is a literal, which exposes it to server
/// logs and plan output.
/// - Parameters:
///   - key: A binary ``Column`` holding the secret key.
///   - message: A binary ``Column`` holding the message to authenticate.
/// - Returns: A ``Column`` that evaluates to a binary.
public func hmac(_ key: Column, _ message: Column) -> Column {
  return fn("hmac", key, message)
}

/// Returns the keyed-hash message authentication code (HMAC) of `message` using `key` and the
/// given hash `algorithm`.
/// This requires a Spark 4.3.0+ server.
///
/// The result is raw MAC bytes; wrap it with ``hex(_:)`` or ``base64(_:)`` for a textual value.
/// The `key` becomes part of the query plan when it is a literal, which exposes it to server
/// logs and plan output.
/// - Parameters:
///   - key: A binary ``Column`` holding the secret key.
///   - message: A binary ``Column`` holding the message to authenticate.
///   - algorithm: A string ``Column`` selecting the hash algorithm. Valid values: `SHA-224`,
///     `SHA-256`, `SHA-384`, `SHA-512`, `SHA-1`, `MD5`.
/// - Returns: A ``Column`` that evaluates to a binary.
public func hmac(_ key: Column, _ message: Column, _ algorithm: Column) -> Column {
  return fn("hmac", key, message, algorithm)
}
