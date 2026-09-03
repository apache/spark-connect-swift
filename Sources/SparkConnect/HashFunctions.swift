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

// MARK: - Hash functions

/// Calculates the MD5 digest of a binary column and returns the value as a 32 character hex
/// string.
/// - Parameter col: A ``Column`` to compute on.
/// - Returns: A ``Column`` that evaluates to a string.
public func md5(_ col: Column) -> Column {
  return fn("md5", col)
}

/// Calculates the SHA-1 digest of a binary column and returns the value as a 40 character hex
/// string.
/// - Parameter col: A ``Column`` to compute on.
/// - Returns: A ``Column`` that evaluates to a string.
public func sha1(_ col: Column) -> Column {
  return fn("sha1", col)
}

/// Returns a sha1 hash value as a hex string of the `col`. This is an alias of ``sha1(_:)``.
/// - Parameter col: A ``Column`` to compute on.
/// - Returns: A ``Column`` that evaluates to a string.
public func sha(_ col: Column) -> Column {
  return fn("sha", col)
}

/// Calculates the SHA-2 family of hash functions of a binary column and returns the value as a
/// hex string.
/// - Parameters:
///   - col: A ``Column`` to compute on.
///   - numBits: One of 224, 256, 384, 512, or 0 which is equivalent to 256.
/// - Returns: A ``Column`` that evaluates to a string.
public func sha2(_ col: Column, _ numBits: Int32) -> Column {
  return fn("sha2", col, lit(numBits))
}

/// Calculates the cyclic redundancy check value (CRC32) of a binary column and returns the value
/// as a bigint.
/// - Parameter col: A ``Column`` to compute on.
/// - Returns: A ``Column`` that evaluates to a long.
public func crc32(_ col: Column) -> Column {
  return fn("crc32", col)
}

/// Calculates the hash code of given columns, and returns the result as an int column.
/// - Parameter cols: One or more ``Column``s to compute on.
/// - Returns: A ``Column`` that evaluates to an integer.
public func hash(_ cols: Column...) -> Column {
  return fn("hash", cols)
}

/// Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm,
/// and returns the result as a long column. The hash computation uses an initial seed of 42.
/// - Parameter cols: One or more ``Column``s to compute on.
/// - Returns: A ``Column`` that evaluates to a long.
public func xxhash64(_ cols: Column...) -> Column {
  return fn("xxhash64", cols)
}
