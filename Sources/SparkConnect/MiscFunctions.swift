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

// MARK: - Misc functions

/// Returns `null` if the input column is `true`, and throws an exception otherwise.
/// - Parameter col: A ``Column`` that evaluates to the boolean condition to check.
/// - Returns: A ``Column`` that always evaluates to `null`.
public func assert_true(_ col: Column) -> Column {
  return fn("assert_true", col)
}

/// Returns `null` if the input column is `true`, and throws an exception with the given error
/// message otherwise.
/// - Parameters:
///   - col: A ``Column`` that evaluates to the boolean condition to check.
///   - errMsg: A ``Column`` that evaluates to the error message to throw.
/// - Returns: A ``Column`` that always evaluates to `null`.
public func assert_true(_ col: Column, _ errMsg: Column) -> Column {
  return fn("assert_true", col, errMsg)
}

/// Returns `null` if the input column is `true`, and throws an exception with the given error
/// message otherwise.
/// - Parameters:
///   - col: A ``Column`` that evaluates to the boolean condition to check.
///   - errMsg: A literal error message to throw.
/// - Returns: A ``Column`` that always evaluates to `null`.
public func assert_true(_ col: Column, _ errMsg: String) -> Column {
  return assert_true(col, lit(errMsg))
}

/// Returns the bit position for the given input column.
/// - Parameter col: A ``Column`` that evaluates to an integral value.
/// - Returns: A ``Column`` that evaluates to a long.
public func bitmap_bit_position(_ col: Column) -> Column {
  return fn("bitmap_bit_position", col)
}

/// Returns the bucket number for the given input column.
/// - Parameter col: A ``Column`` that evaluates to an integral value.
/// - Returns: A ``Column`` that evaluates to a long.
public func bitmap_bucket_number(_ col: Column) -> Column {
  return fn("bitmap_bucket_number", col)
}

/// Returns the number of set bits in the input bitmap.
/// - Parameter col: A ``Column`` that evaluates to a binary bitmap.
/// - Returns: A ``Column`` that evaluates to a long.
public func bitmap_count(_ col: Column) -> Column {
  return fn("bitmap_count", col)
}

/// Returns the current catalog.
/// - Returns: A ``Column``.
public func current_catalog() -> Column {
  return fn("current_catalog")
}

/// Returns the current database.
/// - Returns: A ``Column``.
public func current_database() -> Column {
  return fn("current_database")
}

/// Returns the current SQL path as a comma-separated list of qualified schema names.
/// This requires a Spark 4.2.0+ server.
/// - Returns: A ``Column``.
public func current_path() -> Column {
  return fn("current_path")
}

/// Returns the current schema.
/// - Returns: A ``Column``.
public func current_schema() -> Column {
  return fn("current_schema")
}

/// Returns the user name of the current execution context.
/// - Returns: A ``Column``.
public func current_user() -> Column {
  return fn("current_user")
}

/// Returns the length of the block being read, or -1 if not available.
/// - Returns: A ``Column``.
public func input_file_block_length() -> Column {
  return fn("input_file_block_length")
}

/// Returns the start offset of the block being read, or -1 if not available.
/// - Returns: A ``Column``.
public func input_file_block_start() -> Column {
  return fn("input_file_block_start")
}

/// Returns the file name of the current Spark task, or an empty string if not available.
/// - Returns: A ``Column``.
public func input_file_name() -> Column {
  return fn("input_file_name")
}

/// Calls a Java method with reflection. The first argument is the class name, the second one is
/// the method name, and the remaining ones are the arguments passed to the method. This is an
/// alias of ``reflect(_:)``.
///
/// The method is resolved and invoked on the server, so the class must be on the Spark server's
/// classpath. Never build the class name or the method name from untrusted user input.
///
/// - Parameter cols: The class name, the method name, and the method arguments as ``Column``s.
/// - Returns: A ``Column`` that evaluates to a string.
public func java_method(_ cols: Column...) -> Column {
  return fn("java_method", cols)
}

/// Returns monotonically increasing 64-bit integers. The generated ID is guaranteed to be
/// monotonically increasing and unique, but not consecutive.
/// - Returns: A ``Column``.
public func monotonically_increasing_id() -> Column {
  return fn("monotonically_increasing_id")
}

/// Throws an exception with the given error message.
/// - Parameter errMsg: A ``Column`` that evaluates to the error message to throw.
/// - Returns: A ``Column`` that always evaluates to `null`.
public func raise_error(_ errMsg: Column) -> Column {
  return fn("raise_error", errMsg)
}

/// Throws an exception with the given error message.
/// - Parameter errMsg: A literal error message to throw.
/// - Returns: A ``Column`` that always evaluates to `null`.
public func raise_error(_ errMsg: String) -> Column {
  return raise_error(lit(errMsg))
}

/// Calls a Java method with reflection. The first argument is the class name, the second one is
/// the method name, and the remaining ones are the arguments passed to the method.
///
/// The method is resolved and invoked on the server, so the class must be on the Spark server's
/// classpath. Never build the class name or the method name from untrusted user input.
///
/// - Parameter cols: The class name, the method name, and the method arguments as ``Column``s.
/// - Returns: A ``Column`` that evaluates to a string.
public func reflect(_ cols: Column...) -> Column {
  return fn("reflect", cols)
}

/// Returns the user name of the current execution context.
/// This requires a Spark 4.0.0+ server.
/// - Returns: A ``Column``.
public func session_user() -> Column {
  return fn("session_user")
}

/// Returns the partition ID. This is non-deterministic because it depends on data partitioning
/// and task scheduling.
/// - Returns: A ``Column``.
public func spark_partition_id() -> Column {
  return fn("spark_partition_id")
}

/// Calls a Java method with reflection, returning `null` instead of raising an error when the
/// invoked method throws an exception. This is the `try` version of ``reflect(_:)`` and requires
/// a Spark 4.0.0+ server.
///
/// The method is resolved and invoked on the server, so the class must be on the Spark server's
/// classpath. Never build the class name or the method name from untrusted user input.
///
/// - Parameter cols: The class name, the method name, and the method arguments as ``Column``s.
/// - Returns: A ``Column`` that evaluates to a string.
public func try_reflect(_ cols: Column...) -> Column {
  return fn("try_reflect", cols)
}

/// Returns the DDL-formatted type string for the data type of the input.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func typeof(_ col: Column) -> Column {
  return fn("typeof", col)
}

/// Returns the user name of the current execution context.
/// - Returns: A ``Column``.
public func user() -> Column {
  return fn("user")
}

/// Returns a universally unique identifier (UUID) string. The value is returned as a canonical
/// UUID 36-character string.
/// - Returns: A ``Column``.
public func uuid() -> Column {
  return fn("uuid")
}

/// Returns a universally unique identifier (UUID) string. The value is returned as a canonical
/// UUID 36-character string. This requires a Spark 4.1.0+ server.
/// - Parameter seed: A random number seed ``Column``. Must be a constant.
/// - Returns: A ``Column``.
public func uuid(_ seed: Column) -> Column {
  return fn("uuid", seed)
}

/// Returns a universally unique identifier (UUID) string. The value is returned as a canonical
/// UUID 36-character string. This requires a Spark 4.1.0+ server.
/// - Parameter seed: A literal random number seed.
/// - Returns: A ``Column``.
public func uuid(_ seed: some SparkLiteral) -> Column {
  return uuid(seed.toLiteralColumn)
}

/// Returns the Spark version of the server as a ``Column`` expression. The string contains two
/// fields, the first being a release version and the second being a git revision. Unlike
/// ``SparkSession/version``, which is a `String` already fetched from the server, this builds a
/// SQL expression evaluated by the server.
/// - Returns: A ``Column``.
public func version() -> Column {
  return fn("version")
}
