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

/// Returns monotonically increasing 64-bit integers. The generated ID is guaranteed to be
/// monotonically increasing and unique, but not consecutive.
/// - Returns: A ``Column``.
public func monotonically_increasing_id() -> Column {
  return fn("monotonically_increasing_id")
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
