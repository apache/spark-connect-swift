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

// MARK: - CSV functions

/// Parses a column containing a CSV string into a struct with the given schema.
/// Returns `NULL`, in the case of an unparseable string.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a CSV string.
///   - schema: A DDL schema string, e.g. `a INT, b STRING`.
///   - options: Options to control how the CSV is parsed. It accepts the same options as
///     the CSV data source.
/// - Returns: A ``Column`` of the type given by the schema.
public func from_csv(
  _ col: Column, _ schema: String, _ options: [String: String] = [:]
) -> Column {
  return fn("from_csv", options: options, col, lit(schema))
}

/// Parses a column containing a CSV string into a struct with the given schema.
/// Returns `NULL`, in the case of an unparseable string.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a CSV string.
///   - schema: A ``Column`` that evaluates to a constant schema string,
///     e.g. a `schema_of_csv` result.
///   - options: Options to control how the CSV is parsed. It accepts the same options as
///     the CSV data source.
/// - Returns: A ``Column`` of the type given by the schema.
public func from_csv(
  _ col: Column, _ schema: Column, _ options: [String: String] = [:]
) -> Column {
  return fn("from_csv", options: options, col, schema)
}

/// Parses a CSV string and infers its schema in DDL format.
/// - Parameters:
///   - csv: A CSV string.
///   - options: Options to control how the CSV is parsed. It accepts the same options as
///     the CSV data source.
/// - Returns: A ``Column`` that evaluates to a schema string in DDL format.
public func schema_of_csv(_ csv: String, _ options: [String: String] = [:]) -> Column {
  return schema_of_csv(lit(csv), options)
}

/// Parses a CSV string and infers its schema in DDL format.
/// - Parameters:
///   - csv: A ``Column`` that evaluates to a constant CSV string.
///   - options: Options to control how the CSV is parsed. It accepts the same options as
///     the CSV data source.
/// - Returns: A ``Column`` that evaluates to a schema string in DDL format.
public func schema_of_csv(_ csv: Column, _ options: [String: String] = [:]) -> Column {
  return fn("schema_of_csv", options: options, csv)
}

/// Converts a column containing a struct into a CSV string.
/// Throws an exception, in the case of an unsupported type.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a struct.
///   - options: Options to control how the column is converted. It accepts the same options as
///     the CSV data source.
/// - Returns: A ``Column`` that evaluates to a string.
public func to_csv(_ col: Column, _ options: [String: String] = [:]) -> Column {
  return fn("to_csv", options: options, col)
}
