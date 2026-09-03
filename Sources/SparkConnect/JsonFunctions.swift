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

// MARK: - JSON functions

/// Parses a column containing a JSON string into a struct, an array, or a map with the given
/// schema. Returns `NULL`, in the case of an unparseable string.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a JSON string.
///   - schema: A DDL schema string, e.g. `a INT, b STRING` or `ARRAY<STRUCT<a: INT>>`.
///   - options: Options to control how the JSON is parsed. It accepts the same options as
///     the JSON data source.
/// - Returns: A ``Column`` of the type given by the schema.
public func from_json(
  _ col: Column, _ schema: String, _ options: [String: String] = [:]
) -> Column {
  return fn("from_json", options: options, col, lit(schema))
}

/// Parses a column containing a JSON string into a struct with the given schema.
/// Returns `NULL`, in the case of an unparseable string.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a JSON string.
///   - schema: A ``StructType`` schema.
///   - options: Options to control how the JSON is parsed. It accepts the same options as
///     the JSON data source.
/// - Returns: A ``Column`` of the type given by the schema.
public func from_json(
  _ col: Column, _ schema: StructType, _ options: [String: String] = [:]
) -> Column {
  return from_json(col, schema.toDDL, options)
}

/// Parses a column containing a JSON string into a struct, an array, or a map with the given
/// schema. Returns `NULL`, in the case of an unparseable string.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a JSON string.
///   - schema: A ``Column`` that evaluates to a constant schema string,
///     e.g. a `schema_of_json` result.
///   - options: Options to control how the JSON is parsed. It accepts the same options as
///     the JSON data source.
/// - Returns: A ``Column`` of the type given by the schema.
public func from_json(
  _ col: Column, _ schema: Column, _ options: [String: String] = [:]
) -> Column {
  return fn("from_json", options: options, col, schema)
}

/// Extracts a JSON object from a JSON string based on the given JSON path.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a JSON string.
///   - path: A JSON path to extract, e.g. `$.a.b`.
/// - Returns: A ``Column`` that evaluates to a string.
public func get_json_object(_ col: Column, _ path: String) -> Column {
  return fn("get_json_object", col, lit(path))
}

/// Returns the number of elements in the outermost JSON array. `NULL` is returned in case of
/// any other valid JSON string, `NULL` or an invalid JSON.
/// - Parameter col: A ``Column`` that evaluates to a JSON array string.
/// - Returns: A ``Column``.
public func json_array_length(_ col: Column) -> Column {
  return fn("json_array_length", col)
}

/// Returns all the keys of the outermost JSON object as an array. `NULL` is returned in case of
/// any other valid JSON string, `NULL` or an invalid JSON.
/// - Parameter col: A ``Column`` that evaluates to a JSON object string.
/// - Returns: A ``Column``.
public func json_object_keys(_ col: Column) -> Column {
  return fn("json_object_keys", col)
}

/// Creates a new row for a JSON column according to the given field names.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a JSON string.
///   - fields: The field names to extract.
/// - Returns: A ``Column``.
public func json_tuple(_ col: Column, _ fields: String...) -> Column {
  return fn("json_tuple", [col] + fields.map { lit($0) })
}

/// Parses a JSON string and infers its schema in DDL format.
/// - Parameters:
///   - json: A JSON string.
///   - options: Options to control how the JSON is parsed. It accepts the same options as
///     the JSON data source.
/// - Returns: A ``Column`` that evaluates to a schema string in DDL format.
public func schema_of_json(_ json: String, _ options: [String: String] = [:]) -> Column {
  return schema_of_json(lit(json), options)
}

/// Parses a JSON string and infers its schema in DDL format.
/// - Parameters:
///   - json: A ``Column`` that evaluates to a constant JSON string.
///   - options: Options to control how the JSON is parsed. It accepts the same options as
///     the JSON data source.
/// - Returns: A ``Column`` that evaluates to a schema string in DDL format.
public func schema_of_json(_ json: Column, _ options: [String: String] = [:]) -> Column {
  return fn("schema_of_json", options: options, json)
}

/// Converts a column containing a struct, an array, a map, or a variant into a JSON string.
/// Throws an exception, in the case of an unsupported type.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a struct, an array, a map, or a variant.
///   - options: Options to control how the column is converted. It accepts the same options as
///     the JSON data source and additionally the `pretty` option.
/// - Returns: A ``Column`` that evaluates to a string.
public func to_json(_ col: Column, _ options: [String: String] = [:]) -> Column {
  return fn("to_json", options: options, col)
}
