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

// MARK: - VARIANT functions

/// Parses a JSON string and constructs a `VARIANT` value.
/// Throws an exception, in the case of an invalid JSON string.
/// - Parameter json: A ``Column`` that evaluates to a string containing JSON data.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func parse_json(_ json: Column) -> Column {
  return fn("parse_json", json)
}

/// Parses a JSON string and constructs a `VARIANT` value.
/// Returns `NULL`, in the case of an invalid JSON string.
/// - Parameter json: A ``Column`` that evaluates to a string containing JSON data.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func try_parse_json(_ json: Column) -> Column {
  return fn("try_parse_json", json)
}

/// Converts a column containing nested inputs, i.e. an array, a map, or a struct, into a `VARIANT`
/// where maps and structs are converted to `VARIANT` objects which are unordered unlike SQL
/// structs. Input maps can only have string keys.
/// - Parameter col: A ``Column`` that evaluates to an array, a map, a struct, or a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func to_variant_object(_ col: Column) -> Column {
  return fn("to_variant_object", col)
}

/// Checks if a `VARIANT` value is a `VARIANT` null. Returns `true` if and only if the input is a
/// `VARIANT` null and `false` otherwise, including in the case of a SQL `NULL`.
/// - Parameter v: A ``Column`` that evaluates to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a boolean.
public func is_variant_null(_ v: Column) -> Column {
  return fn("is_variant_null", v)
}

/// Checks if a `VARIANT` value is valid. Returns `true` if the `VARIANT` is valid, `false` if it
/// is malformed, and `NULL` if the input is `NULL`.
/// - Parameter v: A ``Column`` that evaluates to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a boolean.
public func is_valid_variant(_ v: Column) -> Column {
  return fn("is_valid_variant", v)
}

/// Extracts a sub-`VARIANT` from `v` according to `path`, and then casts the sub-`VARIANT` to
/// `targetType`. Returns `NULL` if the path does not exist.
/// Throws an exception, if the cast fails.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: An extraction path. A valid path starts with `$` and is followed by zero or more
///     segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///   - targetType: The target data type to cast into, in a DDL-formatted string.
/// - Returns: A ``Column`` of the type specified by `targetType`.
public func variant_get(_ v: Column, _ path: String, _ targetType: String) -> Column {
  return variant_get(v, lit(path), targetType)
}

/// Extracts a sub-`VARIANT` from `v` according to `path`, and then casts the sub-`VARIANT` to
/// `targetType`. Returns `NULL` if the path does not exist.
/// Throws an exception, if the cast fails.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A ``Column`` that evaluates to an extraction path. A valid path starts with `$` and
///     is followed by zero or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///   - targetType: The target data type to cast into, in a DDL-formatted string.
/// - Returns: A ``Column`` of the type specified by `targetType`.
public func variant_get(_ v: Column, _ path: Column, _ targetType: String) -> Column {
  return fn("variant_get", v, path, lit(targetType))
}

/// Extracts a sub-`VARIANT` from `v` according to `path`, and then casts the sub-`VARIANT` to
/// `targetType`. Returns `NULL` if the path does not exist or the cast fails.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: An extraction path. A valid path starts with `$` and is followed by zero or more
///     segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///   - targetType: The target data type to cast into, in a DDL-formatted string.
/// - Returns: A ``Column`` of the type specified by `targetType`.
public func try_variant_get(_ v: Column, _ path: String, _ targetType: String) -> Column {
  return try_variant_get(v, lit(path), targetType)
}

/// Extracts a sub-`VARIANT` from `v` according to `path`, and then casts the sub-`VARIANT` to
/// `targetType`. Returns `NULL` if the path does not exist or the cast fails.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A ``Column`` that evaluates to an extraction path. A valid path starts with `$` and
///     is followed by zero or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///   - targetType: The target data type to cast into, in a DDL-formatted string.
/// - Returns: A ``Column`` of the type specified by `targetType`.
public func try_variant_get(_ v: Column, _ path: Column, _ targetType: String) -> Column {
  return fn("try_variant_get", v, path, lit(targetType))
}

/// Returns the schema of a `VARIANT` in the SQL format.
/// - Parameter v: A ``Column`` that evaluates to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a string.
public func schema_of_variant(_ v: Column) -> Column {
  return fn("schema_of_variant", v)
}

/// Inserts a value into a `VARIANT` at the given `JSONPath` location. An object path adds a new
/// field; an array path inserts at the index, shifting later elements right. Missing intermediate
/// keys are created. Returns `NULL` if any argument is `NULL`.
/// Throws an exception, if the field already exists or a path segment hits a value of an
/// incompatible type.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A `JSONPath` string identifying the insertion target. A valid path starts with `$`
///     and is followed by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///     The root path `$` is not allowed.
///   - value: The value to insert. Any expression castable to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func variant_insert(_ v: Column, _ path: String, _ value: Column) -> Column {
  return variant_insert(v, lit(path), value)
}

/// Inserts a value into a `VARIANT` at the given `JSONPath` location. An object path adds a new
/// field; an array path inserts at the index, shifting later elements right. Missing intermediate
/// keys are created. Returns `NULL` if any argument is `NULL`.
/// Throws an exception, if the field already exists or a path segment hits a value of an
/// incompatible type.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A ``Column`` that evaluates to a `JSONPath` string identifying the insertion target.
///     A valid path starts with `$` and is followed by one or more segments like `[123]`,
///     `.name`, `['name']`, or `["name"]`. The root path `$` is not allowed.
///   - value: The value to insert. Any expression castable to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func variant_insert(_ v: Column, _ path: Column, _ value: Column) -> Column {
  return fn("variant_insert", v, path, value)
}

/// Inserts a value into a `VARIANT` at the given `JSONPath` location. An object path adds a new
/// field; an array path inserts at the index, shifting later elements right. Missing intermediate
/// keys are created. Returns `NULL` if the field already exists, a path segment hits a value of
/// an incompatible type, or any argument is `NULL`.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A `JSONPath` string identifying the insertion target. A valid path starts with `$`
///     and is followed by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///     The root path `$` is not allowed.
///   - value: The value to insert. Any expression castable to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func try_variant_insert(_ v: Column, _ path: String, _ value: Column) -> Column {
  return try_variant_insert(v, lit(path), value)
}

/// Inserts a value into a `VARIANT` at the given `JSONPath` location. An object path adds a new
/// field; an array path inserts at the index, shifting later elements right. Missing intermediate
/// keys are created. Returns `NULL` if the field already exists, a path segment hits a value of
/// an incompatible type, or any argument is `NULL`.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A ``Column`` that evaluates to a `JSONPath` string identifying the insertion target.
///     A valid path starts with `$` and is followed by one or more segments like `[123]`,
///     `.name`, `['name']`, or `["name"]`. The root path `$` is not allowed.
///   - value: The value to insert. Any expression castable to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func try_variant_insert(_ v: Column, _ path: Column, _ value: Column) -> Column {
  return fn("try_variant_insert", v, path, value)
}

/// Sets or upserts a value in a `VARIANT` at the given `JSONPath` location. An existing object
/// field or array element at the target is replaced. A missing field, array index, or
/// intermediate path is created, unless `createIfMissing` is `false`, in which case the `VARIANT`
/// is left unchanged. Returns `NULL` if any argument is `NULL`.
/// Throws an exception, if a path segment hits a value of an incompatible type.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A `JSONPath` string identifying the set target. A valid path starts with `$` and is
///     followed by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///     The root path `$` is not allowed.
///   - value: The value to set. Any expression castable to a `VARIANT`.
///   - createIfMissing: Whether to create missing keys or out-of-range array indices
///     (default = `true`).
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func variant_set(
  _ v: Column, _ path: String, _ value: Column, _ createIfMissing: Bool = true
) -> Column {
  return variant_set(v, lit(path), value, createIfMissing)
}

/// Sets or upserts a value in a `VARIANT` at the given `JSONPath` location. An existing object
/// field or array element at the target is replaced. A missing field, array index, or
/// intermediate path is created, unless `createIfMissing` is `false`, in which case the `VARIANT`
/// is left unchanged. Returns `NULL` if any argument is `NULL`.
/// Throws an exception, if a path segment hits a value of an incompatible type.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A ``Column`` that evaluates to a `JSONPath` string identifying the set target.
///     A valid path starts with `$` and is followed by one or more segments like `[123]`,
///     `.name`, `['name']`, or `["name"]`. The root path `$` is not allowed.
///   - value: The value to set. Any expression castable to a `VARIANT`.
///   - createIfMissing: Whether to create missing keys or out-of-range array indices
///     (default = `true`).
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func variant_set(
  _ v: Column, _ path: Column, _ value: Column, _ createIfMissing: Bool = true
) -> Column {
  return fn("variant_set", v, path, value, lit(createIfMissing))
}

/// Sets or upserts a value in a `VARIANT` at the given `JSONPath` location. An existing object
/// field or array element at the target is replaced. A missing field, array index, or
/// intermediate path is created, unless `createIfMissing` is `false`, in which case the `VARIANT`
/// is left unchanged. Returns `NULL` if a path segment hits a value of an incompatible type, or
/// if any argument is `NULL`.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A `JSONPath` string identifying the set target. A valid path starts with `$` and is
///     followed by one or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///     The root path `$` is not allowed.
///   - value: The value to set. Any expression castable to a `VARIANT`.
///   - createIfMissing: Whether to create missing keys or out-of-range array indices
///     (default = `true`).
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func try_variant_set(
  _ v: Column, _ path: String, _ value: Column, _ createIfMissing: Bool = true
) -> Column {
  return try_variant_set(v, lit(path), value, createIfMissing)
}

/// Sets or upserts a value in a `VARIANT` at the given `JSONPath` location. An existing object
/// field or array element at the target is replaced. A missing field, array index, or
/// intermediate path is created, unless `createIfMissing` is `false`, in which case the `VARIANT`
/// is left unchanged. Returns `NULL` if a path segment hits a value of an incompatible type, or
/// if any argument is `NULL`.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A ``Column`` that evaluates to a `JSONPath` string identifying the set target.
///     A valid path starts with `$` and is followed by one or more segments like `[123]`,
///     `.name`, `['name']`, or `["name"]`. The root path `$` is not allowed.
///   - value: The value to set. Any expression castable to a `VARIANT`.
///   - createIfMissing: Whether to create missing keys or out-of-range array indices
///     (default = `true`).
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func try_variant_set(
  _ v: Column, _ path: Column, _ value: Column, _ createIfMissing: Bool = true
) -> Column {
  return fn("try_variant_set", v, path, value, lit(createIfMissing))
}

/// Appends a value to the array in a `VARIANT` at the given `JSONPath` location. Returns the
/// `VARIANT` unchanged if a path key or index is absent. Returns `NULL` if any argument is `NULL`.
/// Throws an exception, if a path segment hits a value of an incompatible type or the target is
/// not an array.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A `JSONPath` string identifying the target array. A valid path starts with `$` and
///     is followed by zero or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///   - value: The value to append. Any expression castable to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func variant_array_append(_ v: Column, _ path: String, _ value: Column) -> Column {
  return variant_array_append(v, lit(path), value)
}

/// Appends a value to the array in a `VARIANT` at the given `JSONPath` location. Returns the
/// `VARIANT` unchanged if a path key or index is absent. Returns `NULL` if any argument is `NULL`.
/// Throws an exception, if a path segment hits a value of an incompatible type or the target is
/// not an array.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A ``Column`` that evaluates to a `JSONPath` string identifying the target array.
///     A valid path starts with `$` and is followed by zero or more segments like `[123]`,
///     `.name`, `['name']`, or `["name"]`.
///   - value: The value to append. Any expression castable to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func variant_array_append(_ v: Column, _ path: Column, _ value: Column) -> Column {
  return fn("variant_array_append", v, path, value)
}

/// Appends a value to the array in a `VARIANT` at the given `JSONPath` location. Returns the
/// `VARIANT` unchanged if a path key or index is absent. Returns `NULL` if a path segment hits a
/// value of an incompatible type, the target is not an array, or any argument is `NULL`.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A `JSONPath` string identifying the target array. A valid path starts with `$` and
///     is followed by zero or more segments like `[123]`, `.name`, `['name']`, or `["name"]`.
///   - value: The value to append. Any expression castable to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func try_variant_array_append(_ v: Column, _ path: String, _ value: Column) -> Column {
  return try_variant_array_append(v, lit(path), value)
}

/// Appends a value to the array in a `VARIANT` at the given `JSONPath` location. Returns the
/// `VARIANT` unchanged if a path key or index is absent. Returns `NULL` if a path segment hits a
/// value of an incompatible type, the target is not an array, or any argument is `NULL`.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - path: A ``Column`` that evaluates to a `JSONPath` string identifying the target array.
///     A valid path starts with `$` and is followed by zero or more segments like `[123]`,
///     `.name`, `['name']`, or `["name"]`.
///   - value: The value to append. Any expression castable to a `VARIANT`.
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func try_variant_array_append(_ v: Column, _ path: Column, _ value: Column) -> Column {
  return fn("try_variant_array_append", v, path, value)
}

/// Recursively removes object fields and array elements whose value is a `VARIANT` null, unless
/// `includeArrays` is `false`, in which case null array elements are kept.
/// Returns `NULL` if any argument is `NULL`.
/// - Parameters:
///   - v: A ``Column`` that evaluates to a `VARIANT`.
///   - includeArrays: Whether null elements are also removed from arrays (default = `true`).
/// - Returns: A ``Column`` that evaluates to a `VARIANT`.
public func variant_strip_nulls(_ v: Column, _ includeArrays: Bool = true) -> Column {
  return fn("variant_strip_nulls", v, lit(includeArrays))
}
