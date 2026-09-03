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

// MARK: - Predicate functions

/// Returns true if `col1` is equal to `col2`, treating two nulls as equal.
/// Unlike the `==` operator, this never returns null.
/// - Parameters:
///   - col1: A ``Column`` to compare.
///   - col2: A ``Column`` to compare with.
/// - Returns: A ``Column``.
public func equal_null(_ col1: Column, _ col2: Column) -> Column {
  return fn("equal_null", col1, col2)
}

/// Returns true if `str` matches `pattern` case-insensitively, null if any argument is null,
/// false otherwise. The default escape character is `\`.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - pattern: A SQL `LIKE` pattern ``Column``.
/// - Returns: A ``Column``.
public func ilike(_ str: Column, _ pattern: Column) -> Column {
  return fn("ilike", str, pattern)
}

/// Returns true if `str` matches `pattern` with `escapeChar` case-insensitively, null if any
/// argument is null, false otherwise.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - pattern: A SQL `LIKE` pattern ``Column``.
///   - escapeChar: An escape character ``Column``. Must be a constant.
/// - Returns: A ``Column``.
public func ilike(_ str: Column, _ pattern: Column, _ escapeChar: Column) -> Column {
  return fn("ilike", str, pattern, escapeChar)
}

/// Returns true if `col` is NaN.
/// - Parameter col: A ``Column`` to check.
/// - Returns: A ``Column``.
public func isnan(_ col: Column) -> Column {
  return fn("isnan", col)
}

/// Returns true if `col` is not null.
/// - Parameter col: A ``Column`` to check.
/// - Returns: A ``Column``.
public func isnotnull(_ col: Column) -> Column {
  return fn("isnotnull", col)
}

/// Returns true if `col` is null.
/// - Parameter col: A ``Column`` to check.
/// - Returns: A ``Column``.
public func isnull(_ col: Column) -> Column {
  return fn("isnull", col)
}

/// Returns true if `str` matches `pattern`, null if any argument is null, false otherwise.
/// The default escape character is `\`.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - pattern: A SQL `LIKE` pattern ``Column``.
/// - Returns: A ``Column``.
public func like(_ str: Column, _ pattern: Column) -> Column {
  return fn("like", str, pattern)
}

/// Returns true if `str` matches `pattern` with `escapeChar`, null if any argument is null,
/// false otherwise.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - pattern: A SQL `LIKE` pattern ``Column``.
///   - escapeChar: An escape character ``Column``. Must be a constant.
/// - Returns: A ``Column``.
public func like(_ str: Column, _ pattern: Column, _ escapeChar: Column) -> Column {
  return fn("like", str, pattern, escapeChar)
}

/// Returns true if `str` matches `regexp`, or false otherwise.
/// This is an alias of ``rlike(_:_:)``.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
/// - Returns: A ``Column``.
public func regexp(_ str: Column, _ regexp: Column) -> Column {
  return fn("regexp", str, regexp)
}

/// Returns true if `str` matches `regexp`, or false otherwise.
/// This is an alias of ``rlike(_:_:)``.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
/// - Returns: A ``Column``.
public func regexp_like(_ str: Column, _ regexp: Column) -> Column {
  return fn("regexp_like", str, regexp)
}

/// Returns true if `str` matches `regexp`, or false otherwise.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
/// - Returns: A ``Column``.
public func rlike(_ str: Column, _ regexp: Column) -> Column {
  return fn("rlike", str, regexp)
}
