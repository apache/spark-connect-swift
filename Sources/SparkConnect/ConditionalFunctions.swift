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

// MARK: - Conditional functions

/// Returns the first column that is not null, or null if all inputs are null.
/// - Parameter cols: ``Column``s to coalesce.
/// - Returns: A ``Column``.
public func coalesce(_ cols: Column...) -> Column {
  return fn("coalesce", cols)
}

/// Returns `col2` if `col1` is null, or `col1` otherwise.
/// - Parameters:
///   - col1: A ``Column`` to test for null.
///   - col2: A ``Column`` to return when `col1` is null.
/// - Returns: A ``Column``.
public func ifnull(_ col1: Column, _ col2: Column) -> Column {
  return fn("ifnull", col1, col2)
}

/// Returns `col1` if it is not NaN, or `col2` if `col1` is NaN.
/// Both inputs should be floating point columns (`DoubleType` or `FloatType`).
/// - Parameters:
///   - col1: A ``Column`` to test for NaN.
///   - col2: A ``Column`` to return when `col1` is NaN.
/// - Returns: A ``Column``.
public func nanvl(_ col1: Column, _ col2: Column) -> Column {
  return fn("nanvl", col1, col2)
}

/// Returns null if `col1` equals `col2`, or `col1` otherwise.
/// - Parameters:
///   - col1: A ``Column`` to return when the two columns are not equal.
///   - col2: A ``Column`` to compare with.
/// - Returns: A ``Column``.
public func nullif(_ col1: Column, _ col2: Column) -> Column {
  return fn("nullif", col1, col2)
}

/// Returns null if `col` is equal to zero, or `col` otherwise.
/// - Parameter col: A ``Column`` to test for zero.
/// - Returns: A ``Column``.
public func nullifzero(_ col: Column) -> Column {
  return fn("nullifzero", col)
}

/// Returns `col2` if `col1` is null, or `col1` otherwise.
/// - Parameters:
///   - col1: A ``Column`` to test for null.
///   - col2: A ``Column`` to return when `col1` is null.
/// - Returns: A ``Column``.
public func nvl(_ col1: Column, _ col2: Column) -> Column {
  return fn("nvl", col1, col2)
}

/// Returns `col2` if `col1` is not null, or `col3` otherwise.
/// - Parameters:
///   - col1: A ``Column`` to test for null.
///   - col2: A ``Column`` to return when `col1` is not null.
///   - col3: A ``Column`` to return when `col1` is null.
/// - Returns: A ``Column``.
public func nvl2(_ col1: Column, _ col2: Column, _ col3: Column) -> Column {
  return fn("nvl2", col1, col2, col3)
}

/// Evaluates a list of conditions and returns one of multiple possible result expressions.
/// If `otherwise` is not defined at the end, null is returned for unmatched conditions.
///
/// ```swift
/// df.select(when(col("age") > 21, "adult").otherwise("minor").alias("group"))
/// ```
/// - Parameters:
///   - condition: A condition ``Column``.
///   - value: A value ``Column`` to return when the condition is true.
/// - Returns: A ``Column``.
public func when(_ condition: Column, _ value: Column) -> Column {
  return fn("when", condition, value)
}

/// Evaluates a list of conditions and returns one of multiple possible result expressions.
/// If `otherwise` is not defined at the end, null is returned for unmatched conditions.
/// - Parameters:
///   - condition: A condition ``Column``.
///   - value: A literal value to return when the condition is true.
/// - Returns: A ``Column``.
public func when(_ condition: Column, _ value: some SparkLiteral) -> Column {
  return when(condition, value.toLiteralColumn)
}

/// Returns zero if `col` is null, or `col` otherwise.
/// - Parameter col: A ``Column`` to test for null.
/// - Returns: A ``Column``.
public func zeroifnull(_ col: Column) -> Column {
  return fn("zeroifnull", col)
}
