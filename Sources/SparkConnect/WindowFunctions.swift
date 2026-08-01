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

/// Returns the cumulative distribution of values within a window partition, i.e. the fraction
/// of rows that are below the current row.
/// - Returns: A ``Column``.
public func cume_dist() -> Column {
  return fn("cume_dist")
}

/// Returns the rank of rows within a window partition, without any gaps.
///
/// The difference between ``rank()`` and ``dense_rank()`` is that `dense_rank` leaves no gaps in
/// ranking sequence when there are ties. That is, if you were ranking a competition using
/// `dense_rank` and had three people tie for second place, you would say that all three were in
/// second place and that the next person came in third.
/// - Returns: A ``Column``.
public func dense_rank() -> Column {
  return fn("dense_rank")
}

/// Returns the value that is `offset` rows before the current row, and null if there is less
/// than `offset` rows before the current row.
/// - Parameters:
///   - e: A ``Column`` to get the value of.
///   - offset: A number of rows back from the current row from which to obtain a value.
/// - Returns: A ``Column``.
public func lag(_ e: Column, _ offset: Int32) -> Column {
  return fn("lag", e, lit(offset))
}

/// Returns the value that is `offset` rows before the current row, and `defaultValue` if there
/// is less than `offset` rows before the current row.
/// - Parameters:
///   - e: A ``Column`` to get the value of.
///   - offset: A number of rows back from the current row from which to obtain a value.
///   - defaultValue: A default literal value.
/// - Returns: A ``Column``.
public func lag(_ e: Column, _ offset: Int32, _ defaultValue: some SparkLiteral) -> Column {
  return fn("lag", e, lit(offset), defaultValue.toLiteralColumn)
}

/// Returns the value that is `offset` rows after the current row, and null if there is less
/// than `offset` rows after the current row.
/// - Parameters:
///   - e: A ``Column`` to get the value of.
///   - offset: A number of rows after the current row from which to obtain a value.
/// - Returns: A ``Column``.
public func lead(_ e: Column, _ offset: Int32) -> Column {
  return fn("lead", e, lit(offset))
}

/// Returns the value that is `offset` rows after the current row, and `defaultValue` if there
/// is less than `offset` rows after the current row.
/// - Parameters:
///   - e: A ``Column`` to get the value of.
///   - offset: A number of rows after the current row from which to obtain a value.
///   - defaultValue: A default literal value.
/// - Returns: A ``Column``.
public func lead(_ e: Column, _ offset: Int32, _ defaultValue: some SparkLiteral) -> Column {
  return fn("lead", e, lit(offset), defaultValue.toLiteralColumn)
}

/// Returns the value that is the `offset`th row of the window frame (counting from 1), and null
/// if the size of window frame is less than `offset` rows.
/// - Parameters:
///   - e: A ``Column`` to get the value of.
///   - offset: A position of the row within the window frame, counting from 1.
/// - Returns: A ``Column``.
public func nth_value(_ e: Column, _ offset: Int32) -> Column {
  return fn("nth_value", e, lit(offset))
}

/// Returns the ntile group id (from 1 to `n` inclusive) in an ordered window partition. For
/// example, if `n` is 4, the first quarter of the rows will get value 1, the second quarter will
/// get 2, the third quarter will get 3, and the last quarter will get 4.
/// - Parameter n: A number of buckets.
/// - Returns: A ``Column``.
public func ntile(_ n: Int32) -> Column {
  return fn("ntile", lit(n))
}

/// Returns the relative rank (i.e. percentile) of rows within a window partition.
/// - Returns: A ``Column``.
public func percent_rank() -> Column {
  return fn("percent_rank")
}

/// Returns the rank of rows within a window partition.
///
/// The difference between ``rank()`` and ``dense_rank()`` is that `dense_rank` leaves no gaps in
/// ranking sequence when there are ties. That is, if you were ranking a competition using
/// `dense_rank` and had three people tie for second place, you would say that all three were in
/// second place and that the next person came in third.
/// - Returns: A ``Column``.
public func rank() -> Column {
  return fn("rank")
}

/// Returns a sequential number starting at 1 within a window partition.
/// - Returns: A ``Column``.
public func row_number() -> Column {
  return fn("row_number")
}
