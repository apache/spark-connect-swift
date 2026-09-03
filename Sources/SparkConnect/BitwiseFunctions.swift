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

// MARK: - Bitwise functions

/// Returns the number of bits that are set in the given value as an unsigned 64-bit integer,
/// or `NULL` if the argument is `NULL`.
/// - Parameter col: A ``Column`` that evaluates to an integral or boolean.
/// - Returns: A ``Column``.
public func bit_count(_ col: Column) -> Column {
  return fn("bit_count", col)
}

/// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered
/// from right to left, starting at zero. The position argument cannot be negative.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an integral.
///   - pos: A ``Column`` for the bit position, numbered from right to left starting at zero.
/// - Returns: A ``Column``.
public func bit_get(_ col: Column, _ pos: Column) -> Column {
  return fn("bit_get", col, pos)
}

/// Computes bitwise NOT (`~`) of the given value.
/// - Parameter col: A ``Column`` that evaluates to an integral.
/// - Returns: A ``Column`` of the same type as the input.
public func bitwise_not(_ col: Column) -> Column {
  return fn("~", col)
}

/// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered
/// from right to left, starting at zero. The position argument cannot be negative.
/// This is an alias of ``bit_get(_:_:)``.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an integral.
///   - pos: A ``Column`` for the bit position, numbered from right to left starting at zero.
/// - Returns: A ``Column``.
public func getbit(_ col: Column, _ pos: Column) -> Column {
  return fn("getbit", col, pos)
}

/// Shifts the given value `numBits` left.
/// - Parameters:
///   - col: A ``Column``.
///   - numBits: The number of bits to shift.
/// - Returns: A ``Column``.
public func shiftleft(_ col: Column, _ numBits: Int32) -> Column {
  return fn("shiftleft", col, lit(numBits))
}

/// (Signed) shifts the given value `numBits` right.
/// - Parameters:
///   - col: A ``Column``.
///   - numBits: The number of bits to shift.
/// - Returns: A ``Column``.
public func shiftright(_ col: Column, _ numBits: Int32) -> Column {
  return fn("shiftright", col, lit(numBits))
}

/// (Unsigned) shifts the given value `numBits` right.
/// - Parameters:
///   - col: A ``Column``.
///   - numBits: The number of bits to shift.
/// - Returns: A ``Column``.
public func shiftrightunsigned(_ col: Column, _ numBits: Int32) -> Column {
  return fn("shiftrightunsigned", col, lit(numBits))
}
