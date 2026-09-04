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

import Synchronization

// MARK: - Higher-order functions
//
// These functions take a Swift closure and turn it into a Spark Connect lambda expression. The
// closures are non-escaping because they are invoked immediately while the expression is built
// and never stored. The single-argument ``array_sort(_:)`` lives in `CollectionFunctions.swift`.

/// A monotonically increasing counter used to give every lambda variable a unique name.
private let lambdaVariableID = Atomic<Int>(0)

/// Creates an `UnresolvedNamedLambdaVariable` whose name is `prefix` suffixed with a unique id.
///
/// Lambda variable name prefixes (`x`, `y`, `z`) are reused across higher-order functions, so a
/// unique suffix is required to keep an inner lambda from shadowing an enclosing one.
private func lambdaVariable(_ prefix: String) -> Column {
  let id = lambdaVariableID.wrappingAdd(1, ordering: .relaxed).newValue
  var variable = Spark_Connect_Expression.UnresolvedNamedLambdaVariable()
  variable.nameParts = ["\(prefix)_\(id)"]
  var expr = Spark_Connect_Expression()
  expr.unresolvedNamedLambdaVariable = variable
  return Column(expr)
}

/// Builds a `LambdaFunction` ``Column`` from `arguments` and the given lambda `body`.
private func lambda(_ arguments: [Column], _ body: Column) -> Column {
  var function = Spark_Connect_Expression.LambdaFunction()
  function.function = body.expr
  function.arguments = arguments.map { $0.expr.unresolvedNamedLambdaVariable }
  var expr = Spark_Connect_Expression()
  expr.lambdaFunction = function
  return Column(expr)
}

/// Converts a single-argument Swift closure into a lambda expression ``Column``.
func createLambda(_ f: (Column) -> Column) -> Column {
  let x = lambdaVariable("x")
  return lambda([x], f(x))
}

/// Converts a two-argument Swift closure into a lambda expression ``Column``.
func createLambda(_ f: (Column, Column) -> Column) -> Column {
  let x = lambdaVariable("x")
  let y = lambdaVariable("y")
  return lambda([x, y], f(x, y))
}

/// Converts a three-argument Swift closure into a lambda expression ``Column``.
func createLambda(_ f: (Column, Column, Column) -> Column) -> Column {
  let x = lambdaVariable("x")
  let y = lambdaVariable("y")
  let z = lambdaVariable("z")
  return lambda([x, y, z], f(x, y, z))
}

/// Returns an array of elements after applying a transformation to each element in the input
/// array.
///
/// ```swift
/// let df2 = df.select(transform(col("i")) { $0 + 1 })
/// ```
/// - Parameters:
///   - column: An array ``Column``.
///   - f: A closure applied to each array element.
/// - Returns: A ``Column``.
public func transform(_ column: Column, _ f: (Column) -> Column) -> Column {
  return fn("transform", column, createLambda(f))
}

/// Returns an array of elements after applying a transformation to each element in the input
/// array.
///
/// ```swift
/// let df2 = df.select(transform(col("i")) { x, i in x + i })
/// ```
/// - Parameters:
///   - column: An array ``Column``.
///   - f: A closure applied to each array element and its 0-based index.
/// - Returns: A ``Column``.
public func transform(_ column: Column, _ f: (Column, Column) -> Column) -> Column {
  return fn("transform", column, createLambda(f))
}

/// Returns whether a predicate holds for one or more elements in the array.
/// - Parameters:
///   - column: An array ``Column``.
///   - f: A predicate closure applied to each array element.
/// - Returns: A ``Column``.
public func exists(_ column: Column, _ f: (Column) -> Column) -> Column {
  return fn("exists", column, createLambda(f))
}

/// Returns whether a predicate holds for every element in the array.
/// - Parameters:
///   - column: An array ``Column``.
///   - f: A predicate closure applied to each array element.
/// - Returns: A ``Column``.
public func forall(_ column: Column, _ f: (Column) -> Column) -> Column {
  return fn("forall", column, createLambda(f))
}

/// Returns an array of elements for which a predicate holds in the given array.
///
/// ```swift
/// let df2 = df.select(filter(col("i")) { $0 % 2 == 0 })
/// ```
/// - Parameters:
///   - column: An array ``Column``.
///   - f: A predicate closure applied to each array element.
/// - Returns: A ``Column``.
public func filter(_ column: Column, _ f: (Column) -> Column) -> Column {
  return fn("filter", column, createLambda(f))
}

/// Returns an array of elements for which a predicate holds in the given array.
///
/// ```swift
/// let df2 = df.select(filter(col("i")) { _, i in i % 2 == 0 })
/// ```
/// - Parameters:
///   - column: An array ``Column``.
///   - f: A predicate closure applied to each array element and its 0-based index.
/// - Returns: A ``Column``.
public func filter(_ column: Column, _ f: (Column, Column) -> Column) -> Column {
  return fn("filter", column, createLambda(f))
}

/// Applies a binary operator to an initial state and all elements in the array, and reduces this
/// to a single state. The final state is converted into the final result by applying a finish
/// function.
/// - Parameters:
///   - expr: An array ``Column``.
///   - initialValue: The initial value ``Column``.
///   - merge: A closure taking the combined value and an input value, returning a combined value.
///   - finish: A closure converting the combined value to the final result.
/// - Returns: A ``Column``.
public func aggregate(
  _ expr: Column, _ initialValue: Column, _ merge: (Column, Column) -> Column,
  finish: (Column) -> Column
) -> Column {
  return fn("aggregate", expr, initialValue, createLambda(merge), createLambda(finish))
}

/// Applies a binary operator to an initial state and all elements in the array, and reduces this
/// to a single state.
///
/// ```swift
/// let df2 = df.select(aggregate(col("i"), lit(0)) { acc, x in acc + x })
/// ```
/// - Parameters:
///   - expr: An array ``Column``.
///   - initialValue: The initial value ``Column``.
///   - merge: A closure taking the combined value and an input value, returning a combined value.
/// - Returns: A ``Column``.
public func aggregate(
  _ expr: Column, _ initialValue: Column, _ merge: (Column, Column) -> Column
) -> Column {
  return aggregate(expr, initialValue, merge, finish: { $0 })
}

/// Applies a binary operator to an initial state and all elements in the array, and reduces this
/// to a single state. The final state is converted into the final result by applying a finish
/// function.
/// - Parameters:
///   - expr: An array ``Column``.
///   - initialValue: The initial value ``Column``.
///   - merge: A closure taking the combined value and an input value, returning a combined value.
///   - finish: A closure converting the combined value to the final result.
/// - Returns: A ``Column``.
public func reduce(
  _ expr: Column, _ initialValue: Column, _ merge: (Column, Column) -> Column,
  finish: (Column) -> Column
) -> Column {
  return fn("reduce", expr, initialValue, createLambda(merge), createLambda(finish))
}

/// Applies a binary operator to an initial state and all elements in the array, and reduces this
/// to a single state.
///
/// ```swift
/// let df2 = df.select(reduce(col("i"), lit(0)) { acc, x in acc + x })
/// ```
/// - Parameters:
///   - expr: An array ``Column``.
///   - initialValue: The initial value ``Column``.
///   - merge: A closure taking the combined value and an input value, returning a combined value.
/// - Returns: A ``Column``.
public func reduce(
  _ expr: Column, _ initialValue: Column, _ merge: (Column, Column) -> Column
) -> Column {
  return reduce(expr, initialValue, merge, finish: { $0 })
}

/// Merges two given arrays, element-wise, into a single array using a function. If one array is
/// shorter, nulls are appended at the end to match the length of the longer array, before
/// applying the function.
///
/// ```swift
/// let df2 = df.select(zip_with(col("val1"), col("val2")) { x, y in x + y })
/// ```
/// - Parameters:
///   - left: An array ``Column``.
///   - right: An array ``Column``.
///   - f: A closure taking an element of `left` and the matching element of `right`.
/// - Returns: A ``Column``.
public func zip_with(_ left: Column, _ right: Column, _ f: (Column, Column) -> Column) -> Column {
  return fn("zip_with", left, right, createLambda(f))
}

/// Sorts the input array based on the given comparator function. The comparator will take two
/// arguments representing two elements of the array. It returns a negative integer, 0, or a
/// positive integer as the first element is less than, equal to, or greater than the second
/// element. If the comparator function returns null, the function will fail and raise an error.
///
/// The comparator must return an `INT`. Swift `Int` literals are `BIGINT`, so cast the result
/// when it is derived from them, e.g. `array_sort(col("a")) { x, y in (y - x).cast("int") }`.
/// - Parameters:
///   - col: An array ``Column``.
///   - comparator: A binary comparator closure.
/// - Returns: A ``Column``.
public func array_sort(_ col: Column, _ comparator: (Column, Column) -> Column) -> Column {
  return fn("array_sort", col, createLambda(comparator))
}

/// Applies a function to every key-value pair in a map and returns a map with the results of
/// those applications as the new keys for the pairs.
///
/// ```swift
/// let df2 = df.select(transform_keys(col("m")) { k, v in k + 1 })
/// ```
/// - Parameters:
///   - expr: A map ``Column``.
///   - f: A closure taking a key and its value, returning a new key.
/// - Returns: A ``Column``.
public func transform_keys(_ expr: Column, _ f: (Column, Column) -> Column) -> Column {
  return fn("transform_keys", expr, createLambda(f))
}

/// Applies a function to every key-value pair in a map and returns a map with the results of
/// those applications as the new values for the pairs.
///
/// ```swift
/// let df2 = df.select(transform_values(col("m")) { k, v in k + v })
/// ```
/// - Parameters:
///   - expr: A map ``Column``.
///   - f: A closure taking a key and its value, returning a new value.
/// - Returns: A ``Column``.
public func transform_values(_ expr: Column, _ f: (Column, Column) -> Column) -> Column {
  return fn("transform_values", expr, createLambda(f))
}

/// Returns a map whose key-value pairs satisfy a predicate.
///
/// ```swift
/// let df2 = df.select(map_filter(col("m")) { k, v in v > 30 })
/// ```
/// - Parameters:
///   - expr: A map ``Column``.
///   - f: A predicate closure taking a key and its value.
/// - Returns: A ``Column``.
public func map_filter(_ expr: Column, _ f: (Column, Column) -> Column) -> Column {
  return fn("map_filter", expr, createLambda(f))
}

/// Merges two given maps, key-wise, into a single map using a function. If one map does not have
/// a matching key, null is passed for the missing value.
///
/// ```swift
/// let df2 = df.select(map_zip_with(col("m1"), col("m2")) { k, v1, v2 in v1 + v2 })
/// ```
/// - Parameters:
///   - left: A map ``Column``.
///   - right: A map ``Column``.
///   - f: A closure taking a key, the value from `left`, and the value from `right`.
/// - Returns: A ``Column``.
public func map_zip_with(
  _ left: Column, _ right: Column, _ f: (Column, Column, Column) -> Column
) -> Column {
  return fn("map_zip_with", left, right, createLambda(f))
}
