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

import Testing

@testable import SparkConnect

/// A test suite for `HigherOrderFunctions`
@Suite(.serialized)
struct HigherOrderFunctionsTests {

  @Test
  func higherOrderFunctionExpressions() throws {
    // (expression, function name, expected number of variables of each lambda argument)
    for (column, name, arities) in [
      (transform(col("a")) { $0 }, "transform", [1]),
      (transform(col("a")) { x, i in x + i }, "transform", [2]),
      (exists(col("a")) { $0 }, "exists", [1]),
      (forall(col("a")) { $0 }, "forall", [1]),
      (filter(col("a")) { $0 }, "filter", [1]),
      (filter(col("a")) { x, i in x + i }, "filter", [2]),
      (aggregate(col("a"), lit(0)) { acc, x in acc + x }, "aggregate", [2, 1]),
      (aggregate(col("a"), lit(0), { acc, x in acc + x }, finish: { $0 }), "aggregate", [2, 1]),
      (reduce(col("a"), lit(0)) { acc, x in acc + x }, "reduce", [2, 1]),
      (reduce(col("a"), lit(0), { acc, x in acc + x }, finish: { $0 }), "reduce", [2, 1]),
      (zip_with(col("a"), col("b")) { x, y in x + y }, "zip_with", [2]),
      (array_sort(col("a")) { x, y in x - y }, "array_sort", [2]),
      (transform_keys(col("a")) { k, _ in k }, "transform_keys", [2]),
      (transform_values(col("a")) { _, v in v }, "transform_values", [2]),
      (map_filter(col("a")) { _, v in v }, "map_filter", [2]),
      (map_zip_with(col("a"), col("b")) { _, v1, v2 in v1 + v2 }, "map_zip_with", [3]),
    ] {
      let function = column.expr.unresolvedFunction
      #expect(function.functionName == name)
      #expect(function.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      let lambdas = function.arguments.filter { !$0.lambdaFunction.arguments.isEmpty }
      #expect(lambdas.count == arities.count)
      for (lambda, arity) in zip(lambdas, arities) {
        #expect(lambda.lambdaFunction.arguments.count == arity)
        #expect(lambda.lambdaFunction.arguments.allSatisfy { $0.nameParts.count == 1 })
      }
    }
  }

  @Test
  func lambdaVariableNamesAreUnique() throws {
    // An inner lambda must not shadow the variable of an enclosing lambda.
    let column = transform(col("a")) { x in transform(col("b")) { y in x + y } }
    let outer = column.expr.unresolvedFunction.arguments[1].lambdaFunction
    let inner = outer.function.unresolvedFunction.arguments[1].lambdaFunction
    let outerName = outer.arguments[0].nameParts[0]
    let innerName = inner.arguments[0].nameParts[0]
    #expect(outerName.hasPrefix("x_"))
    #expect(innerName.hasPrefix("x_"))
    #expect(outerName != innerName)

    // The inner body adds the outer variable to the inner one.
    let body = inner.function.unresolvedFunction.arguments
    #expect(body[0].unresolvedNamedLambdaVariable.nameParts == [outerName])
    #expect(body[1].unresolvedNamedLambdaVariable.nameParts == [innerName])
  }

  @Test
  func selectHigherOrderArrayFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let arr = array(lit(1), lit(2), lit(3))

    #expect(
      try await df.select(transform(arr) { $0 * 2 }.cast("string")).collect() == [Row("[2, 4, 6]")])
    #expect(
      try await df.select(transform(arr) { x, i in x + i }.cast("string")).collect()
        == [Row("[1, 3, 5]")])
    #expect(
      try await df.select(filter(arr) { $0 % 2 == 1 }.cast("string")).collect() == [Row("[1, 3]")])
    #expect(
      try await df.select(filter(arr) { _, i in i < 2 }.cast("string")).collect()
        == [Row("[1, 2]")])
    #expect(try await df.select(exists(arr) { $0 > 2 }).collect() == [Row(true)])
    #expect(try await df.select(exists(arr) { $0 > 3 }).collect() == [Row(false)])
    #expect(try await df.select(forall(arr) { $0 > 0 }).collect() == [Row(true)])
    #expect(try await df.select(forall(arr) { $0 > 1 }).collect() == [Row(false)])
    #expect(try await df.select(aggregate(arr, lit(0)) { acc, x in acc + x }).collect() == [Row(6)])
    #expect(
      try await df.select(aggregate(arr, lit(0)) { acc, x in acc + x } finish: { $0 * 10 })
        .collect() == [Row(60)])
    #expect(try await df.select(reduce(arr, lit(0)) { acc, x in acc + x }).collect() == [Row(6)])
    #expect(
      try await df.select(reduce(arr, lit(0)) { acc, x in acc + x } finish: { $0 * 10 })
        .collect() == [Row(60)])
    #expect(
      try await df.select(
        zip_with(arr, array(lit(10), lit(20), lit(30))) { x, y in x + y }.cast("string")
      ).collect() == [Row("[11, 22, 33]")])
    // The comparator must return INT.
    let descending = array_sort(array(lit(1), lit(3), lit(2))) { x, y in (y - x).cast("int") }
    #expect(try await df.select(descending.cast("string")).collect() == [Row("[3, 2, 1]")])
    await spark.stop()
  }

  @Test
  func selectNestedLambda() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    // The inner lambda body references the enclosing lambda variable.
    let nested = transform(array(lit(1), lit(2))) { x in
      transform(array(lit(10), lit(20))) { y in x + y }
    }
    #expect(try await df.select(nested.cast("string")).collect() == [Row("[[11, 21], [12, 22]]")])
    await spark.stop()
  }

  @Test
  func selectHigherOrderMapFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let m = map_from_arrays(array(lit(1), lit(2)), array(lit(10), lit(20)))

    #expect(
      try await df.select(map_filter(m) { _, v in v > 10 }.cast("string")).collect()
        == [Row("{2 -> 20}")])
    #expect(
      try await df.select(transform_keys(m) { k, v in k + v }.cast("string")).collect()
        == [Row("{11 -> 10, 22 -> 20}")])
    #expect(
      try await df.select(transform_values(m) { k, v in k + v }.cast("string")).collect()
        == [Row("{1 -> 11, 2 -> 22}")])
    let m2 = map_from_arrays(array(lit(1), lit(2)), array(lit(100), lit(200)))
    let zipped = map_zip_with(m, m2) { _, v1, v2 in v1 + v2 }
    #expect(try await df.select(zipped.cast("string")).collect() == [Row("{1 -> 110, 2 -> 220}")])
    await spark.stop()
  }
}
