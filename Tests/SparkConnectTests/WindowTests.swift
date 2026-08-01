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

/// A test suite for `Window`, `WindowSpec`, and window functions
@Suite(.serialized)
struct WindowTests {

  @Test
  func windowFunctions() throws {
    for (column, name) in [
      (cume_dist(), "cume_dist"),
      (dense_rank(), "dense_rank"),
      (percent_rank(), "percent_rank"),
      (rank(), "rank"),
      (row_number(), "row_number"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.isEmpty)
    }

    #expect(ntile(4).expr.unresolvedFunction.functionName == "ntile")
    #expect(ntile(4).expr.unresolvedFunction.arguments[0].literal.integer == 4)

    for (column, name, count) in [
      (lag(col("a"), 1), "lag", 2),
      (lag(col("a"), 1, 0), "lag", 3),
      (lead(col("a"), 1), "lead", 2),
      (lead(col("a"), 1, 0), "lead", 3),
      (nth_value(col("a"), 2), "nth_value", 2),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].literal.integer != 0)
    }
  }

  @Test
  func over() throws {
    let window = Window.partitionBy("dept").orderBy(col("salary").desc())
    let expr = rank().over(window).expr
    #expect(expr.window.windowFunction.unresolvedFunction.functionName == "rank")
    #expect(expr.window.partitionSpec.count == 1)
    #expect(expr.window.partitionSpec[0].unresolvedAttribute.unparsedIdentifier == "dept")
    #expect(expr.window.orderSpec.count == 1)
    #expect(expr.window.orderSpec[0].child.unresolvedAttribute.unparsedIdentifier == "salary")
    #expect(expr.window.orderSpec[0].direction == .descending)
    #expect(!expr.window.hasFrameSpec)

    let ascending = Window.orderBy("a", "b")
    let ascendingExpr = row_number().over(ascending).expr
    #expect(ascendingExpr.window.partitionSpec.isEmpty)
    #expect(ascendingExpr.window.orderSpec.count == 2)
    #expect(ascendingExpr.window.orderSpec[0].direction == .ascending)

    let emptyExpr = count(col("a")).over().expr
    #expect(emptyExpr.window.windowFunction.unresolvedFunction.functionName == "count")
    #expect(emptyExpr.window.partitionSpec.isEmpty)
    #expect(emptyExpr.window.orderSpec.isEmpty)
    #expect(!emptyExpr.window.hasFrameSpec)
  }

  @Test
  func rowsBetween() throws {
    let spec = try Window.partitionBy(col("a")).orderBy(col("b"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    let frame = try #require(spec.frame)
    #expect(frame.frameType == .row)
    #expect(frame.lower.unbounded)
    #expect(frame.upper.currentRow)

    let bounded = try #require(try Window.orderBy("a").rowsBetween(-1, 2).frame)
    #expect(bounded.frameType == .row)
    #expect(bounded.lower.value.literal.integer == -1)
    #expect(bounded.upper.value.literal.integer == 2)

    #expect(throws: SparkConnectError.InvalidArgument) {
      try Window.orderBy("a").rowsBetween(Int64(Int32.min) - 1, Window.currentRow)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try Window.orderBy("a").rowsBetween(Window.currentRow, Int64(Int32.max) + 1)
    }
  }

  @Test
  func rangeBetween() throws {
    let spec = Window.orderBy("a").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    let frame = try #require(spec.frame)
    #expect(frame.frameType == .range)
    #expect(frame.lower.unbounded)
    #expect(frame.upper.unbounded)

    let bounded = try #require(Window.orderBy("a").rangeBetween(Window.currentRow, 10000000000).frame)
    #expect(bounded.frameType == .range)
    #expect(bounded.lower.currentRow)
    #expect(bounded.upper.value.literal.long == 10000000000)
  }

  @Test
  func rankOverWindow() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES ('a', 100), ('a', 200), ('a', 200), ('b', 300) AS t(dept, salary)")
    let window = Window.partitionBy("dept").orderBy(col("salary").desc())
    let rows = try await df
      .withColumn("rank", rank().over(window))
      .withColumn("dense_rank", dense_rank().over(window))
      .withColumn("row_number", row_number().over(Window.partitionBy("dept").orderBy("salary")))
      .orderBy(col("dept"), col("salary"), col("row_number"))
      .collect()
    #expect(
      rows == [
        Row("a", 100, 3, 2, 1),
        Row("a", 200, 1, 1, 2),
        Row("a", 200, 1, 1, 3),
        Row("b", 300, 1, 1, 1),
      ])
    await spark.stop()
  }

  @Test
  func sumOverRowsBetween() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 10) AS t(dept, v)")
    let window = try Window.partitionBy("dept").orderBy("v")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    let rows = try await df
      .withColumn("sum", sum(col("v")).over(window))
      .orderBy(col("dept"), col("v"))
      .collect()
    #expect(rows == [Row("a", 1, 1), Row("a", 2, 3), Row("a", 3, 6), Row("b", 10, 10)])
    await spark.stop()
  }

  @Test
  func lagLeadOverWindow() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 10) AS t(dept, v)")
    let window = Window.partitionBy("dept").orderBy("v")
    let rows = try await df
      .withColumn("lag", lag(col("v"), 1).over(window))
      .withColumn("lead", lead(col("v"), 1, -1).over(window))
      .withColumn("nth", nth_value(col("v"), 2).over(window))
      .orderBy(col("dept"), col("v"))
      .collect()
    #expect(
      rows == [
        Row("a", 1, nil, 2, nil),
        Row("a", 2, 1, 3, 2),
        Row("a", 3, 2, -1, 2),
        Row("b", 10, nil, -1, nil),
      ])
    await spark.stop()
  }
}
