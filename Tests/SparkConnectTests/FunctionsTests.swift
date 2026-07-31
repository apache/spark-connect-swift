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

/// A test suite for `Column` and `Functions`
@Suite(.serialized)
struct FunctionsTests {

  @Test
  func colFunction() throws {
    let expr = col("id").expr
    #expect(expr.unresolvedAttribute.unparsedIdentifier == "id")
  }

  @Test
  func columnFunction() throws {
    let expr = column("id").expr
    #expect(expr.unresolvedAttribute.unparsedIdentifier == "id")
  }

  @Test
  func star() throws {
    let expr = col("*").expr
    #expect(expr.unresolvedStar.hasUnparsedTarget == false)

    let target = col("t.*").expr
    #expect(target.unresolvedStar.unparsedTarget == "t.*")
  }

  @Test
  func litFunction() throws {
    #expect(lit(true).expr.literal.boolean == true)
    #expect(lit(Int8(1)).expr.literal.byte == 1)
    #expect(lit(Int16(1)).expr.literal.short == 1)
    #expect(lit(Int32(1)).expr.literal.integer == 1)
    #expect(lit(Int64(1)).expr.literal.long == 1)
    #expect(lit(1).expr.literal.long == 1)
    #expect(lit(Float(1.0)).expr.literal.float == 1.0)
    #expect(lit(1.0).expr.literal.double == 1.0)
    #expect(lit("a").expr.literal.string == "a")
  }

  @Test
  func alias() throws {
    let expr = col("id").alias("x").expr
    #expect(expr.alias.name == ["x"])
    #expect(expr.alias.expr.unresolvedAttribute.unparsedIdentifier == "id")
  }

  @Test
  func ascFunction() throws {
    let expr = asc("id").expr
    #expect(expr.sortOrder.child.unresolvedAttribute.unparsedIdentifier == "id")
    #expect(expr.sortOrder.direction == .ascending)
    #expect(expr.sortOrder.nullOrdering == .sortNullsFirst)
  }

  @Test
  func descFunction() throws {
    let expr = desc("id").expr
    #expect(expr.sortOrder.child.unresolvedAttribute.unparsedIdentifier == "id")
    #expect(expr.sortOrder.direction == .descending)
    #expect(expr.sortOrder.nullOrdering == .sortNullsLast)
  }

  @Test
  func cast() throws {
    let expr = col("id").cast("string").expr
    #expect(expr.cast.expr.unresolvedAttribute.unparsedIdentifier == "id")
    #expect(expr.cast.typeStr == "string")
  }

  @Test
  func aggregateFunctions() throws {
    for (aggColumn, name) in [
      (count(col("id")), "count"),
      (sum(col("id")), "sum"),
      (avg(col("id")), "avg"),
      (mean(col("id")), "avg"),
      (min(col("id")), "min"),
      (max(col("id")), "max"),
    ] {
      let expr = aggColumn.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(
        expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "id")
    }
  }

  @Test
  func comparisonOperators() throws {
    for (column, name) in [
      (col("a") == col("b"), "="),
      (col("a") < col("b"), "<"),
      (col("a") <= col("b"), "<="),
      (col("a") > col("b"), ">"),
      (col("a") >= col("b"), ">="),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "b")
    }
  }

  @Test
  func notEqualOperator() throws {
    let expr = (col("a") != col("b")).expr
    #expect(expr.unresolvedFunction.functionName == "!")
    #expect(expr.unresolvedFunction.arguments.count == 1)
    let inner = expr.unresolvedFunction.arguments[0].unresolvedFunction
    #expect(inner.functionName == "=")
    #expect(inner.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    #expect(inner.arguments[1].unresolvedAttribute.unparsedIdentifier == "b")
  }

  @Test
  func logicalOperators() throws {
    for (column, name) in [
      (col("a") && col("b"), "and"),
      (col("a") || col("b"), "or"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
    }

    let not = (!col("a")).expr
    #expect(not.unresolvedFunction.functionName == "!")
    #expect(not.unresolvedFunction.arguments.count == 1)
    #expect(not.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
  }

  @Test
  func arithmeticOperators() throws {
    for (column, name) in [
      (col("a") + col("b"), "+"),
      (col("a") - col("b"), "-"),
      (col("a") * col("b"), "*"),
      (col("a") / col("b"), "/"),
      (col("a") % col("b"), "%"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
    }

    let negate = (-col("a")).expr
    #expect(negate.unresolvedFunction.functionName == "negative")
    #expect(negate.unresolvedFunction.arguments.count == 1)
    #expect(negate.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
  }

  @Test
  func operatorPrecedence() throws {
    // Parsed as ((a > b) and (c = d)) or (not e)
    let expr = (col("a") > col("b") && col("c") == col("d") || !col("e")).expr
    #expect(expr.unresolvedFunction.functionName == "or")
    let and = expr.unresolvedFunction.arguments[0].unresolvedFunction
    #expect(and.functionName == "and")
    #expect(and.arguments[0].unresolvedFunction.functionName == ">")
    #expect(and.arguments[1].unresolvedFunction.functionName == "=")
    #expect(expr.unresolvedFunction.arguments[1].unresolvedFunction.functionName == "!")
  }

  @Test
  func selectColumns() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(3).select(col("id"), col("id").cast("string").alias("id_string"))
    #expect(try await df.columns == ["id", "id_string"])
    #expect(try await df.count() == 3)
    await spark.stop()
  }

  @Test
  func aggColumns() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(3).groupBy("id")
      .agg(count(col("*")).alias("cnt"), sum(col("id")), avg(col("id")))
      .orderBy("id").collect()
    #expect(rows == [Row(0, 1, 0, 0.0), Row(1, 1, 1, 1.0), Row(2, 1, 2, 2.0)])
    await spark.stop()
  }

  @Test
  func filterWithColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(10)
    #expect(try await df.filter(col("id") == lit(3)).collect() == [Row(3)])
    #expect(try await df.filter(col("id") != lit(3)).count() == 9)
    #expect(try await df.filter(col("id") < lit(3)).count() == 3)
    #expect(try await df.filter(col("id") <= lit(3)).count() == 4)
    #expect(try await df.filter(col("id") > lit(7)).count() == 2)
    #expect(try await df.filter(col("id") >= lit(7)).count() == 3)
    #expect(try await df.filter(col("id") > lit(2) && col("id") < lit(5)).count() == 2)
    #expect(try await df.filter(col("id") < lit(2) || col("id") > lit(7)).count() == 4)
    #expect(try await df.filter(!(col("id") < lit(8))).count() == 2)
    #expect(try await df.where(col("id") % lit(3) == lit(0)).count() == 4)
    await spark.stop()
  }

  @Test
  func selectWithOperators() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1, 4)
      .select(
        (col("id") + lit(1)).alias("plus"),
        (col("id") - lit(1)).alias("minus"),
        (col("id") * lit(2)).alias("times"),
        (-col("id")).alias("negated")
      ).orderBy("id").collect()
    #expect(rows == [Row(2, 0, 2, -1), Row(3, 1, 4, -2), Row(4, 2, 6, -3)])
    await spark.stop()
  }
}
