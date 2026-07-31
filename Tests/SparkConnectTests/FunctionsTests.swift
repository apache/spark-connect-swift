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
  func literalOperands() throws {
    for (column, name) in [
      (col("a") == "x", "="),
      (col("a") < 1, "<"),
      (col("a") <= 1, "<="),
      (col("a") > 1, ">"),
      (col("a") >= 1, ">="),
      (col("a") && true, "and"),
      (col("a") || true, "or"),
      (col("a") + 1, "+"),
      (col("a") - 1, "-"),
      (col("a") * 1, "*"),
      (col("a") / 1, "/"),
      (col("a") % 1, "%"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      if case .literal = expr.unresolvedFunction.arguments[1].exprType {
      } else {
        Issue.record("Expected a literal argument: \(name)")
      }
    }

    // The literal can also be used on the left-hand side.
    let lhs = (21 < col("age")).expr
    #expect(lhs.unresolvedFunction.functionName == "<")
    #expect(lhs.unresolvedFunction.arguments[0].literal.long == 21)
    #expect(lhs.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "age")
  }

  @Test
  func literalOperandTypes() throws {
    #expect((col("a") == true).expr.unresolvedFunction.arguments[1].literal.boolean == true)
    #expect((col("a") == Int8(1)).expr.unresolvedFunction.arguments[1].literal.byte == 1)
    #expect((col("a") == Int16(1)).expr.unresolvedFunction.arguments[1].literal.short == 1)
    #expect((col("a") == Int32(1)).expr.unresolvedFunction.arguments[1].literal.integer == 1)
    #expect((col("a") == Int64(1)).expr.unresolvedFunction.arguments[1].literal.long == 1)
    #expect((col("a") == 1).expr.unresolvedFunction.arguments[1].literal.long == 1)
    #expect((col("a") == Float(1.0)).expr.unresolvedFunction.arguments[1].literal.float == 1.0)
    #expect((col("a") == 1.0).expr.unresolvedFunction.arguments[1].literal.double == 1.0)
    #expect((col("a") == "x").expr.unresolvedFunction.arguments[1].literal.string == "x")
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
  func predicates() throws {
    let isNull = col("a").isNull().expr
    #expect(isNull.unresolvedFunction.functionName == "isnull")
    #expect(isNull.unresolvedFunction.arguments.count == 1)
    #expect(isNull.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")

    let isNotNull = col("a").isNotNull().expr
    #expect(isNotNull.unresolvedFunction.functionName == "isnotnull")
    #expect(isNotNull.unresolvedFunction.arguments.count == 1)

    let isin = col("a").isin(1, 2, "x").expr
    #expect(isin.unresolvedFunction.functionName == "in")
    #expect(isin.unresolvedFunction.arguments.count == 4)
    #expect(isin.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    #expect(isin.unresolvedFunction.arguments[1].literal.long == 1)
    #expect(isin.unresolvedFunction.arguments[3].literal.string == "x")

    let eqNullSafe = col("a").eqNullSafe(col("b")).expr
    #expect(eqNullSafe.unresolvedFunction.functionName == "<=>")
    #expect(eqNullSafe.unresolvedFunction.arguments.count == 2)
    #expect(col("a").eqNullSafe("x").expr.unresolvedFunction.arguments[1].literal.string == "x")
  }

  @Test
  func between() throws {
    // Composed as (a >= 1) and (a <= 10)
    let expr = col("a").between(1, 10).expr
    #expect(expr.unresolvedFunction.functionName == "and")
    let lower = expr.unresolvedFunction.arguments[0].unresolvedFunction
    #expect(lower.functionName == ">=")
    #expect(lower.arguments[1].literal.long == 1)
    let upper = expr.unresolvedFunction.arguments[1].unresolvedFunction
    #expect(upper.functionName == "<=")
    #expect(upper.arguments[1].literal.long == 10)

    #expect(col("a").between(col("b"), col("c")).expr.unresolvedFunction.functionName == "and")
    #expect(col("a").between(col("b"), 10).expr.unresolvedFunction.functionName == "and")
    #expect(col("a").between(1, col("c")).expr.unresolvedFunction.functionName == "and")
  }

  @Test
  func stringMethods() throws {
    for (column, name) in [
      (col("a").like("x%"), "like"),
      (col("a").rlike("^x"), "rlike"),
      (col("a").ilike("x%"), "ilike"),
      (col("a").contains(col("b")), "contains"),
      (col("a").contains("x"), "contains"),
      (col("a").startsWith(col("b")), "startswith"),
      (col("a").startsWith("x"), "startswith"),
      (col("a").endsWith(col("b")), "endswith"),
      (col("a").endsWith("x"), "endswith"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }

    let substr = col("a").substr(1, 3).expr
    #expect(substr.unresolvedFunction.functionName == "substr")
    #expect(substr.unresolvedFunction.arguments.count == 3)
    #expect(substr.unresolvedFunction.arguments[1].literal.long == 1)
    #expect(substr.unresolvedFunction.arguments[2].literal.long == 3)
  }

  @Test
  func extraction() throws {
    let item = col("a").getItem(0).expr
    #expect(item.unresolvedExtractValue.child.unresolvedAttribute.unparsedIdentifier == "a")
    #expect(item.unresolvedExtractValue.extraction.literal.long == 0)

    let field = col("a").getField("b").expr
    #expect(field.unresolvedExtractValue.child.unresolvedAttribute.unparsedIdentifier == "a")
    #expect(field.unresolvedExtractValue.extraction.literal.string == "b")
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
  func filterWithLiteralOperands() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(10)
    #expect(try await df.filter(col("id") == 3).collect() == [Row(3)])
    #expect(try await df.filter(col("id") > 2 && col("id") < 5).count() == 2)
    #expect(try await df.filter(2 > col("id")).count() == 2)
    #expect(try await df.where(col("id") % 3 == 0).count() == 4)
    #expect(try await df.select((col("id") + 1).alias("plus")).filter(col("plus") == 10).count() == 1)
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

  @Test
  func filterWithPredicates() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES ('Alice', 20), ('Bob', NULL), (NULL, 30) T(name, age)")
    #expect(try await df.filter(col("name").isNull()).count() == 1)
    #expect(try await df.filter(col("name").isNotNull()).count() == 2)
    #expect(try await df.filter(col("age").isin(20, 30)).count() == 2)
    #expect(try await df.filter(col("age").between(20, 30)).count() == 2)
    #expect(try await df.filter(col("name").isNotNull() && col("age").between(20, 30)).count() == 1)

    let pairs = try await spark.sql("SELECT * FROM VALUES ('a', 'a'), (NULL, NULL), ('a', NULL) T(x, y)")
    #expect(try await pairs.filter(col("x").eqNullSafe(col("y"))).count() == 2)
    #expect(try await pairs.filter(col("x").eqNullSafe("a")).count() == 2)
    await spark.stop()
  }

  @Test
  func filterWithStringMethods() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES ('Alice'), ('Bob'), ('Charlie') T(name)")
    #expect(try await df.filter(col("name").like("Al%")).count() == 1)
    #expect(try await df.filter(col("name").rlike("^.o.$")).count() == 1)
    #expect(try await df.filter(col("name").ilike("alice")).count() == 1)
    #expect(try await df.filter(col("name").contains("li")).count() == 2)
    #expect(try await df.filter(col("name").startsWith("B")).count() == 1)
    #expect(try await df.filter(col("name").endsWith("e")).count() == 2)
    await spark.stop()
  }

  @Test
  func selectWithSubstrAndExtraction() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT 'Alice' AS name, array(1, 2, 3) AS arr, map('k', 10) AS m, named_struct('a', 7) AS s")
    #expect(try await df.select(col("name").substr(1, 3)).collect() == [Row("Ali")])
    #expect(try await df.select(col("name").substr(lit(2), lit(3))).collect() == [Row("lic")])
    let rows = try await df.select(
      col("arr").getItem(0), col("m").getItem("k"), col("s").getField("a")).collect()
    #expect(rows == [Row(1, 10, 7)])
    await spark.stop()
  }
}
