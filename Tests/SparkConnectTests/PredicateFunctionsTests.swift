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

/// A test suite for `PredicateFunctions`
@Suite(.serialized)
struct PredicateFunctionsTests {

  @Test
  func predicateFunctions() throws {
    for (column, name) in [
      (isnan(col("a")), "isnan"),
      (isnotnull(col("a")), "isnotnull"),
      (isnull(col("a")), "isnull"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func predicateFunctionArguments() throws {
    for (column, name) in [
      (equal_null(col("a"), col("b")), "equal_null"),
      (ilike(col("a"), col("b")), "ilike"),
      (like(col("a"), col("b")), "like"),
      (regexp(col("a"), col("b")), "regexp"),
      (regexp_like(col("a"), col("b")), "regexp_like"),
      (rlike(col("a"), col("b")), "rlike"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "b")
    }
  }

  @Test
  func predicateFunctionEscapeChar() throws {
    for (column, name) in [
      (ilike(col("a"), col("b"), lit("/")), "ilike"),
      (like(col("a"), col("b"), lit("/")), "like"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 3)
      #expect(expr.unresolvedFunction.arguments[2].literal.string == "/")
    }
  }

  @Test
  func filterWithNullPredicates() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES ('Alice', 20), ('Bob', NULL), (NULL, 30) T(name, age)")
    #expect(try await df.filter(isnull(col("age"))).count() == 1)
    #expect(try await df.filter(isnotnull(col("age"))).count() == 2)
    #expect(try await df.filter(isnull(col("name"))).count() == 1)

    let pairs = try await spark.sql(
      "SELECT * FROM VALUES ('a', 'a'), (NULL, NULL), ('a', NULL) T(x, y)")
    #expect(try await pairs.filter(equal_null(col("x"), col("y"))).count() == 2)
    #expect(try await pairs.select(equal_null(col("x"), col("y"))).collect()
      == [Row(true), Row(true), Row(false)])
    await spark.stop()
  }

  @Test
  func selectIsnan() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT CAST('NaN' AS DOUBLE) AS a, 1.0D AS b")
    #expect(try await df.select(isnan(col("a")), isnan(col("b"))).collect() == [Row(true, false)])
    await spark.stop()
  }

  @Test
  func filterWithPatternPredicates() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES ('Alice'), ('Bob'), ('Charlie') T(name)")
    #expect(try await df.filter(like(col("name"), lit("Al%"))).count() == 1)
    #expect(try await df.filter(ilike(col("name"), lit("al%"))).count() == 1)
    #expect(try await df.filter(rlike(col("name"), lit("^.o.$"))).count() == 1)
    #expect(try await df.filter(regexp(col("name"), lit("^.o.$"))).count() == 1)
    #expect(try await df.filter(regexp_like(col("name"), lit("^.o.$"))).count() == 1)

    // `_` matches any single character unless it is preceded by the escape character.
    let escaped = try await spark.sql("SELECT * FROM VALUES ('a_b'), ('axb') T(s)")
    #expect(try await escaped.filter(like(col("s"), lit("a_b"))).count() == 2)
    #expect(try await escaped.filter(like(col("s"), lit("a/_b"), lit("/"))).count() == 1)
    #expect(try await escaped.filter(ilike(col("s"), lit("A/_B"), lit("/"))).count() == 1)
    await spark.stop()
  }
}
