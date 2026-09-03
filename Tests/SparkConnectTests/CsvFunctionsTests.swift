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

/// A test suite for `CsvFunctions`
@Suite(.serialized)
struct CsvFunctionsTests {

  @Test
  func csvFunctions() throws {
    for (column, name) in [
      (to_csv(col("a")), "to_csv"),
      (schema_of_csv(col("a")), "schema_of_csv"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }

    let schemaOfCsv = schema_of_csv("1,a").expr
    #expect(schemaOfCsv.unresolvedFunction.functionName == "schema_of_csv")
    #expect(schemaOfCsv.unresolvedFunction.arguments[0].literal.string == "1,a")
  }

  @Test
  func fromCsvSchema() throws {
    let ddl = from_csv(col("a"), "b INT").expr
    #expect(ddl.unresolvedFunction.functionName == "from_csv")
    #expect(ddl.unresolvedFunction.arguments.count == 2)
    #expect(ddl.unresolvedFunction.arguments[1].literal.string == "b INT")

    let column = from_csv(col("a"), schema_of_csv("1")).expr
    #expect(column.unresolvedFunction.functionName == "from_csv")
    let schema = column.unresolvedFunction.arguments[1].unresolvedFunction
    #expect(schema.functionName == "schema_of_csv")
  }

  /// Options are appended as a single `map` argument like Spark SQL's `Column.fnWithOptions`.
  @Test
  func options() throws {
    #expect(to_csv(col("a"), [:]).expr == to_csv(col("a")).expr)

    let expr = from_csv(col("a"), "b INT", ["sep": ";", "mode": "FAILFAST"]).expr
    #expect(expr.unresolvedFunction.functionName == "from_csv")
    #expect(expr.unresolvedFunction.arguments.count == 3)
    let options = expr.unresolvedFunction.arguments[2].unresolvedFunction
    #expect(options.functionName == "map")
    #expect(options.arguments.map { $0.literal.string } == ["mode", "FAILFAST", "sep", ";"])
  }

  @Test
  func roundTrip() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 1 AS id, 'a' AS name")
    let csv = to_csv(`struct`(col("id"), col("name")))
    let parsed = from_csv(csv, "id INT, name STRING").alias("parsed")
    let rows = try await df.select(parsed).selectExpr("parsed.id", "parsed.name").collect()
    #expect(rows == [Row(Int32(1), "a")])
    await spark.stop()
  }

  @Test
  func schemaOfCsv() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      schema_of_csv("1,abc"),
      schema_of_csv(lit("1;abc"), ["sep": ";"])
    ).collect()
    #expect(rows == [Row("STRUCT<_c0: INT, _c1: STRING>", "STRUCT<_c0: INT, _c1: STRING>")])
    await spark.stop()
  }

  @Test
  func csvWithOptions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 1 AS id, 'a' AS name")
    let rows = try await df.select(
      to_csv(`struct`(col("id"), col("name")), ["sep": ";"]),
      from_csv(lit("1;a"), "id INT, name STRING", ["sep": ";"])
    ).collect()
    #expect(try rows[0].get(0) as! String == "1;a")
    await spark.stop()
  }
}
