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

/// A test suite for `JsonFunctions`
@Suite(.serialized)
struct JsonFunctionsTests {

  @Test
  func jsonFunctions() throws {
    for (column, name) in [
      (json_array_length(col("a")), "json_array_length"),
      (json_object_keys(col("a")), "json_object_keys"),
      (to_json(col("a")), "to_json"),
      (schema_of_json(col("a")), "schema_of_json"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func jsonFunctionArguments() throws {
    let getJsonObject = get_json_object(col("a"), "$.b").expr
    #expect(getJsonObject.unresolvedFunction.functionName == "get_json_object")
    #expect(getJsonObject.unresolvedFunction.arguments[1].literal.string == "$.b")

    let jsonTuple = json_tuple(col("a"), "b", "c").expr
    #expect(jsonTuple.unresolvedFunction.functionName == "json_tuple")
    #expect(jsonTuple.unresolvedFunction.arguments.count == 3)
    #expect(jsonTuple.unresolvedFunction.arguments[1].literal.string == "b")
    #expect(jsonTuple.unresolvedFunction.arguments[2].literal.string == "c")

    let schemaOfJson = schema_of_json("{\"a\": 1}").expr
    #expect(schemaOfJson.unresolvedFunction.functionName == "schema_of_json")
    #expect(schemaOfJson.unresolvedFunction.arguments[0].literal.string == "{\"a\": 1}")
  }

  @Test
  func fromJsonSchema() throws {
    let ddl = from_json(col("a"), "b INT").expr
    #expect(ddl.unresolvedFunction.functionName == "from_json")
    #expect(ddl.unresolvedFunction.arguments.count == 2)
    #expect(ddl.unresolvedFunction.arguments[1].literal.string == "b INT")

    let schema = StructType(fields: [StructField(name: "b", dataType: .integer)])
    #expect(from_json(col("a"), schema).expr == ddl)

    let column = from_json(col("a"), schema_of_json("{\"b\": 1}")).expr
    #expect(column.unresolvedFunction.functionName == "from_json")
    let inferred = column.unresolvedFunction.arguments[1].unresolvedFunction
    #expect(inferred.functionName == "schema_of_json")
  }

  /// Options are appended as a single `map` argument like Spark SQL's `Column.fnWithOptions`.
  @Test
  func options() throws {
    #expect(to_json(col("a"), [:]).expr == to_json(col("a")).expr)

    let expr = to_json(col("a"), ["pretty": "true", "ignoreNullFields": "false"]).expr
    #expect(expr.unresolvedFunction.functionName == "to_json")
    #expect(expr.unresolvedFunction.arguments.count == 2)
    let options = expr.unresolvedFunction.arguments[1].unresolvedFunction
    #expect(options.functionName == "map")
    let entries = options.arguments.map { $0.literal.string }
    #expect(entries == ["ignoreNullFields", "false", "pretty", "true"])
  }

  @Test
  func roundTrip() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 1 AS id, 'a' AS name")
    let schema = "id INT, name STRING"
    let parsed = from_json(to_json(`struct`(col("id"), col("name"))), schema).alias("parsed")
    let rows = try await df.select(parsed).selectExpr("parsed.id", "parsed.name").collect()
    #expect(rows == [Row(Int32(1), "a")])
    await spark.stop()
  }

  @Test
  func schemaOfJson() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      schema_of_json("{\"a\": 1, \"b\": \"x\"}"),
      schema_of_json(lit("[1, 2]"))
    ).collect()
    #expect(rows == [Row("STRUCT<a: BIGINT, b: STRING>", "ARRAY<BIGINT>")])
    await spark.stop()
  }

  @Test
  func jsonAccessors() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      get_json_object(lit("{\"a\": {\"b\": 1}}"), "$.a.b"),
      json_array_length(lit("[1, 2, 3]")),
      json_object_keys(lit("{\"a\": 1, \"b\": 2}"))
    ).collect()
    #expect(try rows[0].get(0) as! String == "1")
    #expect(try rows[0].get(1) as! Int32 == 3)
    #expect(try rows[0].get(2) as! [String] == ["a", "b"])
    await spark.stop()
  }

  @Test
  func jsonTuple() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1)
      .select(json_tuple(lit("{\"a\": 1, \"b\": 2}"), "a", "b")).collect()
    #expect(rows == [Row("1", "2")])
    await spark.stop()
  }

  @Test
  func toJsonWithOptions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 1 AS id, CAST(NULL AS STRING) AS name")
    let rows = try await df.select(
      to_json(`struct`(col("id"), col("name"))),
      to_json(`struct`(col("id"), col("name")), ["ignoreNullFields": "false"])
    ).collect()
    #expect(rows == [Row("{\"id\":1}", "{\"id\":1,\"name\":null}")])
    await spark.stop()
  }
}
