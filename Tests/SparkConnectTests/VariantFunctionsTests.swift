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

/// A test suite for `VariantFunctions`
@Suite(.serialized)
struct VariantFunctionsTests {

  @Test
  func variantFunctions() throws {
    for (column, name) in [
      (parse_json(col("a")), "parse_json"),
      (try_parse_json(col("a")), "try_parse_json"),
      (to_variant_object(col("a")), "to_variant_object"),
      (is_variant_null(col("a")), "is_variant_null"),
      (is_valid_variant(col("a")), "is_valid_variant"),
      (schema_of_variant(col("a")), "schema_of_variant"),
      (schema_of_variant_agg(col("a")), "schema_of_variant_agg"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  /// A `String` path is a shorthand for a literal ``Column`` path.
  @Test
  func variantGet() throws {
    for (column, name) in [
      (variant_get(col("a"), "$.b", "int"), "variant_get"),
      (try_variant_get(col("a"), "$.b", "int"), "try_variant_get"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 3)
      #expect(expr.unresolvedFunction.arguments[1].literal.string == "$.b")
      #expect(expr.unresolvedFunction.arguments[2].literal.string == "int")
    }

    #expect(
      variant_get(col("a"), lit("$.b"), "int").expr == variant_get(col("a"), "$.b", "int").expr)
    #expect(
      try_variant_get(col("a"), lit("$.b"), "int").expr
        == try_variant_get(col("a"), "$.b", "int").expr)

    let path = variant_get(col("a"), col("p"), "int").expr
    #expect(path.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "p")
  }

  @Test
  func variantUpdates() throws {
    for (column, name) in [
      (variant_insert(col("a"), "$.b", lit(1)), "variant_insert"),
      (try_variant_insert(col("a"), "$.b", lit(1)), "try_variant_insert"),
      (variant_array_append(col("a"), "$.b", lit(1)), "variant_array_append"),
      (try_variant_array_append(col("a"), "$.b", lit(1)), "try_variant_array_append"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 3)
      #expect(expr.unresolvedFunction.arguments[1].literal.string == "$.b")
      #expect(expr.unresolvedFunction.arguments[2].literal.long == 1)
    }

    #expect(
      variant_insert(col("a"), lit("$.b"), lit(1)).expr
        == variant_insert(col("a"), "$.b", lit(1)).expr)
    #expect(
      try_variant_insert(col("a"), lit("$.b"), lit(1)).expr
        == try_variant_insert(col("a"), "$.b", lit(1)).expr)
    #expect(
      variant_array_append(col("a"), lit("$.b"), lit(1)).expr
        == variant_array_append(col("a"), "$.b", lit(1)).expr)
    #expect(
      try_variant_array_append(col("a"), lit("$.b"), lit(1)).expr
        == try_variant_array_append(col("a"), "$.b", lit(1)).expr)
  }

  /// `createIfMissing` is always sent as a literal argument like PySpark's Spark Connect client.
  @Test
  func variantSet() throws {
    for (column, name) in [
      (variant_set(col("a"), "$.b", lit(1)), "variant_set"),
      (try_variant_set(col("a"), "$.b", lit(1)), "try_variant_set"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 4)
      #expect(expr.unresolvedFunction.arguments[1].literal.string == "$.b")
      #expect(expr.unresolvedFunction.arguments[2].literal.long == 1)
      #expect(expr.unresolvedFunction.arguments[3].literal.boolean == true)
    }

    #expect(variant_set(col("a"), "$.b", lit(1), false).expr.unresolvedFunction.arguments[3].literal
      .boolean == false)
    #expect(
      try_variant_set(col("a"), "$.b", lit(1), false).expr.unresolvedFunction.arguments[3].literal
        .boolean == false)
    #expect(
      variant_set(col("a"), lit("$.b"), lit(1)).expr == variant_set(col("a"), "$.b", lit(1)).expr)
    #expect(
      try_variant_set(col("a"), lit("$.b"), lit(1)).expr
        == try_variant_set(col("a"), "$.b", lit(1)).expr)
  }

  /// `includeArrays` is always sent as a literal argument like PySpark's Spark Connect client.
  @Test
  func variantStripNulls() throws {
    let expr = variant_strip_nulls(col("a")).expr
    #expect(expr.unresolvedFunction.functionName == "variant_strip_nulls")
    #expect(expr.unresolvedFunction.arguments.count == 2)
    #expect(expr.unresolvedFunction.arguments[1].literal.boolean == true)

    let excluded = variant_strip_nulls(col("a"), false).expr
    #expect(excluded.unresolvedFunction.arguments[1].literal.boolean == false)
  }

  @Test
  func parseJson() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0") {
      let v = parse_json(lit("{\"a\": 1, \"b\": \"x\"}"))
      let rows = try await spark.range(1).select(
        v.cast("string"),
        schema_of_variant(v),
        variant_get(v, "$.a", "int"),
        try_variant_get(v, "$.b", "int"),
        try_parse_json(lit("{{{")).isNull()
      ).collect()
      #expect(
        rows == [Row("{\"a\":1,\"b\":\"x\"}", "OBJECT<a: BIGINT, b: STRING>", Int32(1), nil, true)])
    }
    await spark.stop()
  }

  @Test
  func isVariantNull() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0") {
      let rows = try await spark.range(1).select(
        is_variant_null(parse_json(lit("null"))),
        is_variant_null(parse_json(lit("{\"a\": 1}")))
      ).collect()
      #expect(rows == [Row(true, false)])
    }
    await spark.stop()
  }

  @Test
  func toVariantObject() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0") {
      let rows = try await spark.range(1).select(
        to_variant_object(`struct`(lit(1).alias("a"), lit("x").alias("b"))).cast("string")
      ).collect()
      #expect(rows == [Row("{\"a\":1,\"b\":\"x\"}")])
    }
    await spark.stop()
  }

  @Test
  func schemaOfVariantAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0") {
      let df = try await spark.sql(
        "SELECT * FROM VALUES ('{\"a\": 1}'), ('{\"b\": \"x\"}') AS T(j)")
      let rows = try await df.select(schema_of_variant_agg(parse_json(col("j")))).collect()
      #expect(rows == [Row("OBJECT<a: BIGINT, b: STRING>")])
    }
    await spark.stop()
  }

  @Test
  func isValidVariant() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let rows = try await spark.range(1).select(
        is_valid_variant(parse_json(lit("{\"a\": 1}")))
      ).collect()
      #expect(rows == [Row(true)])
    }
    await spark.stop()
  }

  @Test
  func variantInsert() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let v = parse_json(lit("{\"a\": 1}"))
      let rows = try await spark.range(1).select(
        to_json(variant_insert(v, "$.b", lit(2))),
        to_json(try_variant_insert(v, "$.b", lit(2))),
        to_json(try_variant_insert(v, "$.a", lit(2)))
      ).collect()
      #expect(rows == [Row("{\"a\":1,\"b\":2}", "{\"a\":1,\"b\":2}", nil)])
    }
    await spark.stop()
  }

  @Test
  func variantSetValue() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let v = parse_json(lit("{\"a\": 1}"))
      let rows = try await spark.range(1).select(
        to_json(variant_set(v, "$.a", lit(9))),
        to_json(variant_set(v, "$.b", lit(2))),
        to_json(variant_set(v, "$.b", lit(2), false)),
        to_json(try_variant_set(v, "$.b", lit(2), false))
      ).collect()
      #expect(rows == [Row("{\"a\":9}", "{\"a\":1,\"b\":2}", "{\"a\":1}", "{\"a\":1}")])
    }
    await spark.stop()
  }

  @Test
  func variantArrayAppend() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let v = parse_json(lit("[1, 2]"))
      let rows = try await spark.range(1).select(
        to_json(variant_array_append(v, "$", lit(3))),
        to_json(try_variant_array_append(v, "$", lit(3))),
        to_json(try_variant_array_append(v, "$[1]", lit(3)))
      ).collect()
      #expect(rows == [Row("[1,2,3]", "[1,2,3]", nil)])
    }
    await spark.stop()
  }

  @Test
  func variantStripNullsValue() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let v = parse_json(lit("{\"a\": null, \"b\": [null, 1]}"))
      let rows = try await spark.range(1).select(
        to_json(variant_strip_nulls(v)),
        to_json(variant_strip_nulls(v, false))
      ).collect()
      #expect(rows == [Row("{\"b\":[1]}", "{\"b\":[null,1]}")])
    }
    await spark.stop()
  }
}
