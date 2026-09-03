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

/// A test suite for `XmlFunctions`
@Suite(.serialized)
struct XmlFunctionsTests {

  @Test
  func xmlFunctions() throws {
    for (column, name) in [
      (to_xml(col("a")), "to_xml"),
      (schema_of_xml(col("a")), "schema_of_xml"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }

    let schemaOfXml = schema_of_xml("<p><a>1</a></p>").expr
    #expect(schemaOfXml.unresolvedFunction.functionName == "schema_of_xml")
    #expect(schemaOfXml.unresolvedFunction.arguments[0].literal.string == "<p><a>1</a></p>")
  }

  @Test
  func xpathFunctions() throws {
    for (column, name) in [
      (xpath(col("a"), col("b")), "xpath"),
      (xpath_boolean(col("a"), col("b")), "xpath_boolean"),
      (xpath_double(col("a"), col("b")), "xpath_double"),
      (xpath_float(col("a"), col("b")), "xpath_float"),
      (xpath_int(col("a"), col("b")), "xpath_int"),
      (xpath_long(col("a"), col("b")), "xpath_long"),
      (xpath_number(col("a"), col("b")), "xpath_number"),
      (xpath_short(col("a"), col("b")), "xpath_short"),
      (xpath_string(col("a"), col("b")), "xpath_string"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "b")
    }
  }

  @Test
  func fromXmlSchema() throws {
    let ddl = from_xml(col("a"), "b INT").expr
    #expect(ddl.unresolvedFunction.functionName == "from_xml")
    #expect(ddl.unresolvedFunction.arguments.count == 2)
    #expect(ddl.unresolvedFunction.arguments[1].literal.string == "b INT")

    let schema = StructType(fields: [StructField(name: "b", dataType: .integer)])
    #expect(from_xml(col("a"), schema).expr == ddl)

    let column = from_xml(col("a"), schema_of_xml("<p><b>1</b></p>")).expr
    #expect(column.unresolvedFunction.functionName == "from_xml")
    let inferred = column.unresolvedFunction.arguments[1].unresolvedFunction
    #expect(inferred.functionName == "schema_of_xml")
  }

  /// Options are appended as a single `map` argument like Spark SQL's `Column.fnWithOptions`.
  @Test
  func options() throws {
    #expect(to_xml(col("a"), [:]).expr == to_xml(col("a")).expr)

    let expr = from_xml(col("a"), "b INT", ["rowTag": "item", "mode": "FAILFAST"]).expr
    #expect(expr.unresolvedFunction.functionName == "from_xml")
    #expect(expr.unresolvedFunction.arguments.count == 3)
    let options = expr.unresolvedFunction.arguments[2].unresolvedFunction
    #expect(options.functionName == "map")
    #expect(options.arguments.map { $0.literal.string } == ["mode", "FAILFAST", "rowTag", "item"])
  }

  @Test
  func roundTrip() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0.0") {
      let df = try await spark.sql("SELECT 1 AS id, 'a' AS name")
      let schema = "id INT, name STRING"
      let parsed = from_xml(to_xml(`struct`(col("id"), col("name"))), schema).alias("parsed")
      let rows = try await df.select(parsed).selectExpr("parsed.id", "parsed.name").collect()
      #expect(rows == [Row(Int32(1), "a")])
    }
    await spark.stop()
  }

  @Test
  func schemaOfXml() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0.0") {
      let rows = try await spark.range(1).select(
        schema_of_xml("<p><a>1</a><b>x</b></p>"),
        schema_of_xml(lit("<p><a>1</a></p>"), ["rowTag": "p"])
      ).collect()
      #expect(rows == [Row("STRUCT<a: BIGINT, b: STRING>", "STRUCT<a: BIGINT>")])
    }
    await spark.stop()
  }

  @Test
  func xmlWithOptions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0.0") {
      let df = try await spark.sql("SELECT 1 AS id")
      let rows = try await df.select(
        to_xml(`struct`(col("id")), ["rowTag": "item"]),
        from_xml(lit("<item><id>1</id></item>"), "id INT", ["rowTag": "item"]).alias("parsed")
      ).selectExpr("*", "parsed.id").collect()
      #expect(try rows[0].get(0) as! String == "<item>\n    <id>1</id>\n</item>")
      #expect(try rows[0].get(2) as! Int32 == 1)
    }
    await spark.stop()
  }

  @Test
  func xpathArray() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1)
      .select(xpath(lit("<a><b>b1</b><b>b2</b></a>"), lit("a/b/text()"))).collect()
    #expect(try rows[0].get(0) as! [String] == ["b1", "b2"])
    await spark.stop()
  }

  @Test
  func xpathScalars() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let xml = lit("<a><b>2</b></a>")
    let path = lit("a/b")
    let rows = try await spark.range(1).select(
      xpath_string(xml, path),
      xpath_boolean(xml, path),
      xpath_short(xml, path),
      xpath_int(xml, path),
      xpath_long(xml, path),
      xpath_float(xml, path),
      xpath_double(xml, path),
      xpath_number(xml, path)
    ).collect()
    #expect(try rows[0].get(0) as! String == "2")
    #expect(try rows[0].get(1) as! Bool == true)
    #expect(try rows[0].get(2) as! Int16 == 2)
    #expect(try rows[0].get(3) as! Int32 == 2)
    #expect(try rows[0].get(4) as! Int64 == 2)
    #expect(try rows[0].get(5) as! Float == 2.0)
    #expect(try rows[0].get(6) as! Double == 2.0)
    #expect(try rows[0].get(7) as! Double == 2.0)
    await spark.stop()
  }
}
