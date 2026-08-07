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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import SparkConnect
import Testing

/// A test suite for `DataFrame`
@Suite(.serialized)
struct DataFrameTests {
  let DEALER_TABLE =
    """
    VALUES
      (100, 'Fremont', 'Honda Civic', 10),
      (100, 'Fremont', 'Honda Accord', 15),
      (100, 'Fremont', 'Honda CRV', 7),
      (200, 'Dublin', 'Honda Civic', 20),
      (200, 'Dublin', 'Honda Accord', 10),
      (200, 'Dublin', 'Honda CRV', 3),
      (300, 'San Jose', 'Honda Civic', 5),
      (300, 'San Jose', 'Honda Accord', 8)
    dealer (id, city, car_model, quantity)
    """

  @Test
  func sparkSession() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(1).sparkSession == spark)
    await spark.stop()
  }

  @Test
  func rdd() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.UnsupportedOperation) {
      try await spark.range(1).rdd()
    }
    await spark.stop()
  }

  @Test
  func columns() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.sql("SELECT 1 as col1").columns == ["col1"])
    #expect(try await spark.sql("SELECT 1 as col1, 2 as col2").columns == ["col1", "col2"])
    #expect(try await spark.sql("SELECT CAST(null as STRING) col1").columns == ["col1"])
    #expect(try await spark.sql("DROP TABLE IF EXISTS nonexistent").columns == [])
    await spark.stop()
  }

  @Test
  func schema() async throws {
    let spark = try await SparkSession.builder.getOrCreate()

    let schema1 = try await spark.sql("SELECT 'a' as col1").schema
    #expect(
      schema1
        == StructType(fields: [StructField(name: "col1", dataType: .string, nullable: false)]))
    #expect(schema1.fieldNames == ["col1"])
    #expect(schema1["col1"]?.dataType == .string)
    #expect(schema1.count == 1)

    let schema2 = try await spark.sql("SELECT 'a' as col1, 'b' as col2").schema
    #expect(
      schema2
        == StructType(fields: [
          StructField(name: "col1", dataType: .string, nullable: false),
          StructField(name: "col2", dataType: .string, nullable: false),
        ]))
    #expect(schema2.map { $0.name } == ["col1", "col2"])

    let emptySchema = try await spark.sql("DROP TABLE IF EXISTS nonexistent").schema
    #expect(emptySchema == StructType())
    #expect(emptySchema.isEmpty)
    await spark.stop()
  }

  @Test
  func printSchema() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.sql("SELECT struct(1, 2)").printSchema()
    try await spark.sql("SELECT struct(1, 2)").printSchema(1)
    await spark.stop()
  }

  @Test
  func dtypes() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = [
      ("null", "void"),
      ("127Y", "tinyint"),
      ("32767S", "smallint"),
      ("2147483647", "int"),
      ("9223372036854775807L", "bigint"),
      ("1.0F", "float"),
      ("1.0D", "double"),
      ("1.23", "decimal(3,2)"),
      ("binary('abc')", "binary"),
      ("true", "boolean"),
      ("'abc'", "string"),
      ("INTERVAL 1 YEAR", "interval year"),
      ("INTERVAL 1 MONTH", "interval month"),
      ("INTERVAL 1 DAY", "interval day"),
      ("INTERVAL 1 HOUR", "interval hour"),
      ("INTERVAL 1 MINUTE", "interval minute"),
      ("INTERVAL 1 SECOND", "interval second"),
      ("array(1, 2, 3)", "array<int>"),
      ("struct(1, 'a')", "struct<col1:int,col2:string>"),
      ("map('language', 'Swift')", "map<string,string>"),
    ]
    for pair in expected {
      #expect(try await spark.sql("SELECT \(pair.0)").dtypes[0].1 == pair.1)
    }
    await spark.stop()
  }

  @Test
  func dtypesGeospatial() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version >= "4.2" {
      let expected = [
        ("st_geomfromwkb(X'0101000000000000000000F03F0000000000000040')", "geometry(0)"),
        (
          "st_setsrid(st_geomfromwkb(X'0101000000000000000000F03F0000000000000040'), 4326)",
          "geometry(4326)"
        ),
        ("st_geogfromwkb(X'0101000000000000000000F03F0000000000000040')", "geography(4326)"),
      ]
      for pair in expected {
        #expect(try await spark.sql("SELECT \(pair.0)").dtypes[0].1 == pair.1)
      }
    }
    await spark.stop()
  }

  @Test
  func dtypesTime() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version >= "4.2" {
      try await spark.conf.set("spark.sql.timeType.enabled", "true")
      let expected = [
        ("TIME'12:34:56'", "time(6)"),
        ("CAST(TIME'12:34:56' AS TIME(0))", "time(0)"),
        ("CAST(TIME'12:34:56.123' AS TIME(3))", "time(3)"),
      ]
      for pair in expected {
        #expect(try await spark.sql("SELECT \(pair.0)").dtypes[0].1 == pair.1)
      }
    }
    await spark.stop()
  }

  @Test
  func collectTime() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version >= "4.2" {
      try await spark.conf.set("spark.sql.timeType.enabled", "true")
      let expected = [
        ("TIME'00:00:00'", LocalTime(hour: 0, minute: 0)),
        ("TIME'12:34:56'", LocalTime(hour: 12, minute: 34, second: 56)),
        ("CAST(TIME'12:34:56' AS TIME(0))", LocalTime(hour: 12, minute: 34, second: 56)),
        (
          "CAST(TIME'12:34:56.123' AS TIME(3))",
          LocalTime(hour: 12, minute: 34, second: 56, nanosecond: 123_000_000)
        ),
        (
          "TIME'23:59:59.999999'",
          LocalTime(hour: 23, minute: 59, second: 59, nanosecond: 999_999_000)
        ),
      ]
      for pair in expected {
        #expect(try await spark.sql("SELECT \(pair.0)").collect() == [Row(pair.1)])
      }
      #expect(try await spark.sql("SELECT CAST(NULL AS TIME)").collect() == [Row(nil)])
    }
    await spark.stop()
  }

  @Test
  func selectTimeLiteral() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version >= "4.2" {
      try await spark.conf.set("spark.sql.timeType.enabled", "true")
      let time = try #require(LocalTime(hour: 12, minute: 34, second: 56, nanosecond: 123_456_000))
      let df = try await spark.range(1).select(lit(time))
      #expect(try await df.dtypes[0].1 == "time(6)")
      #expect(try await df.collect() == [Row(time)])
    }
    await spark.stop()
  }

  @Test
  func array() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.sql("SELECT array('a')").collect() == [Row(Array(["a"]))])
    #expect(try await spark.sql("SELECT array(true)").collect() == [Row(Array([true]))])
    #expect(try await spark.sql("SELECT array(1)").collect() == [Row(Array([1]))])
    #expect(try await spark.sql("SELECT array(1.0F)").collect() == [Row(Array([Float(1.0)]))])
    #expect(try await spark.sql("SELECT array(1.0D)").collect() == [Row(Array([1.0]))])
    await spark.stop()
  }

  @Test
  func sameSemantics() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df1 = try await spark.range(1)
    let df2 = try await spark.range(1)
    let df3 = try await spark.range(2)
    #expect(try await df1.sameSemantics(other: df2))
    #expect(try await df1.semanticHash() == df2.semanticHash())
    #expect(try await df1.sameSemantics(other: df3) == false)
    #expect(try await df1.semanticHash() != df3.semanticHash())
    await spark.stop()
  }

  @Test
  func explain() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.range(1).explain()
    try await spark.range(1).explain(true)
    try await spark.range(1).explain("formatted")
    await spark.stop()
  }

  @Test
  func inputFiles() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(1).inputFiles().isEmpty)
    await spark.stop()
  }

  @Test
  func count() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.sql("SELECT 'spark' as swift").count() == 1)
    #expect(try await spark.sql("SELECT * FROM RANGE(2025)").count() == 2025)
    await spark.stop()
  }

  @Test
  func countNull() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.sql("SELECT null").count() == 1)
    await spark.stop()
  }

  @Test
  func collectNull() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.sql("SELECT null").collect() == [Row(nil)])
    #expect(try await spark.sql("SELECT null, 1").collect() == [Row(nil, 1)])
    #expect(try await spark.sql("SELECT null FROM RANGE(2)").collect() == [Row(nil), Row(nil)])
    #expect(try await spark.sql("SELECT struct(null)").collect().count == 1)
    await spark.stop()
  }

  @Test
  func selectNone() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let emptySchema = try await spark.range(1).select().schema
    #expect(emptySchema == StructType())
    await spark.stop()
  }

  @Test
  func select() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(1).select().columns.isEmpty)
    let schema = try await spark.range(1).select("id").schema
    #expect(
      schema == StructType(fields: [StructField(name: "id", dataType: .long, nullable: false)]))
    await spark.stop()
  }

  @Test
  func toDF() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(1).toDF().columns == ["id"])
    #expect(try await spark.range(1).toDF("id").columns == ["id"])
    await spark.stop()
  }

  @Test
  func to() async throws {
    let spark = try await SparkSession.builder.getOrCreate()

    let schema1 = try await spark.range(1).to("shortID SHORT").schema
    #expect(
      schema1 == StructType(fields: [StructField(name: "shortID", dataType: .short)]))

    let schema2 = try await spark.sql("SELECT '1'").to("id INT").schema
    print(schema2)
    #expect(
      schema2 == StructType(fields: [StructField(name: "id", dataType: .integer)]))

    await spark.stop()
  }

  @Test
  func toWithStructType() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT 1 AS id, struct('a' AS x) AS s, array(1, 2) AS a, map('k', 1.0) AS m")
    let structType = StructType(fields: [
      StructField(name: "id", dataType: .short, nullable: false),
      StructField(
        name: "s", dataType: .struct(StructType(fields: [StructField(name: "x", dataType: .string)]))),
      StructField(name: "a", dataType: .array(elementType: .long, containsNull: true)),
      StructField(
        name: "m",
        dataType: .map(
          keyType: .string, valueType: .decimal(precision: 10, scale: 2), valueContainsNull: true)),
    ])
    let ddl = "id SMALLINT NOT NULL,s STRUCT<x: STRING>,a ARRAY<BIGINT>,m MAP<STRING, DECIMAL(10,2)>"
    #expect(structType.toDDL == ddl)

    let schema1 = try await df.to(ddl).schema
    let schema2 = try await df.to(structType).schema
    #expect(schema1 == schema2)

    await spark.stop()
  }

  @Test
  func selectMultipleColumns() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let schema = try await spark.sql("SELECT * FROM VALUES (1, 2)").select("col2", "col1").schema
    #expect(
      schema
        == StructType(fields: [
          StructField(name: "col2", dataType: .integer, nullable: false),
          StructField(name: "col1", dataType: .integer, nullable: false),
        ]))
    await spark.stop()
  }

  @Test
  func selectInvalidColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await #require(throws: SparkConnectError.ColumnNotFound) {
      try await spark.range(1).select("invalid").schema
    }
    try await #require(throws: SparkConnectError.ColumnNotFound) {
      try await spark.range(1).select("id + 1").schema
    }
    await spark.stop()
  }

  @Test
  func selectExpr() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let schema = try await spark.range(1).selectExpr("id + 1 as id2").schema
    #expect(
      schema == StructType(fields: [StructField(name: "id2", dataType: .long, nullable: false)]))
    await spark.stop()
  }

  @Test
  func observe() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(10).observe("my_metrics", sum(col("id")), max(col("id")))
    #expect(try await df.columns == ["id"])
    #expect(try await df.count() == 10)
    #expect(try await df.collect().count == 10)
    await spark.stop()
  }

  @Test
  func observation() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let observation = Observation("my_metrics")
    let df = try await spark.range(10)
      .observe(observation, SparkConnect.count(col("id")), sum(col("id")), max(col("id")))
    await #expect(throws: SparkConnectError.self) {
      try await observation.get
    }
    #expect(try await df.count() == 10)
    let metrics = try await observation.get
    #expect(metrics["count(id)"] as? Int64 == 10)
    #expect(metrics["sum(id)"] as? Int64 == 45)
    #expect(metrics["max(id)"] as? Int64 == 9)
    await spark.stop()
  }

  @Test
  func withColumnRenamed() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(1).withColumnRenamed("id", "id2").columns == ["id2"])
    let df = try await spark.sql("SELECT 1 a, 2 b, 3 c, 4 d")
    #expect(try await df.withColumnRenamed(["a": "x", "c": "z"]).columns == ["x", "b", "z", "d"])
    // Ignore unknown column names.
    #expect(try await df.withColumnRenamed(["unknown": "x"]).columns == ["a", "b", "c", "d"])
    await spark.stop()
  }

  @Test
  func withColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    // Add a new column.
    #expect(try await spark.range(1).withColumn("b", "id + 1").columns == ["id", "b"])
    #expect(try await spark.range(1).withColumn("b", "id + 1").collect() == [Row(0, 1)])
    // Replace an existing column in place.
    #expect(try await spark.range(1).withColumn("id", "id + 1").columns == ["id"])
    #expect(try await spark.range(1).withColumn("id", "id + 1").collect() == [Row(1)])
    // Map overload: replace multiple existing columns (positions are preserved).
    let df = try await spark.sql("SELECT 1 a, 2 b")
    #expect(try await df.withColumns(["a": "a + 10", "b": "b + 20"]).collect() == [Row(11, 22)])
    await spark.stop()
  }

  @Test
  func withColumnWithColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    // Add a new column.
    #expect(try await spark.range(1).withColumn("b", col("id") + 1).columns == ["id", "b"])
    #expect(try await spark.range(2).withColumn("doubled", col("id") * 2).collect()
      == [Row(0, 0), Row(1, 2)])
    // Replace an existing column in place.
    #expect(try await spark.range(1).withColumn("id", col("id") + 1).columns == ["id"])
    #expect(try await spark.range(1).withColumn("id", col("id") + 1).collect() == [Row(1)])
    await spark.stop()
  }

  @Test
  func drop() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 1 a, 2 b, 3 c, 4 d")
    #expect(try await df.drop("a").columns == ["b", "c", "d"])
    #expect(try await df.drop("b", "c").columns == ["a", "d"])
    // Ignore unknown column names.
    #expect(try await df.drop("x", "y").columns == ["a", "b", "c", "d"])
    await spark.stop()
  }

  @Test
  func dropWithColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 1 a, 2 b, 3 c, 4 d")
    #expect(try await df.drop(col("a")).columns == ["b", "c", "d"])
    #expect(try await df.drop(col("b"), col("c")).columns == ["a", "d"])
    // Ignore unknown columns.
    #expect(try await df.drop(col("x"), col("y")).columns == ["a", "b", "c", "d"])
    await spark.stop()
  }

  @Test
  func filter() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(2025).filter("id % 2 == 0").count() == 1013)
    await spark.stop()
  }

  @Test
  func `where`() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(2025).where("id % 2 == 1").count() == 1012)
    await spark.stop()
  }

  @Test
  func limit() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(10).limit(0).count() == 0)
    #expect(try await spark.range(10).limit(1).count() == 1)
    #expect(try await spark.range(10).limit(2).count() == 2)
    #expect(try await spark.range(10).limit(15).count() == 10)
    await spark.stop()
  }

  @Test
  func offset() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(10).offset(0).count() == 10)
    #expect(try await spark.range(10).offset(1).count() == 9)
    #expect(try await spark.range(10).offset(2).count() == 8)
    #expect(try await spark.range(10).offset(15).count() == 0)
    await spark.stop()
  }

  @Test
  func sample() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(100000).sample(0.001).count() < 1000)
    #expect(try await spark.range(100000).sample(0.999).count() > 99000)
    #expect(try await spark.range(100000).sample(true, 0.001, 0).count() < 1000)
    await spark.stop()
  }

  @Test
  func randomSplit() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1000)
    let splits = try await df.randomSplit([1.0, 2.0, 3.0], 0)
    #expect(splits.count == 3)
    var total: Int64 = 0
    for split in splits {
      total += try await split.count()
    }
    #expect(total == 1000)
    for (df1, df2) in zip(splits, try await df.randomSplit([1.0, 2.0, 3.0], 0)) {
      #expect(try await df1.count() == df2.count())
    }
    await #expect(throws: SparkConnectError.InvalidArgument) { try await df.randomSplit([]) }
    await #expect(throws: SparkConnectError.InvalidArgument) {
      try await df.randomSplit([1.0, -1.0])
    }
    await spark.stop()
  }

  @Test
  func isEmpty() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).isEmpty())
    #expect(!(try await spark.range(1).isEmpty()))
    await spark.stop()
  }

  @Test
  func isLocal() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let version = await spark.version
    if !(version.starts(with: "4.1") || version.starts(with: "4.2")) { // TODO(SPARK-52746)
      #expect(try await spark.sql("SHOW DATABASES").isLocal())
      #expect(try await spark.sql("SHOW TABLES").isLocal())
    }
    #expect(try await spark.range(1).isLocal() == false)
    await spark.stop()
  }

  @Test
  func isStreaming() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(1).isStreaming() == false)
    await spark.stop()
  }

  @Test
  func sort() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = Array((1...10).map { Row($0) })
    #expect(try await spark.range(10, 0, -1).sort("id").collect() == expected)
    await spark.stop()
  }

  @Test
  func sortWithColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = Array((1...10).map { Row($0) })
    #expect(try await spark.range(10, 0, -1).sort(col("id")).collect() == expected)
    #expect(try await spark.range(1, 11).sort(col("id").desc()).collect()
      == Array(expected.reversed()))
    // Mix a sort order expression and a plain column.
    let df = try await spark.sql("SELECT * FROM VALUES (1, 'a'), (2, 'c'), (1, 'b') T(a, b)")
    #expect(try await df.sort(col("a").desc(), col("b")).collect()
      == [Row(2, "c"), Row(1, "a"), Row(1, "b")])
    await spark.stop()
  }

  @Test
  func orderBy() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = Array((1...10).map { Row($0) })
    #expect(try await spark.range(10, 0, -1).orderBy("id").collect() == expected)
    await spark.stop()
  }

  @Test
  func orderByWithColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = Array((1...10).map { Row($0) })
    #expect(try await spark.range(10, 0, -1).orderBy(col("id")).collect() == expected)
    #expect(try await spark.range(10, 0, -1).orderBy(col("id").asc()).collect() == expected)
    #expect(try await spark.range(1, 11).orderBy(col("id").desc()).collect()
      == Array(expected.reversed()))
    await spark.stop()
  }

  @Test
  func sortWithinPartitions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = Array((1...10).map { Row($0) })
    #expect(
      try await spark.range(10, 0, -1).repartition(1).sortWithinPartitions("id").collect()
        == expected)
    await spark.stop()
  }

  @Test
  func sortWithinPartitionsWithColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = Array((1...10).map { Row($0) })
    #expect(
      try await spark.range(10, 0, -1).repartition(1).sortWithinPartitions(col("id")).collect()
        == expected)
    #expect(
      try await spark.range(1, 11).repartition(1).sortWithinPartitions(col("id").desc()).collect()
        == Array(expected.reversed()))
    await spark.stop()
  }

  @Test
  func table() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.sql("DROP TABLE IF EXISTS t").count() == 0)
    #expect(try await spark.sql("SHOW TABLES").count() == 0)
    #expect(try await spark.sql("CREATE TABLE IF NOT EXISTS t(a INT) USING ORC").count() == 0)
    #expect(try await spark.sql("SHOW TABLES").count() == 1)
    #expect(try await spark.sql("SELECT * FROM t").count() == 0)
    #expect(try await spark.sql("INSERT INTO t VALUES (1), (2), (3)").count() == 0)
    #expect(try await spark.sql("SELECT * FROM t").count() == 3)
    #expect(try await spark.sql("DROP TABLE IF EXISTS t").count() == 0)
    await spark.stop()
  }

  @Test
  func collect() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).collect().isEmpty)
    #expect(
      try await spark.sql(
        "SELECT * FROM VALUES (1, true, 'abc'), (null, null, null), (3, false, 'def')"
      ).collect() == [Row(1, true, "abc"), Row(nil, nil, nil), Row(3, false, "def")])
    await spark.stop()
  }

  @Test
  func collectByName() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.sql(
      "SELECT * FROM VALUES (1, 'abc'), (2, 'def') T(id, name)"
    ).collect()
    #expect(try rows[0].get("id") as! Int32 == 1)
    #expect(try rows[0].get("name") as! String == "abc")
    #expect(try rows[1]["id"] as! Int32 == 2)
    #expect(try rows[1]["name"] as! String == "def")
    #expect(try rows[0].fieldIndex("name") == 1)
    await spark.stop()
  }

  @Test
  func collectMultiple() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    #expect(try await df.collect().count == 1)
    #expect(try await df.collect().count == 1)
    await spark.stop()
  }

  @Test
  func first() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(2).sort("id").first() == Row(0))
    #expect(try await spark.range(2).sort("id").head() == Row(0))
    await spark.stop()
  }

  @Test
  func head() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).head(1).isEmpty)
    #expect(try await spark.range(2).sort("id").head() == Row(0))
    #expect(try await spark.range(2).sort("id").head(1) == [Row(0)])
    #expect(try await spark.range(2).sort("id").head(2) == [Row(0), Row(1)])
    #expect(try await spark.range(2).sort("id").head(3) == [Row(0), Row(1)])
    await spark.stop()
  }

  @Test
  func take() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).take(1).isEmpty)
    #expect(try await spark.range(2).sort("id").take(1) == [Row(0)])
    #expect(try await spark.range(2).sort("id").take(2) == [Row(0), Row(1)])
    #expect(try await spark.range(2).sort("id").take(3) == [Row(0), Row(1)])
    await spark.stop()
  }

  @Test
  func tail() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).tail(1).isEmpty)
    #expect(try await spark.range(2).sort("id").tail(1) == [Row(1)])
    #expect(try await spark.range(2).sort("id").tail(2) == [Row(0), Row(1)])
    #expect(try await spark.range(2).sort("id").tail(3) == [Row(0), Row(1)])
    await spark.stop()
  }

  @Test
  func show() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.sql("SHOW TABLES").show()
    try await spark.sql("SELECT * FROM VALUES (true, false)").show()
    try await spark.sql("SELECT * FROM VALUES (1, 2)").show()
    try await spark.sql("SELECT * FROM VALUES ('abc', 'def'), ('ghi', 'jkl')").show()

    // Check all signatures
    try await spark.range(1000).show()
    try await spark.range(1000).show(1)
    try await spark.range(1000).show(true)
    try await spark.range(1000).show(false)
    try await spark.range(1000).show(1, true)
    try await spark.range(1000).show(1, false)
    try await spark.range(1000).show(1, 20)
    try await spark.range(1000).show(1, 20, true)
    try await spark.range(1000).show(1, 20, false)

    await spark.stop()
  }

  @Test
  func showNull() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.sql(
      "SELECT * FROM VALUES (1, true, 'abc'), (null, null, null), (3, false, 'def')"
    ).show()
    await spark.stop()
  }

  @Test
  func showCommand() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.sql("DROP TABLE IF EXISTS t").show()
    await spark.stop()
  }

  @Test
  func cache() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(10).cache().count() == 10)
    await spark.stop()
  }

  @Test
  func checkpoint() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version >= "4.0.0" {
      // By default, reliable checkpoint location is required.
      try await #require(throws: Error.self) {
        try await spark.range(10).checkpoint()
      }
      // Checkpointing with unreliable checkpoint
      let df = try await spark.range(10).checkpoint(true, false)
      #expect(try await df.count() == 10)
    }
    await spark.stop()
  }

  @Test
  func localCheckpoint() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version >= "4.0.0" {
      #expect(try await spark.range(10).localCheckpoint().count() == 10)
    }
    await spark.stop()
  }

  @Test
  func persist() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(20).persist().count() == 20)
    #expect(
      try await spark.range(21).persist(storageLevel: StorageLevel.MEMORY_ONLY).count() == 21)
    await spark.stop()
  }

  @Test
  func persistInvalidStorageLevel() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await #require(throws: Error.self) {
      var invalidLevel = StorageLevel.DISK_ONLY
      invalidLevel.replication = 0
      try await spark.range(9999).persist(storageLevel: invalidLevel).count()
    }
    await spark.stop()
  }

  @Test
  func unpersist() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(30)
    #expect(try await df.persist().count() == 30)
    #expect(try await df.unpersist().count() == 30)
    await spark.stop()
  }

  @Test
  func join() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df1 = try await spark.sql("SELECT * FROM VALUES ('a', 1), ('b', 2) AS T(a, b)")
    let df2 = try await spark.sql("SELECT * FROM VALUES ('c', 2), ('d', 3) AS S(c, b)")
    let expectedCross = [
      Row("a", 1, "c", 2),
      Row("a", 1, "d", 3),
      Row("b", 2, "c", 2),
      Row("b", 2, "d", 3),
    ]
    #expect(try await df1.join(df2).collect() == expectedCross)
    #expect(try await df1.crossJoin(df2).collect() == expectedCross)

    #expect(try await df1.join(df2, "b").collect() == [Row(2, "b", "c")])
    #expect(try await df1.join(df2, ["b"]).collect() == [Row(2, "b", "c")])

    #expect(
      try await df1.join(df2, "b", "left").collect() == [Row(1, "a", nil), Row(2, "b", "c")])
    #expect(
      try await df1.join(df2, "b", "right").collect() == [Row(2, "b", "c"), Row(3, nil, "d")])
    #expect(try await df1.join(df2, "b", "semi").collect() == [Row(2, "b")])
    #expect(try await df1.join(df2, "b", "anti").collect() == [Row(1, "a")])

    let expectedOuter = [
      Row(1, "a", nil),
      Row(2, "b", "c"),
      Row(3, nil, "d"),
    ]
    #expect(try await df1.join(df2, "b", "outer").collect() == expectedOuter)
    #expect(try await df1.join(df2, "b", "full").collect() == expectedOuter)
    #expect(try await df1.join(df2, ["b"], "full").collect() == expectedOuter)

    let expected = [Row("b", 2, "c", 2)]
    #expect(try await df1.join(df2, joinExprs: "T.b = S.b").collect() == expected)
    #expect(
      try await df1.join(df2, joinExprs: "T.b = S.b", joinType: "inner").collect() == expected)
    await spark.stop()
  }

  @Test
  func joinWithColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df1 = try await spark.sql("SELECT * FROM VALUES ('a', 1), ('b', 2) AS T(a, b)")
    let df2 = try await spark.sql("SELECT * FROM VALUES ('c', 2), ('d', 3) AS S(c, b)")
    let expected = [Row("b", 2, "c", 2)]
    #expect(try await df1.join(df2, joinExprs: col("T.b") == col("S.b")).collect() == expected)
    #expect(
      try await df1.join(df2, joinExprs: col("T.b") == col("S.b"), joinType: "inner").collect()
        == expected)
    #expect(
      try await df1.join(df2, joinExprs: col("T.b") == col("S.b"), joinType: "left").collect()
        == [Row("a", 1, nil, nil), Row("b", 2, "c", 2)])

    let df = try await spark.range(3)
    let joined = try await df.alias("l").join(df.alias("r"), joinExprs: col("l.id") == col("r.id"))
    #expect(try await joined.orderBy(col("l.id")).collect() == [Row(0, 0), Row(1, 1), Row(2, 2)])
    await spark.stop()
  }

  @Test
  func alias() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(3)
    #expect(try await df.alias("a").select("a.id").collect() == [Row(0), Row(1), Row(2)])

    let joined = try await df.alias("l").join(df.alias("r"), joinExprs: "l.id = r.id")
    #expect(try await joined.orderBy("l.id").collect() == [Row(0, 0), Row(1, 1), Row(2, 2)])
    await spark.stop()
  }

  @Test
  func lateralJoin() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version.starts(with: "4.") {
      let df1 = try await spark.sql("SELECT * FROM VALUES ('a', 1), ('b', 2) AS T(a, b)")
      let df2 = try await spark.sql("SELECT * FROM VALUES ('c', 2), ('d', 3) AS S(c, b)")
      let expectedCross = [
        Row("a", 1, "c", 2),
        Row("a", 1, "d", 3),
        Row("b", 2, "c", 2),
        Row("b", 2, "d", 3),
      ]
      #expect(try await df1.lateralJoin(df2).collect() == expectedCross)
      #expect(try await df1.lateralJoin(df2, joinType: "inner").collect() == expectedCross)

      let expected = [Row("b", 2, "c", 2)]
      #expect(try await df1.lateralJoin(df2, joinExprs: "T.b = S.b").collect() == expected)
      #expect(
        try await df1.lateralJoin(df2, joinExprs: "T.b = S.b", joinType: "inner").collect()
          == expected)
    }
    await spark.stop()
  }

  @Test
  func nearestByJoin() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let version = await spark.version
    if version >= "4.2" {
      let users = try await spark.sql(
        "SELECT * FROM VALUES (1, 10.0), (2, 20.0), (3, 30.0) AS T(user_id, score)")
      let products = try await spark.sql(
        "SELECT * FROM VALUES ('A', 11.0), ('B', 22.0), ('C', 5.0) AS S(product, pscore)")

      let distance = await users.nearestByJoin(
        products,
        rankingExpression: "abs(score - pscore)",
        numResults: 2,
        mode: "approx",
        direction: "distance"
      ).select("user_id", "product").orderBy("user_id", "product")
      #expect(
        try await distance.collect() == [
          Row(1, "A"), Row(1, "C"),
          Row(2, "A"), Row(2, "B"),
          Row(3, "A"), Row(3, "B"),
        ])

      let similarity = await users.nearestByJoin(
        products,
        rankingExpression: "-abs(score - pscore)",
        numResults: 1,
        mode: "approx",
        direction: "similarity"
      ).select("user_id", "product").orderBy("user_id")
      #expect(try await similarity.collect() == [Row(1, "A"), Row(2, "B"), Row(3, "B")])
    }
    await spark.stop()
  }

  @Test
  func except() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1, 3)
    #expect(try await df.except(spark.range(1, 5)).collect() == [])
    #expect(try await df.except(spark.range(2, 5)).collect() == [Row(1)])
    #expect(try await df.except(spark.range(3, 5)).collect() == [Row(1), Row(2)])
    #expect(try await spark.sql("SELECT * FROM VALUES 1, 1").except(df).count() == 0)
    await spark.stop()
  }

  @Test
  func exceptAll() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1, 3)
    #expect(try await df.exceptAll(spark.range(1, 5)).collect() == [])
    #expect(try await df.exceptAll(spark.range(2, 5)).collect() == [Row(1)])
    #expect(try await df.exceptAll(spark.range(3, 5)).collect() == [Row(1), Row(2)])
    #expect(try await spark.sql("SELECT * FROM VALUES 1, 1").exceptAll(df).count() == 1)
    await spark.stop()
  }

  @Test
  func intersect() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1, 3)
    #expect(try await df.intersect(spark.range(1, 5)).collect() == [Row(1), Row(2)])
    #expect(try await df.intersect(spark.range(2, 5)).collect() == [Row(2)])
    #expect(try await df.intersect(spark.range(3, 5)).collect() == [])
    let df2 = try await spark.sql("SELECT * FROM VALUES 1, 1")
    #expect(try await df2.intersect(df2).count() == 1)
    await spark.stop()
  }

  @Test
  func intersectAll() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1, 3)
    #expect(try await df.intersectAll(spark.range(1, 5)).collect() == [Row(1), Row(2)])
    #expect(try await df.intersectAll(spark.range(2, 5)).collect() == [Row(2)])
    #expect(try await df.intersectAll(spark.range(3, 5)).collect() == [])
    let df2 = try await spark.sql("SELECT * FROM VALUES 1, 1")
    #expect(try await df2.intersectAll(df2).count() == 2)
    await spark.stop()
  }

  @Test
  func union() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1, 2)
    #expect(try await df.union(spark.range(1, 3)).collect() == [Row(1), Row(1), Row(2)])
    #expect(try await df.union(spark.range(2, 3)).collect() == [Row(1), Row(2)])
    let df2 = try await spark.sql("SELECT * FROM VALUES 1, 1")
    #expect(try await df2.union(df2).count() == 4)
    await spark.stop()
  }

  @Test
  func unionAll() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1, 2)
    #expect(try await df.unionAll(spark.range(1, 3)).collect() == [Row(1), Row(1), Row(2)])
    #expect(try await df.unionAll(spark.range(2, 3)).collect() == [Row(1), Row(2)])
    let df2 = try await spark.sql("SELECT * FROM VALUES 1, 1")
    #expect(try await df2.unionAll(df2).count() == 4)
    await spark.stop()
  }

  @Test
  func unionByName() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df1 = try await spark.sql("SELECT 1 a, 2 b")
    let df2 = try await spark.sql("SELECT 4 b, 3 a")
    #expect(try await df1.unionByName(df2).collect() == [Row(1, 2), Row(3, 4)])
    #expect(try await df1.union(df2).collect() == [Row(1, 2), Row(4, 3)])
    let df3 = try await spark.sql("SELECT * FROM VALUES 1, 1")
    #expect(try await df3.unionByName(df3).count() == 4)
    await spark.stop()
  }

  @Test
  func repartition() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tmpDir = "/tmp/" + UUID().uuidString
    let df = try await spark.range(2025)
    for n in [1, 3, 5] as [Int32] {
      try await df.repartition(n).write.mode("overwrite").orc(tmpDir)
      #expect(try await spark.read.orc(tmpDir).inputFiles().count == n)
    }
    try await spark.range(1).repartition(10).write.mode("overwrite").orc(tmpDir)
    #expect(try await spark.read.orc(tmpDir).inputFiles().count < 10)
    await spark.stop()
  }

  @Test
  func repartitionByExpression() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tmpDir = "/tmp/" + UUID().uuidString
    let df = try await spark.range(2025)
    for n in [1, 3, 5] as [Int32] {
      try await df.repartition(n, "id").write.mode("overwrite").orc(tmpDir)
      #expect(try await spark.read.orc(tmpDir).inputFiles().count == n)
      try await df.repartitionByExpression(n, "id").write.mode("overwrite").orc(tmpDir)
      #expect(try await spark.read.orc(tmpDir).inputFiles().count == n)
    }
    try await spark.range(1).repartition(10, "id").write.mode("overwrite").orc(tmpDir)
    #expect(try await spark.read.orc(tmpDir).inputFiles().count < 10)
    try await spark.range(1).repartition("id").write.mode("overwrite").orc(tmpDir)
    #expect(try await spark.read.orc(tmpDir).inputFiles().count < 10)
    await spark.stop()
  }

  @Test
  func repartitionByRange() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tmpDir = "/tmp/" + UUID().uuidString
    let df = try await spark.range(2025)
    for n in [1, 3, 5] as [Int32] {
      try await df.repartitionByRange(n, "id").write.mode("overwrite").orc(tmpDir)
      #expect(try await spark.read.orc(tmpDir).inputFiles().count == n)
    }
    try await spark.range(1).repartitionByRange(10, "id").write.mode("overwrite").orc(tmpDir)
    #expect(try await spark.read.orc(tmpDir).inputFiles().count < 10)
    try await spark.range(1).repartitionByRange("id").write.mode("overwrite").orc(tmpDir)
    #expect(try await spark.read.orc(tmpDir).inputFiles().count < 10)
    await spark.stop()
  }

  @Test
  func coalesce() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tmpDir = "/tmp/" + UUID().uuidString
    let df = try await spark.range(2025)
    for n in [1, 2, 3] as [Int32] {
      try await df.coalesce(n).write.mode("overwrite").orc(tmpDir)
      #expect(try await spark.read.orc(tmpDir).inputFiles().count == n)
    }
    try await spark.range(1).coalesce(10).write.mode("overwrite").orc(tmpDir)
    #expect(try await spark.read.orc(tmpDir).inputFiles().count < 10)
    await spark.stop()
  }

  @Test
  func distinct() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (3), (1), (3) T(a)")
    #expect(try await df.distinct().count() == 3)
    await spark.stop()
  }

  @Test
  func dropDuplicates() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (3), (1), (3) T(a)")
    #expect(try await df.dropDuplicates().count() == 3)
    #expect(try await df.dropDuplicates("a").count() == 3)
    await spark.stop()
  }

  @Test
  func dropDuplicatesWithinWatermark() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (3), (1), (3) T(a)")
    #expect(try await df.dropDuplicatesWithinWatermark().count() == 3)
    #expect(try await df.dropDuplicatesWithinWatermark("a").count() == 3)
    await spark.stop()
  }

  @Test
  func withWatermark() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df =
      try await spark
      .sql(
        """
        SELECT * FROM VALUES
          (1, now()),
          (1, now() - INTERVAL 1 HOUR),
          (1, now() - INTERVAL 2 HOUR)
          T(data, eventTime)
        """
      )
      .withWatermark("eventTime", "1 minute")  // This tests only API for now
    #expect(try await df.dropDuplicatesWithinWatermark("data").count() == 1)
    await spark.stop()
  }

  @Test
  func describe() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(10)
    let expected = [Row("10"), Row("4.5"), Row("3.0276503540974917"), Row("0"), Row("9")]
    #expect(try await df.describe().select("id").collect() == expected)
    #expect(try await df.describe("id").select("id").collect() == expected)
    await spark.stop()
  }

  @Test
  func summary() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = [
      Row("10"), Row("4.5"), Row("3.0276503540974917"),
      Row("0"), Row("2"), Row("4"), Row("7"), Row("9"),
    ]
    #expect(try await spark.range(10).summary().select("id").collect() == expected)
    #expect(
      try await spark.range(10).summary("min", "max").select("id").collect() == [
        Row("0"), Row("9"),
      ])
    await spark.stop()
  }

  @Test
  func groupBy() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(3).groupBy("id").agg("count(*)", "sum(*)", "avg(*)")
      .collect()
    #expect(rows == [Row(0, 1, 0, 0.0), Row(1, 1, 1, 1.0), Row(2, 1, 2, 2.0)])
    await spark.stop()
  }

  @Test
  func groupByCount() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(DEALER_TABLE).groupBy("city").count().orderBy("city")
    #expect(try await df.columns == ["city", "count"])
    #expect(
      try await df.collect() == [Row("Dublin", 3), Row("Fremont", 3), Row("San Jose", 2)])
    await spark.stop()
  }

  @Test
  func groupBySum() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(DEALER_TABLE).groupBy("city").sum("quantity", "id")
      .orderBy("city")
    #expect(try await df.columns == ["city", "sum(quantity)", "sum(id)"])
    #expect(
      try await df.collect() == [
        Row("Dublin", 33, 600), Row("Fremont", 32, 300), Row("San Jose", 13, 600),
      ])
    await spark.stop()
  }

  @Test
  func groupByAvg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = [Row("Dublin", 200.0), Row("Fremont", 100.0), Row("San Jose", 300.0)]
    let df = try await spark.sql(DEALER_TABLE).groupBy("city").avg("id").orderBy("city")
    #expect(try await df.columns == ["city", "avg(id)"])
    #expect(try await df.collect() == expected)
    let df2 = try await spark.sql(DEALER_TABLE).groupBy("city").mean("id").orderBy("city")
    #expect(try await df2.columns == ["city", "avg(id)"])
    #expect(try await df2.collect() == expected)
    await spark.stop()
  }

  @Test
  func groupByMinMax() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(DEALER_TABLE).groupBy("city").min("quantity").orderBy("city")
    #expect(try await df.columns == ["city", "min(quantity)"])
    #expect(
      try await df.collect() == [Row("Dublin", 3), Row("Fremont", 7), Row("San Jose", 5)])
    let df2 = try await spark.sql(DEALER_TABLE).groupBy("city").max("quantity").orderBy("city")
    #expect(try await df2.columns == ["city", "max(quantity)"])
    #expect(
      try await df2.collect() == [Row("Dublin", 20), Row("Fremont", 15), Row("San Jose", 8)])
    await spark.stop()
  }

  @Test
  func rollupCount() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.sql(DEALER_TABLE).rollup("city").count().orderBy("city").collect()
    #expect(
      rows == [Row("Dublin", 3), Row("Fremont", 3), Row("San Jose", 2), Row(nil, 8)])
    await spark.stop()
  }

  @Test
  func cubeSum() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.sql(DEALER_TABLE).cube("city").sum("quantity").orderBy("city")
      .collect()
    #expect(
      rows == [Row("Dublin", 33), Row("Fremont", 32), Row("San Jose", 13), Row(nil, 78)])
    await spark.stop()
  }

  @Test
  func rollup() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.sql(DEALER_TABLE).rollup("city", "car_model")
      .agg("sum(quantity) sum").orderBy("city", "car_model").collect()
    #expect(
      rows == [
        Row("Dublin", "Honda Accord", 10),
        Row("Dublin", "Honda CRV", 3),
        Row("Dublin", "Honda Civic", 20),
        Row("Dublin", nil, 33),
        Row("Fremont", "Honda Accord", 15),
        Row("Fremont", "Honda CRV", 7),
        Row("Fremont", "Honda Civic", 10),
        Row("Fremont", nil, 32),
        Row("San Jose", "Honda Accord", 8),
        Row("San Jose", "Honda Civic", 5),
        Row("San Jose", nil, 13),
        Row(nil, nil, 78),
      ])
    await spark.stop()
  }

  @Test
  func cube() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.sql(DEALER_TABLE).cube("city", "car_model")
      .agg("sum(quantity) sum").orderBy("city", "car_model").collect()
    #expect(
      rows == [
        Row("Dublin", "Honda Accord", 10),
        Row("Dublin", "Honda CRV", 3),
        Row("Dublin", "Honda Civic", 20),
        Row("Dublin", nil, 33),
        Row("Fremont", "Honda Accord", 15),
        Row("Fremont", "Honda CRV", 7),
        Row("Fremont", "Honda Civic", 10),
        Row("Fremont", nil, 32),
        Row("San Jose", "Honda Accord", 8),
        Row("San Jose", "Honda Civic", 5),
        Row("San Jose", nil, 13),
        Row(nil, "Honda Accord", 33),
        Row(nil, "Honda CRV", 10),
        Row(nil, "Honda Civic", 35),
        Row(nil, nil, 78),
      ])
    await spark.stop()
  }

  @Test
  func pivot() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.sql(DEALER_TABLE).groupBy("city")
      .pivot("car_model", ["Honda Civic", "Honda CRV"])
      .agg("sum(quantity)").orderBy("city").collect()
    #expect(
      rows == [
        Row("Dublin", 20, 3),
        Row("Fremont", 10, 7),
        Row("San Jose", 5, nil),
      ])
    await spark.stop()
  }

  @Test
  func pivotWithoutValues() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.sql(DEALER_TABLE).groupBy("city")
      .pivot("car_model")
      .agg("sum(quantity)").orderBy("city").collect()
    #expect(
      rows == [
        Row("Dublin", 10, 3, 20),
        Row("Fremont", 15, 7, 10),
        Row("San Jose", 8, nil, 5),
      ])
    await spark.stop()
  }

  @Test
  func pivotUnsupportedOperation() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(DEALER_TABLE)
    await #expect(throws: SparkConnectError.UnsupportedOperation) {
      try await df.rollup("city").pivot("car_model")
    }
    await #expect(throws: SparkConnectError.UnsupportedOperation) {
      try await df.cube("city").pivot("car_model")
    }
    await spark.stop()
  }

  @Test
  func toJSON() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(2).toJSON()
    #expect(try await df.columns == ["to_json(struct(id))"])
    #expect(try await df.collect() == [Row("{\"id\":0}"), Row("{\"id\":1}")])

    let expected = [Row("{\"a\":1,\"b\":2,\"c\":3}")]
    #expect(try await spark.sql("SELECT 1 a, 2 b, 3 c").toJSON().collect() == expected)
    await spark.stop()
  }

  @Test
  func unpivot() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      """
      SELECT * FROM
      VALUES (1, 11, 12L),
             (2, 21, 22L)
      T(id, int, long)
      """)
    let expected = [
      Row(1, "int", 11),
      Row(1, "long", 12),
      Row(2, "int", 21),
      Row(2, "long", 22),
    ]
    #expect(
      try await df.unpivot(["id"], ["int", "long"], "variable", "value").collect() == expected)
    #expect(
      try await df.melt(["id"], ["int", "long"], "variable", "value").collect() == expected)
    await spark.stop()
  }

  @Test
  func transpose() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version.starts(with: "4.") {
      #expect(try await spark.range(1).transpose().columns == ["key", "0"])
      #expect(try await spark.range(1).transpose().count() == 0)

      let df = try await spark.sql(
        """
        SELECT * FROM
        VALUES ('A', 1, 2),
               ('B', 3, 4)
        T(id, val1, val2)
        """)
      let expected = [
        Row("val1", 1, 3),
        Row("val2", 2, 4),
      ]
      #expect(try await df.transpose().collect() == expected)
      #expect(try await df.transpose("id").collect() == expected)
    }
    await spark.stop()
  }

  @Test
  func decimal() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      """
      SELECT * FROM VALUES
      (1.0, 3.4, CAST(NULL AS DECIMAL), CAST(0 AS DECIMAL)),
      (2.0, 34.56, CAST(0 AS DECIMAL), CAST(NULL AS DECIMAL))
      """)
    #expect(
      try await df.dtypes.map { $0.1 } == [
        "decimal(2,1)", "decimal(4,2)", "decimal(10,0)", "decimal(10,0)",
      ])
    let expected = [
      Row(Decimal(1.0), Decimal(3.40), nil, Decimal(0)),
      Row(Decimal(2.0), Decimal(34.56), Decimal(0), nil),
    ]
    #expect(try await df.collect() == expected)
    await spark.stop()
  }

  @Test
  func timestamp() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    // TODO(SPARK-52747)
    let df = try await spark.sql(
      "SELECT TIMESTAMP '2025-05-01 16:23:40Z', TIMESTAMP '2025-05-01 16:23:40.123456Z'")
    let expected = [
      Row(
        Date(timeIntervalSince1970: 1746116620.0), Date(timeIntervalSince1970: 1746116620.123456))
    ]
    #expect(try await df.collect() == expected)
    await spark.stop()
  }

  @Test
  func storageLevel() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)

    try await df.unpersist()
    #expect(try await df.storageLevel == StorageLevel.NONE)
    try await df.persist()
    #expect(try await df.storageLevel == StorageLevel.MEMORY_AND_DISK)

    try await df.unpersist()
    #expect(try await df.storageLevel == StorageLevel.NONE)
    try await df.persist(storageLevel: StorageLevel.MEMORY_ONLY)
    #expect(try await df.storageLevel == StorageLevel.MEMORY_ONLY)

    await spark.stop()
  }

  @Test
  func hint() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df1 = try await spark.range(1)
    let df2 = try await spark.range(1)

    try await df1.join(df2.hint("broadcast")).count()
    try await df1.join(df2.hint("coalesce", 10)).count()
    try await df1.join(df2.hint("rebalance", 10)).count()
    try await df1.join(df2.hint("rebalance", 10, "id")).count()
    try await df1.join(df2.hint("repartition", 10)).count()
    try await df1.join(df2.hint("repartition", 10, "id")).count()
    try await df1.join(df2.hint("repartition", "id")).count()
    try await df1.join(df2.hint("repartition_by_range")).count()
    try await df1.join(df2.hint("merge")).count()
    try await df1.join(df2.hint("shuffle_hash")).count()
    try await df1.join(df2.hint("shuffle_replicate_nl")).count()
    try await df1.join(df2.hint("shuffle_merge")).count()

    await spark.stop()
  }
}
