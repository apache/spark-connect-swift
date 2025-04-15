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

import SparkConnect

/// A test suite for `DataFrame`
struct DataFrameTests {
  @Test
  func sparkSession() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(1).sparkSession == spark)
    await spark.stop()
  }

  @Test
  func rdd() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.UnsupportedOperationException) {
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
        == #"{"struct":{"fields":[{"name":"col1","dataType":{"string":{"collation":"UTF8_BINARY"}}}]}}"#
    )

    let schema2 = try await spark.sql("SELECT 'a' as col1, 'b' as col2").schema
    #expect(
      schema2
        == #"{"struct":{"fields":[{"name":"col1","dataType":{"string":{"collation":"UTF8_BINARY"}}},{"name":"col2","dataType":{"string":{"collation":"UTF8_BINARY"}}}]}}"#
    )

    let emptySchema = try await spark.sql("DROP TABLE IF EXISTS nonexistent").schema
    #expect(emptySchema == #"{"struct":{}}"#)
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
  func explain() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.range(1).explain()
    try await spark.range(1).explain(true)
    try await spark.range(1).explain("formatted")
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
  func selectNone() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let emptySchema = try await spark.range(1).select().schema
    #expect(emptySchema == #"{"struct":{}}"#)
    await spark.stop()
  }

  @Test
  func select() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let schema = try await spark.range(1).select("id").schema
    #expect(
      schema
        == #"{"struct":{"fields":[{"name":"id","dataType":{"long":{}}}]}}"#
    )
    await spark.stop()
  }

  @Test
  func selectMultipleColumns() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let schema = try await spark.sql("SELECT * FROM VALUES (1, 2)").select("col2", "col1").schema
    #expect(
      schema
        == #"{"struct":{"fields":[{"name":"col2","dataType":{"integer":{}}},{"name":"col1","dataType":{"integer":{}}}]}}"#
    )
    await spark.stop()
  }

  @Test
  func selectInvalidColumn() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await #require(throws: Error.self) {
      let _ = try await spark.range(1).select("invalid").schema
    }
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
  func drop() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 1 a, 2 b, 3 c, 4 d")
    #expect(try await df.drop("a").collect() == [["2", "3", "4"]])
    #expect(try await df.drop("b", "c").collect() == [["1", "4"]])
    // Ignore unknown column names.
    #expect(try await df.drop("x", "y").collect() == [["1", "2", "3", "4"]])
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
  func sample() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(100000).sample(0.001).count() < 1000)
    #expect(try await spark.range(100000).sample(0.999).count() > 99000)
    #expect(try await spark.range(100000).sample(true, 0.001, 0).count() < 1000)
    await spark.stop()
  }

  @Test
  func isEmpty() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).isEmpty())
    #expect(!(try await spark.range(1).isEmpty()))
    await spark.stop()
  }

#if !os(Linux)
  @Test
  func sort() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = (1...10).map{ [String($0)] }
    #expect(try await spark.range(10, 0, -1).sort("id").collect() == expected)
    await spark.stop()
  }

  @Test
  func orderBy() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = (1...10).map{ [String($0)] }
    #expect(try await spark.range(10, 0, -1).orderBy("id").collect() == expected)
    await spark.stop()
  }
#endif

  @Test
  func table() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.sql("DROP TABLE IF EXISTS t").count() == 0)
    #expect(try await spark.sql("SHOW TABLES").count() == 0)
    #expect(try await spark.sql("CREATE TABLE IF NOT EXISTS t(a INT)").count() == 0)
    #expect(try await spark.sql("SHOW TABLES").count() == 1)
    #expect(try await spark.sql("SELECT * FROM t").count() == 0)
    #expect(try await spark.sql("INSERT INTO t VALUES (1), (2), (3)").count() == 0)
    #expect(try await spark.sql("SELECT * FROM t").count() == 3)
    await spark.stop()
  }

#if !os(Linux)
  @Test
  func collect() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).collect().isEmpty)
    #expect(
      try await spark.sql(
        "SELECT * FROM VALUES (1, true, 'abc'), (null, null, null), (3, false, 'def')"
      ).collect() == [["1", "true", "abc"], [nil, nil, nil], ["3", "false", "def"]])
    await spark.stop()
  }

  @Test
  func head() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).head().isEmpty)
    #expect(try await spark.range(2).sort("id").head() == [["0"]])
    #expect(try await spark.range(2).sort("id").head(1) == [["0"]])
    #expect(try await spark.range(2).sort("id").head(2) == [["0"], ["1"]])
    #expect(try await spark.range(2).sort("id").head(3) == [["0"], ["1"]])
    await spark.stop()
  }

  @Test
  func tail() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(0).tail(1).isEmpty)
    #expect(try await spark.range(2).sort("id").tail(1) == [["1"]])
    #expect(try await spark.range(2).sort("id").tail(2) == [["0"], ["1"]])
    #expect(try await spark.range(2).sort("id").tail(3) == [["0"], ["1"]])
    await spark.stop()
  }

  @Test
  func show() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.sql("SHOW TABLES").show()
    try await spark.sql("SELECT * FROM VALUES (true, false)").show()
    try await spark.sql("SELECT * FROM VALUES (1, 2)").show()
    try await spark.sql("SELECT * FROM VALUES ('abc', 'def'), ('ghi', 'jkl')").show()
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
  func persist() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(20).persist().count() == 20)
    #expect(try await spark.range(21).persist(storageLevel: StorageLevel.MEMORY_ONLY).count() == 21)
    await spark.stop()
  }

  @Test
  func persistInvalidStorageLevel() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await #require(throws: Error.self) {
      var invalidLevel = StorageLevel.DISK_ONLY
      invalidLevel.replication = 0
      let _ = try await spark.range(9999).persist(storageLevel: invalidLevel).count()
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
#endif

  @Test
  func storageLevel() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)

    _ = try await df.unpersist()
    #expect(try await df.storageLevel == StorageLevel.NONE)
    _ = try await df.persist()
    #expect(try await df.storageLevel == StorageLevel.MEMORY_AND_DISK)

    _ = try await df.unpersist()
    #expect(try await df.storageLevel == StorageLevel.NONE)
    _ = try await df.persist(storageLevel: StorageLevel.MEMORY_ONLY)
    #expect(try await df.storageLevel == StorageLevel.MEMORY_ONLY)

    await spark.stop()
  }
}
