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

/// A test suite for `DataFrame`
struct DataFrameTests {
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
    #expect(try await spark.sql("SELECT 1 as col1").columns() == ["col1"])
    #expect(try await spark.sql("SELECT 1 as col1, 2 as col2").columns() == ["col1", "col2"])
    #expect(try await spark.sql("SELECT CAST(null as STRING) col1").columns() == ["col1"])
    #expect(try await spark.sql("DROP TABLE IF EXISTS nonexistent").columns() == [])
    await spark.stop()
  }

  @Test
  func schema() async throws {
    let spark = try await SparkSession.builder.getOrCreate()

    let schema1 = try await spark.sql("SELECT 'a' as col1").schema()
    #expect(
      schema1
        == #"{"struct":{"fields":[{"name":"col1","dataType":{"string":{"collation":"UTF8_BINARY"}}}]}}"#
    )

    let schema2 = try await spark.sql("SELECT 'a' as col1, 'b' as col2").schema()
    #expect(
      schema2
        == #"{"struct":{"fields":[{"name":"col1","dataType":{"string":{"collation":"UTF8_BINARY"}}},{"name":"col2","dataType":{"string":{"collation":"UTF8_BINARY"}}}]}}"#
    )

    let emptySchema = try await spark.sql("DROP TABLE IF EXISTS nonexistent").schema()
    #expect(emptySchema == #"{"struct":{}}"#)
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
    let emptySchema = try await spark.range(1).select().schema()
    #expect(emptySchema == #"{"struct":{}}"#)
    await spark.stop()
  }

  @Test
  func select() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let schema = try await spark.range(1).select("id").schema()
    #expect(
      schema
        == #"{"struct":{"fields":[{"name":"id","dataType":{"long":{}}}]}}"#
    )
    await spark.stop()
  }

  @Test
  func selectMultipleColumns() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let schema = try await spark.sql("SELECT * FROM VALUES (1, 2)").select("col2", "col1").schema()
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
      let _ = try await spark.range(1).select("invalid").schema()
    }
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
    #expect(try await spark.range(21).persist(useDisk: false).count() == 21)
    await spark.stop()
  }

  @Test
  func persistInvalidStorageLevel() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await #require(throws: Error.self) {
      let _ = try await spark.range(9999).persist(replication: 0).count()
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
}
