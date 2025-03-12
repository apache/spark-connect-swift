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
}
