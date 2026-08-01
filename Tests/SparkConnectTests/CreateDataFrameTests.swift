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

/// A test suite for `SparkSession.createDataFrame`
@Suite(.serialized)
struct CreateDataFrameTests {
  @Test
  func createDataFrame() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.createDataFrame(
      [[1, "Alice"], [2, "Bob"], [3, nil]], "id INT, name STRING")
    #expect(try await df.columns == ["id", "name"])
    #expect(try await df.count() == 3)
    #expect(try await df.collect() == [Row(1, "Alice"), Row(2, "Bob"), Row(3, nil)])
    try await df.show()
    await spark.stop()
  }

  @Test
  func supportedTypes() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let date = Date(timeIntervalSince1970: 86400 * 19_000)
    let timestamp = Date(timeIntervalSince1970: 1_706_000_000.5)
    let rows = try await spark.createDataFrame(
      [
        [true, Int8(1), Int16(2), 3, Int64(4), Float(1.5), 2.5, "abc", date, timestamp],
        [nil, nil, nil, nil, nil, nil, nil, nil, nil, nil],
      ],
      "a BOOLEAN, b TINYINT, c SMALLINT, d INT, e BIGINT, f FLOAT, g DOUBLE, h STRING, i DATE, j TIMESTAMP"
    ).collect()
    #expect(
      rows == [
        Row(true, 1, 2, 3, 4, Float(1.5), 2.5, "abc", date, timestamp),
        Row(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil),
      ])
    await spark.stop()
  }

  @Test
  func binaryType() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.createDataFrame([[Data([1, 2, 3])], [nil]], "a BINARY")
    #expect(try await df.count() == 2)
    await spark.stop()
  }

  @Test
  func emptyData() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.createDataFrame([], "id INT, name STRING")
    #expect(try await df.columns == ["id", "name"])
    #expect(try await df.count() == 0)
    #expect(try await df.collect() == [])
    await spark.stop()
  }

  @Test
  func integerWidening() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.createDataFrame([[Int8(1), 2, Int32(3)]], "a INT, b BIGINT, c BIGINT")
      .collect()
    #expect(rows == [Row(1, 2, 3)])
    await spark.stop()
  }

  @Test
  func invalidTypeValue() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.InvalidType) {
      try await spark.createDataFrame([["a"]], "id INT")
    }
    await #expect(throws: SparkConnectError.InvalidType) {
      try await spark.createDataFrame([[Int64(Int32.max) + 1]], "id INT")
    }
    await spark.stop()
  }

  @Test
  func unsupportedType() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.InvalidType) {
      try await spark.createDataFrame([[Decimal(1)]], "id DECIMAL(10, 2)")
    }
    await spark.stop()
  }

  @Test
  func nonStructSchema() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.InvalidType) {
      try await spark.createDataFrame([[1]], "INT")
    }
    await spark.stop()
  }

  @Test
  func invalidRowSize() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.InvalidArgument) {
      try await spark.createDataFrame([[1, 2]], "id INT")
    }
    await spark.stop()
  }
}
