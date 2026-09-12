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
  func createDataFrameWithStructType() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let schema = StructType(fields: [
      StructField(name: "id", dataType: .integer, nullable: false),
      StructField(name: "name", dataType: .string),
    ])
    let df = try await spark.createDataFrame([[1, "Alice"], [2, nil]], schema)
    #expect(try await df.schema == schema)
    #expect(try await df.collect() == [Row(1, "Alice"), Row(2, nil)])

    // Same result as the DDL string version.
    let df2 = try await spark.createDataFrame(
      [[1, "Alice"], [2, nil]], "id INT NOT NULL, name STRING")
    #expect(try await df2.schema == schema)
    #expect(try await df2.collect() == [Row(1, "Alice"), Row(2, nil)])
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
  func timeType() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      try await spark.conf.set("spark.sql.timeType.enabled", "true")
      let time = try #require(LocalTime(hour: 12, minute: 34, second: 56, nanosecond: 123_456_000))
      let df = try await spark.createDataFrame([[time], [nil]], "t TIME")
      #expect(try await df.dtypes.map { $0.1 } == ["time(6)"])
      #expect(try await df.collect() == [Row(time), Row(nil)])
      for precision in 0...6 {
        #expect(
          try await spark.createDataFrame([[time]], "t TIME(\(precision))").dtypes.map { $0.1 }
            == ["time(\(precision))"])
      }
    }
    await spark.stop()
  }

  @Test
  func timestampNanosTypes() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      try await spark.conf.set("spark.sql.timestampNanosTypes.enabled", "true")
      let timestamp = try #require(
        TimestampNanos(epochMicros: 1_706_000_000_123_456, nanosWithinMicro: 789))
      let beforeEpoch = TimestampNanos(epochNanos: -1)  // 1969-12-31 23:59:59.999999999
      for type in ["TIMESTAMP_NTZ", "TIMESTAMP_LTZ"] {
        let df = try await spark.createDataFrame(
          [[timestamp], [beforeEpoch], [nil]], "t \(type)(9)")
        #expect(try await df.dtypes.map { $0.1 } == ["\(type.lowercased())(9)"])
        #expect(try await df.collect() == [Row(timestamp), Row(beforeEpoch), Row(nil)])
        for (precision, nanos) in [(7, Int16(700)), (8, 780), (9, 789)] {
          let value = TimestampNanos(epochMicros: 1_706_000_000_123_456, nanosWithinMicro: nanos)
          let df = try await spark.createDataFrame([[value]], "t \(type)(\(precision))")
          #expect(try await df.dtypes.map { $0.1 } == ["\(type.lowercased())(\(precision))"])
          #expect(try await df.collect() == [Row(value)])
        }
        // Out of the Arrow nanosecond range (about 1677 ~ 2262).
        await #expect(throws: SparkConnectError.InvalidType) {
          try await spark.createDataFrame(
            [[TimestampNanos(epochMicros: Int64.max)]], "t \(type)(9)")
        }
      }
    }
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

  @Test
  func longStrings() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    // A first string longer than the initial 64-byte buffer and a row count where
    // `4 * rowCount` is a multiple of 64 in order to detect Arrow serialization regressions.
    let value = String(repeating: "x", count: 200)
    let data: [[Sendable?]] = (0..<16).map { (i: Int) in [i, value + String(i)] }
    let rows = try await spark.createDataFrame(data, "id INT, value STRING").collect()
    #expect(rows.count == 16)
    #expect(try rows[0].get(1) as? String == value + "0")
    #expect(try rows[15].get(1) as? String == value + "15")
    await spark.stop()
  }

  @Test
  func largeData() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    // Larger than `spark.sql.session.localRelationCacheThreshold` (1MiB by default) in order to
    // exercise the `CachedLocalRelation`-based cache path.
    let value = String(repeating: "x", count: 200)
    let data: [[Sendable?]] = (0..<10000).map { [$0, value] }
    let df = try await spark.createDataFrame(data, "id INT, value STRING")
    #expect(try await df.count() == 10000)
    #expect(try await df.selectExpr("sum(id)").collect() == [Row(Int64(49_995_000))])
    #expect(try await df.filter("id = 9999").collect() == [Row(9999, value)])

    // The second `createDataFrame` call with the same data skips the upload.
    let df2 = try await spark.createDataFrame(data, "id INT, value STRING")
    #expect(try await df2.count() == 10000)
    await spark.stop()
  }

  struct Person: Codable, Sendable, Equatable {
    let name: String
    let age: Int
  }

  struct UserProfile: Codable, Sendable, Equatable {
    let id: Int
    let nickname: String?
    let score: Double?
  }

  @Test
  func createDataFrameWithEncodable() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let data = [
      Person(name: "Alice", age: 20),
      Person(name: "Bob", age: 25),
    ]
    let df = try await spark.createDataFrame(data)
    #expect(try await df.columns == ["name", "age"])
    #expect(try await df.count() == 2)
    let collected: [Person] = try await df.collect(as: Person.self)
    #expect(collected == data)
    await spark.stop()
  }

  @Test
  func createDataFrameWithEncodableOptionals() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let data = [
      UserProfile(id: 1, nickname: nil, score: 98.5),
      UserProfile(id: 2, nickname: "spark_user", score: nil),
    ]
    let df = try await spark.createDataFrame(data)
    #expect(try await df.count() == 2)
    let collected: [UserProfile] = try await df.collect(as: UserProfile.self)
    #expect(collected == data)
    await spark.stop()
  }

  @Test
  func createDataFrameWithEncodableEmpty() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let empty: [Person] = []
    await #expect(throws: SparkConnectError.self) {
      try await spark.createDataFrame(empty)
    }

    // With explicit schema, empty encodable array works:
    let df = try await spark.createDataFrame(empty, "name STRING, age INT")
    #expect(try await df.count() == 0)
    let collected: [Person] = try await df.collect(as: Person.self)
    #expect(collected == [])
    await spark.stop()
  }

  @Test
  func createDataFrameWithEncodablePrimitives() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let numbers = [10, 20, 30]
    let df = try await spark.createDataFrame(numbers)
    #expect(try await df.count() == 3)
    let collected: [Int] = try await df.collect(as: Int.self)
    #expect(collected == numbers)
    await spark.stop()
  }

  @Test
  func collectAsWithSparkQuery() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 'Alice' AS name, 30 AS age")
    let people: [Person] = try await df.collect(as: Person.self)
    #expect(people == [Person(name: "Alice", age: 30)])
    await spark.stop()
  }

  struct Person32: Codable, Sendable, Equatable {
    let name: String
    let age: Int32
  }

  struct Person64: Codable, Sendable, Equatable {
    let name: String
    let age: Int64
  }

  @Test
  func collectAsWithUpcast() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(
      try await spark.sql("SELECT 'Alice' AS name, 30 AS age").collect(as: Person64.self)
        == [Person64(name: "Alice", age: 30)])
    #expect(
      try await spark.sql("SELECT CAST(1.5 AS FLOAT) AS score, 1L AS id")
        .collect(as: UserProfile.self) == [UserProfile(id: 1, nickname: nil, score: 1.5)])
    #expect(try await spark.sql("SELECT 1 AS a, 2Y AS b").collect(as: [Int64].self) == [[1, 2]])
    #expect(try await spark.sql("SELECT 1S AS a").collect(as: Int32.self) == [1])
    await spark.stop()
  }

  @Test
  func collectAsWithInvalidTypeOrNull() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    var error = await #expect(throws: ArrowError.self) {
      try await spark.sql("SELECT 'Alice' AS name, 30L AS age").collect(as: Person32.self)
    }
    #expect(error.map { "\($0)" } == #"invalid("Cannot decode Int32 for age")"#)

    error = await #expect(throws: ArrowError.self) {
      try await spark.sql("SELECT CAST(NULL AS STRING) AS name, 1L AS age").collect(as: Person.self)
    }
    #expect(error.map { "\($0)" } == #"invalid("Cannot decode String for name")"#)

    error = await #expect(throws: ArrowError.self) {
      try await spark.sql("SELECT 'Alice' AS name, CAST(NULL AS INT) AS age")
        .collect(as: Person32.self)
    }
    #expect(error.map { "\($0)" } == #"invalid("Cannot decode Int32 for age")"#)

    error = await #expect(throws: ArrowError.self) {
      try await spark.sql("SELECT 1 AS a, 2L AS b").collect(as: [Int32].self)
    }
    #expect(error.map { "\($0)" } == #"invalid("Cannot decode Int32 for column 1")"#)

    error = await #expect(throws: ArrowError.self) {
      try await spark.sql("SELECT 1.5D AS a").collect(as: Float.self)
    }
    #expect(error.map { "\($0)" } == #"invalid("Cannot decode Float for column 0")"#)

    error = await #expect(throws: ArrowError.self) {
      try await spark.sql("SELECT CAST(NULL AS STRING) AS a").collect(as: String.self)
    }
    #expect(error.map { "\($0)" } == #"invalid("Cannot decode String for column 0")"#)
    await spark.stop()
  }

  @Test
  func collectAsUIntWithNegativeValue() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.sql("SELECT 1 AS a, 2L AS b").collect(as: [UInt].self) == [[1, 2]])

    var error = await #expect(throws: ArrowError.self) {
      try await spark.sql("SELECT -1 AS a").collect(as: UInt.self)
    }
    #expect(error.map { "\($0)" } == #"invalid("Cannot decode UInt for column 0")"#)

    error = await #expect(throws: ArrowError.self) {
      try await spark.sql("SELECT 1L AS a, -1L AS b").collect(as: [UInt].self)
    }
    #expect(error.map { "\($0)" } == #"invalid("Cannot decode UInt for column 1")"#)
    await spark.stop()
  }
}
