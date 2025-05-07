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

import Foundation
import SparkConnect
import Testing

/// A test suite for `DataFrameWriterV2`
@Suite(.serialized)
struct DataFrameWriterV2Tests {

  @Test
  func create() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      let write = try await spark.range(2).writeTo(tableName).using("orc")
      try await write.create()
      #expect(try await spark.table(tableName).count() == 2)
      try await #require(throws: Error.self) {
        try await write.create()
      }
    })
    await spark.stop()
  }

  @Test
  func createOrReplace() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      let write = try await spark.range(2).writeTo(tableName).using("orc")
      try await write.create()
      #expect(try await spark.table(tableName).count() == 2)
      // TODO: Use Iceberg to verify success case after Iceberg supports Apache Spark 4
      try await #require(throws: Error.self) {
        try await write.createOrReplace()
      }
    })
    await spark.stop()
  }

  @Test
  func replace() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      let write = try await spark.range(2).writeTo(tableName).using("orc")
      try await write.create()
      #expect(try await spark.table(tableName).count() == 2)
      // TODO: Use Iceberg to verify success case after Iceberg supports Apache Spark 4
      try await #require(throws: Error.self) {
        try await write.replace()
      }
    })
    await spark.stop()
  }

  @Test
  func append() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      let write = try await spark.range(2).writeTo(tableName).using("orc")
      try await write.create()
      #expect(try await spark.table(tableName).count() == 2)
      // TODO: Use Iceberg to verify success case after Iceberg supports Apache Spark 4
      try await #require(throws: Error.self) {
        try await write.append()
      }
    })
    await spark.stop()
  }
}
