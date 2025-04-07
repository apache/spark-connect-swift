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
import Testing

import SparkConnect

/// A test suite for `DataFrameWriter`
struct DataFrameWriterTests {

  @Test
  func csv() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.range(2025).write.csv(tmpDir)
    #expect(try await spark.read.csv(tmpDir).count() == 2025)
    await spark.stop()
  }

  @Test
  func json() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.range(2025).write.json(tmpDir)
    #expect(try await spark.read.json(tmpDir).count() == 2025)
    await spark.stop()
  }

  @Test
  func orc() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.range(2025).write.orc(tmpDir)
    #expect(try await spark.read.orc(tmpDir).count() == 2025)
    await spark.stop()
  }

  @Test
  func parquet() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.range(2025).write.parquet(tmpDir)
    #expect(try await spark.read.parquet(tmpDir).count() == 2025)
    await spark.stop()
  }

  @Test
  func pathAlreadyExist() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.range(2025).write.csv(tmpDir)
    try await #require(throws: Error.self) {
      try await spark.range(2025).write.csv(tmpDir)
    }
    await spark.stop()
  }

  @Test
  func overwrite() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.range(2025).write.csv(tmpDir)
    try await spark.range(2025).write.mode("overwrite").csv(tmpDir)
    await spark.stop()
  }

  @Test
  func save() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    for format in ["csv", "json", "orc", "parquet"] {
      try await spark.range(2025).write.format(format).mode("overwrite").save(tmpDir)
      #expect(try await spark.read.format(format).load(tmpDir).count() == 2025)
    }
    await spark.stop()
  }

  @Test
  func partitionBy() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.sql("SELECT 1 col1, 2 col2").write.partitionBy("col2").csv(tmpDir)
    #expect(try await spark.read.csv("\(tmpDir)/col2=2").count() == 1)
    await spark.stop()
  }

  @Test
  func sortByBucketBy() async throws {
    let tmpDir = "/tmp/" + UUID().uuidString
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 1 col1, 2 col2")
    try await #require(throws: Error.self) {
      try await df.write.sortBy("col2").csv(tmpDir)
    }
    try await #require(throws: Error.self) {
      try await df.write.sortBy("col2").bucketBy(numBuckets: 3, "col2").csv(tmpDir)
    }
    try await #require(throws: Error.self) {
      try await df.write.bucketBy(numBuckets: 3, "col2").csv(tmpDir)
    }
    await spark.stop()
  }
}
