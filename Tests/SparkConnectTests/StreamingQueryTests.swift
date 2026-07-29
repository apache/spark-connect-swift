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
import Testing

@testable import SparkConnect

/// A test suite for `StreamingQuery`
@Suite(.serialized)
struct StreamingQueryTests {

  @Test
  func create() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let id = UUID()
    let runId = UUID()
    let query = StreamingQuery(id, runId, "name", spark)
    #expect(await query.id == id)
    #expect(await query.runId == runId)
    #expect(await query.name == "name")

    // Streaming query xxx is not found
    try await #require(throws: Error.self) {
      try await query.isActive
    }
    try await #require(throws: Error.self) {
      try await query.recentProgress
    }
    try await #require(throws: Error.self) {
      try await query.lastProgress
    }
    try await #require(throws: Error.self) {
      try await query.awaitTermination()
    }
    try await #require(throws: Error.self) {
      try await query.awaitTermination(1000)
    }
    await spark.stop()
  }

  @Test
  func exception() async throws {
    let spark = try await SparkSession.builder.getOrCreate()

    // Prepare directories
    let input = "/tmp/input-" + UUID().uuidString
    let checkpoint = "/tmp/checkpoint-" + UUID().uuidString
    let output = "/tmp/output-" + UUID().uuidString
    try await spark.range(2025).write.orc(input)

    // Start a streaming query
    let query =
      try await spark
      .readStream
      .schema("id LONG")
      .orc(input)
      .writeStream
      .option("checkpointLocation", checkpoint)
      .outputMode("append")
      .format("orc")
      .trigger(Trigger.ProcessingTime(1000))
      .start(output)
    #expect(try await query.isActive)

    // A query without any exception returns nil.
    #expect(try await query.exception() == nil)

    try await query.stop()
    #expect(try await query.isActive == false)
    #expect(try await query.exception() == nil)

    await spark.stop()
  }
}
