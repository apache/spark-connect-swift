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

/// A test suite for `DataStreamReader` and `DataStreamWriter`
@Suite(.serialized)
struct DataStreamTests {
  @Test
  func query() async throws {
    let spark = try await SparkSession.builder.getOrCreate()

    // Prepare directories
    let input = "/tmp/input-" + UUID().uuidString
    let checkpoint = "/tmp/checkpoint-" + UUID().uuidString
    let output = "/tmp/output-" + UUID().uuidString
    try await spark.range(2025).write.orc(input)

    // Create a streaming dataframe.
    let df =
      try await spark
      .readStream
      .schema("id LONG")
      .orc(input)
    #expect(try await df.isStreaming())

    // Processing
    let df2 = await df.selectExpr("id", "id * 10 as value")

    // Start a streaming query
    let query =
      try await df2
      .writeStream
      .option("checkpointLocation", checkpoint)
      .outputMode("append")
      .format("orc")
      .trigger(Trigger.ProcessingTime(1000))
      .start(output)
    #expect(try await query.isActive)
    // Wait for processing
    try await Task.sleep(nanoseconds: 2_000_000_000)

    try await query.stop()
    #expect(try await query.isActive == false)

    let df3 = await spark.read.orc(output)
    #expect(try await df3.dtypes.count == 2)
    #expect(try await df3.count() == 2025)
    await spark.stop()
  }
}
