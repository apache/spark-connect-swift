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

@testable import SparkConnect

/// A test suite for `RuntimeConf`
@Suite(.serialized)
struct RuntimeConfTests {
  @Test
  func get() async throws {
    let client = SparkConnectClient(remote: "sc://localhost", user: "test")
    _ = try await client.connect(UUID().uuidString)
    let conf = RuntimeConf(client)

    #expect(try await !conf.get("spark.app.name").isEmpty)

    try await #require(throws: Error.self) {
      try await conf.get("spark.test.non-exist")
    }

    await client.stop()
  }

  @Test
  func set() async throws {
    let client = SparkConnectClient(remote: "sc://localhost", user: "test")
    _ = try await client.connect(UUID().uuidString)
    let conf = RuntimeConf(client)
    try await conf.set("spark.test.key1", "value1")
    #expect(try await conf.get("spark.test.key1") == "value1")
    await client.stop()
  }

  @Test
  func reset() async throws {
    let client = SparkConnectClient(remote: "sc://localhost", user: "test")
    _ = try await client.connect(UUID().uuidString)
    let conf = RuntimeConf(client)

    // Success with a key that doesn't exist
    try await conf.unset("spark.test.key1")

    // Make it sure that `spark.test.key1` exists before testing `reset`.
    try await conf.set("spark.test.key1", "value1")
    #expect(try await conf.get("spark.test.key1") == "value1")

    try await conf.unset("spark.test.key1")
    try await #require(throws: Error.self) {
      try await conf.get("spark.test.key1")
    }

    await client.stop()
  }

  @Test
  func getAll() async throws {
    let client = SparkConnectClient(remote: "sc://localhost", user: "test")
    _ = try await client.connect(UUID().uuidString)
    let conf = RuntimeConf(client)
    let map = try await conf.getAll()
    #expect(map.count > 0)
    #expect(map["spark.app.id"] != nil)
    #expect(map["spark.app.startTime"] != nil)
    #expect(map["spark.executor.id"] == "driver")
    #expect(map["spark.master"] != nil)
    await client.stop()
  }
}
