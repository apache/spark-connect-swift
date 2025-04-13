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

/// A test suite for `SparkConnectClient`
@Suite(.serialized)
struct SparkConnectClientTests {
  @Test
  func createAndStop() async throws {
    let client = SparkConnectClient(remote: "sc://localhost", user: "test")
    await client.stop()
  }

  @Test
  func connectWithInvalidUUID() async throws {
    let client = SparkConnectClient(remote: "sc://localhost", user: "test")
    try await #require(throws: SparkConnectError.InvalidSessionIDException) {
      let _ = try await client.connect("not-a-uuid-format")
    }
    await client.stop()
  }

  @Test
  func connect() async throws {
    let client = SparkConnectClient(remote: "sc://localhost", user: "test")
    let _ = try await client.connect(UUID().uuidString)
    await client.stop()
  }

  @Test
  func tags() async throws {
    let client = SparkConnectClient(remote: "sc://localhost", user: "test")
    let _ = try await client.connect(UUID().uuidString)
    let plan = await client.getPlanRange(0, 1, 1)

    #expect(await client.getExecutePlanRequest(plan).tags.isEmpty)
    try await client.addTag(tag: "tag1")

    #expect(await client.getExecutePlanRequest(plan).tags == ["tag1"])
    await client.clearTags()

    #expect(await client.getExecutePlanRequest(plan).tags.isEmpty)
    await client.stop()
  }
}
