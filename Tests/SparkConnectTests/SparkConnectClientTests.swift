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
  let TEST_REMOTE = ProcessInfo.processInfo.environment["SPARK_REMOTE"] ?? "sc://localhost"

  @Test
  func createAndStop() async throws {
    let client = SparkConnectClient(remote: TEST_REMOTE)
    await client.stop()
  }

  @Test
  func parameters() async throws {
    let client = SparkConnectClient(remote: "sc://host1:123/;token=abcd;userId=test;userAgent=myagent")
    #expect(await client.token == "abcd")
    #expect(await client.userContext.userID == "test")
    #expect(await client.clientType == "myagent")
    #expect(await client.host == "host1")
    #expect(await client.port == 123)
    await client.stop()
  }

  @Test
  func connectWithInvalidUUID() async throws {
    let client = SparkConnectClient(remote: TEST_REMOTE)
    try await #require(throws: SparkConnectError.InvalidSessionIDException) {
      let _ = try await client.connect("not-a-uuid-format")
    }
    await client.stop()
  }

  @Test
  func connect() async throws {
    let client = SparkConnectClient(remote: TEST_REMOTE)
    let _ = try await client.connect(UUID().uuidString)
    await client.stop()
  }

  @Test
  func tags() async throws {
    let client = SparkConnectClient(remote: TEST_REMOTE)
    let _ = try await client.connect(UUID().uuidString)
    let plan = await client.getPlanRange(0, 1, 1)

    #expect(await client.getExecutePlanRequest(plan).tags.isEmpty)
    try await client.addTag(tag: "tag1")

    #expect(await client.getExecutePlanRequest(plan).tags == ["tag1"])
    await client.clearTags()

    #expect(await client.getExecutePlanRequest(plan).tags.isEmpty)
    await client.stop()
  }

  @Test
  func ddlParse() async throws {
    let client = SparkConnectClient(remote: TEST_REMOTE)
    let _ = try await client.connect(UUID().uuidString)
    #expect(try await client.ddlParse("a int").simpleString == "struct<a:int>")
    await client.stop()
  }

#if !os(Linux) // TODO: Enable this with the offical Spark 4 docker image
  @Test
  func jsonToDdl() async throws {
    let client = SparkConnectClient(remote: TEST_REMOTE)
    let _ = try await client.connect(UUID().uuidString)
    let json =
      #"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}"#
    #expect(try await client.jsonToDdl(json) == "id BIGINT NOT NULL")
    await client.stop()
  }
#endif
}
