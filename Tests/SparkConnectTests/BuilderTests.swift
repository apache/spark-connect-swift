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

/// A test suite for `SparkSession.Builder`
@Suite(.serialized)
struct BuilderTests {
  let TEST_REMOTE = ProcessInfo.processInfo.environment["SPARK_REMOTE"] ?? "sc://localhost:15002"

  @Test
  func builderDefault() async throws {
    let url = URL(string: self.TEST_REMOTE)!
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(await spark.client.clientType == "swift")
    #expect(await spark.client.url.host() == url.host())
    #expect(await spark.client.url.port == url.port)
    await spark.stop()
  }

  @Test
  func remote() async throws {
    // Don't try to connect
    let builder = await SparkSession.builder.remote("sc://spark:1234")
    #expect(await builder.sparkConf["spark.remote"] == "sc://spark:1234")
    await builder.clear()
  }

  @Test
  func appName() async throws {
    let builder = await SparkSession.builder.appName("TestApp")
    #expect(await builder.sparkConf["spark.app.name"] == "TestApp")
    try await builder.getOrCreate().stop()
  }
}
