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

/// A test suite for `SparkSession`
struct SparkSessionTests {
  @Test
  func sparkContext() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.UnsupportedOperationException) {
      try await spark.sparkContext
    }
    await spark.stop()
  }

  @Test
  func stop() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    await spark.stop()
  }

  @Test func userContext() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let defaultUserContext = ProcessInfo.processInfo.userName.toUserContext
    #expect(await spark.client.userContext == defaultUserContext)
    await spark.stop()
  }

  @Test
  func version() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(await spark.version.starts(with: "4.0.0"))
    await spark.stop()
  }

  @Test
  func conf() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.conf.get("spark.app.name") == "Spark Connect server")
    try await spark.conf.set("spark.x", "y")
    #expect(try await spark.conf.get("spark.x") == "y")
    #expect(try await spark.conf.getAll().count > 10)
    await spark.stop()
  }

  @Test
  func range() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(10).count() == 10)
    #expect(try await spark.range(0, 100).count() == 100)
    #expect(try await spark.range(0, 100, 2).count() == 50)
    await spark.stop()
  }
}
