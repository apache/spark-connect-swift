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

/// A test suite for the artifact operations of `SparkConnectClient`
@Suite(.serialized)
struct SparkConnectClientArtifactTests {
  let TEST_REMOTE = ProcessInfo.processInfo.environment["SPARK_REMOTE"] ?? "sc://localhost"

  @Test
  func cacheArtifact() async throws {
    let client = try SparkConnectClient(remote: TEST_REMOTE)
    try await client.connect(UUID().uuidString)

    let data = "Apache Spark Connect Client for Swift".data(using: .utf8)!
    let hash = try await client.cacheArtifact(data)
    #expect(hash == SHA256.hexString(data: data))
    #expect(try await client.artifactExists("cache/\(hash)"))

    // The second call skips the upload and returns the same hash.
    #expect(try await client.cacheArtifact(data) == hash)
    await client.stop()
  }

  @Test
  func cacheArtifactChunked() async throws {
    let client = try SparkConnectClient(remote: TEST_REMOTE)
    try await client.connect(UUID().uuidString)

    // Larger than the 32KiB chunk size in order to exercise the chunked upload path.
    var data = Data(capacity: 100 * 1024)
    for i in 0..<(100 * 1024) {
      data.append(UInt8(i % 251))
    }
    let hash = try await client.cacheArtifact(data)
    #expect(hash == SHA256.hexString(data: data))
    #expect(try await client.artifactExists("cache/\(hash)"))
    await client.stop()
  }

  @Test
  func artifactExists() async throws {
    let client = try SparkConnectClient(remote: TEST_REMOTE)
    try await client.connect(UUID().uuidString)
    #expect(try await client.artifactExists("cache/nonexistent") == false)
    await client.stop()
  }
}
