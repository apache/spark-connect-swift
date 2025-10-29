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
@Suite(.serialized)
struct SparkSessionTests {
  @Test
  func sparkContext() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.UnsupportedOperation) {
      try await spark.sparkContext
    }
    await spark.stop()
  }

  @Test
  func stop() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    await spark.stop()
  }

  @Test
  func newSession() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    await spark.stop()
    let newSpark = try await spark.newSession()
    #expect(newSpark != spark)
    #expect(try await newSpark.range(1).count() == 1)
    await newSpark.stop()
  }

  @Test
  func sessionID() async throws {
    await SparkSession.builder.clear()
    let spark1 = try await SparkSession.builder.getOrCreate()
    let remote = ProcessInfo.processInfo.environment["SPARK_REMOTE"] ?? "sc://localhost"
    let spark2 = try await SparkSession.builder.remote("\(remote)/;session_id=\(spark1.sessionID)")
      .getOrCreate()
    await spark2.stop()
    #expect(spark1.sessionID == spark2.sessionID)
    #expect(spark1 == spark2)
  }

  @Test
  func closedSessionID() async throws {
    await SparkSession.builder.clear()
    let spark1 = try await SparkSession.builder.getOrCreate()
    if await spark1.version >= "4.0.0" {
      let sessionID = spark1.sessionID
      await spark1.stop()
      let remote = ProcessInfo.processInfo.environment["SPARK_REMOTE"] ?? "sc://localhost"
      try await #require(throws: SparkConnectError.SessionClosed) {
        try await SparkSession.builder.remote("\(remote)/;session_id=\(sessionID)").getOrCreate()
      }
    }
  }

  @Test func userContext() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #if os(macOS) || os(Linux)
      let defaultUserContext = ProcessInfo.processInfo.userName.toUserContext
    #else
      let defaultUserContext = "".toUserContext
    #endif
    #expect(await spark.client.userContext == defaultUserContext)
    await spark.stop()
  }

  @Test
  func version() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    let version = await spark.version
    #expect(version.starts(with: "4.") || version.starts(with: "3.5."))
    await spark.stop()
  }

  @Test
  func conf() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.conf.set("spark.x", "y")
    #expect(try await spark.conf.get("spark.x") == "y")
    #expect(try await spark.conf.getAll().count > 10)
    await spark.stop()
  }

  @Test
  func emptyDataFrame() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.emptyDataFrame.count() == 0)
    #expect(try await spark.emptyDataFrame.dtypes.isEmpty)
    #expect(try await spark.emptyDataFrame.isLocal())
    await spark.stop()
  }

  @Test
  func range() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.range(10).count() == 10)
    #expect(try await spark.range(0, 100).count() == 100)
    #expect(try await spark.range(0, 100, 2).count() == 50)
    await spark.stop()
  }

  @Test
  func sql() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    let expected = [Row(true, 1, "a")]
    if await spark.version.starts(with: "4.") {
      #expect(try await spark.sql("SELECT ?, ?, ?", true, 1, "a").collect() == expected)
      #expect(
        try await spark.sql("SELECT :x, :y, :z", args: ["x": true, "y": 1, "z": "a"]).collect()
          == expected)
    }
    await spark.stop()
  }

  @Test
  func addInvalidArtifact() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.InvalidArgument) {
      try await spark.addArtifact("x.txt")
    }
    await spark.stop()
  }

  @Test
  func addArtifact() async throws {
    let fm = FileManager()
    let path = "my.jar"
    let url = URL(fileURLWithPath: path)

    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(fm.createFile(atPath: path, contents: "abc".data(using: .utf8)))
    if await spark.version.starts(with: "4.") {
      try await spark.addArtifact(path)
      try await spark.addArtifact(url)
    }
    try fm.removeItem(atPath: path)
    await spark.stop()
  }

  @Test
  func addArtifacts() async throws {
    let fm = FileManager()
    let path = "my.jar"
    let url = URL(fileURLWithPath: path)

    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(fm.createFile(atPath: path, contents: "abc".data(using: .utf8)))
    if await spark.version.starts(with: "4.") {
      try await spark.addArtifacts(url, url)
    }
    try fm.removeItem(atPath: path)
    await spark.stop()
  }

  @Test
  func executeCommand() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version.starts(with: "4.") {
      await #expect(throws: SparkConnectError.DataSourceNotFound) {
        try await spark.executeCommand("runner", "command", [:]).show()
      }
    }
    await spark.stop()
  }

  @Test
  func table() async throws {
    await SparkSession.builder.clear()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    let spark = try await SparkSession.builder.getOrCreate()
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.sql("CREATE TABLE \(tableName) USING ORC AS VALUES (1), (2)").count()
      #expect(try await spark.table(tableName).count() == 2)
    })
    await spark.stop()
  }

  @Test
  func time() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.time(spark.range(1000).count) == 1000)
    #expect(try await spark.time(spark.range(1).collect) == [Row(0)])
    try await spark.time(spark.range(10).show)
    await spark.stop()
  }

  @Test
  func tag() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.addTag("tag1")
    #expect(await spark.getTags() == Set(["tag1"]))
    try await spark.addTag("tag2")
    #expect(await spark.getTags() == Set(["tag1", "tag2"]))
    try await spark.removeTag("tag1")
    #expect(await spark.getTags() == Set(["tag2"]))
    await spark.clearTags()
    #expect(await spark.getTags().isEmpty)
    await spark.stop()
  }

  @Test
  func invalidTags() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    await #expect(throws: SparkConnectError.InvalidArgument) {
      try await spark.addTag("")
    }
    await #expect(throws: SparkConnectError.InvalidArgument) {
      try await spark.addTag(",")
    }
    await spark.stop()
  }

  @Test
  func interruptAll() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.interruptAll() == [])
    await spark.stop()
  }

  @Test
  func interruptTag() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.interruptTag("etl") == [])
    await spark.stop()
  }

  @Test
  func interruptOperation() async throws {
    await SparkSession.builder.clear()
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.interruptOperation("id") == [])
    await spark.stop()
  }
}
