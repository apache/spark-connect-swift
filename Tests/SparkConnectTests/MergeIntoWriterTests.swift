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

/// A test suite for `MergeIntoWriter`
/// Since this requires Apache Spark 4 with Iceberg support (SPARK-48794), this suite only tests syntaxes.
@Suite(.serialized)
struct MergeIntoWriterTests {
  @Test
  func whenMatched() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      let mergeInto = try await spark.range(1).mergeInto(tableName, "true")
      if await isSparkVersionAtLeast(spark.version, "4.0.0") {
        try await #require(throws: SparkConnectError.TableOrViewNotFound) {
          try await mergeInto.whenMatched().delete().merge()
        }
        try await #require(throws: SparkConnectError.TableOrViewNotFound) {
          try await mergeInto.whenMatched("true").delete().merge()
        }
      } else {
        try await #require(throws: Error.self) {
          try await mergeInto.whenMatched().delete().merge()
        }
        try await #require(throws: Error.self) {
          try await mergeInto.whenMatched("true").delete().merge()
        }
      }
    })
    await spark.stop()
  }

  @Test
  func whenNotMatched() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      let mergeInto = try await spark.range(1).mergeInto(tableName, "true")
      if await isSparkVersionAtLeast(spark.version, "4.0.0") {
        try await #require(throws: SparkConnectError.TableOrViewNotFound) {
          try await mergeInto.whenNotMatched().insertAll().merge()
        }
        try await #require(throws: SparkConnectError.TableOrViewNotFound) {
          try await mergeInto.whenNotMatched("true").insertAll().merge()
        }
      } else {
        try await #require(throws: Error.self) {
          try await mergeInto.whenNotMatched().insertAll().merge()
        }
        try await #require(throws: Error.self) {
          try await mergeInto.whenNotMatched("true").insertAll().merge()
        }
      }
    })
    await spark.stop()
  }

  @Test
  func whenNotMatchedBySource() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      let mergeInto = try await spark.range(1).mergeInto(tableName, "true")
      if await isSparkVersionAtLeast(spark.version, "4.0.0") {
        try await #require(throws: SparkConnectError.TableOrViewNotFound) {
          try await mergeInto.whenNotMatchedBySource().delete().merge()
        }
        try await #require(throws: SparkConnectError.TableOrViewNotFound) {
          try await mergeInto.whenNotMatchedBySource("true").delete().merge()
        }
      } else {
        try await #require(throws: Error.self) {
          try await mergeInto.whenNotMatchedBySource().delete().merge()
        }
        try await #require(throws: Error.self) {
          try await mergeInto.whenNotMatchedBySource("true").delete().merge()
        }
      }
    })
    await spark.stop()
  }

  @Test
  func updateAssignments() async throws {
    let spark = try SparkSession("sc://localhost")
    let mergeInto = try await spark.range(1).mergeInto("t", "true")
    _ = await mergeInto.whenMatched().update(map: ["t.v": "s.v"])
    _ = await mergeInto.whenNotMatchedBySource("t.v > 0").update(map: ["t.v": "0"])
    let command = await mergeInto.mergeIntoTableCommand
    #expect(command.matchActions.count == 1)
    #expect(command.notMatchedBySourceActions.count == 1)
    for (action, value) in [
      (command.matchActions[0].mergeAction, "s.v"),
      (command.notMatchedBySourceActions[0].mergeAction, "0"),
    ] {
      #expect(action.actionType == .update)
      #expect(action.assignments.count == 1)
      #expect(action.assignments.first?.key.expressionString.expression == "t.v")
      #expect(action.assignments.first?.value.expressionString.expression == value)
    }
  }
}
