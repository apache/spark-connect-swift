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

/// A test suite for various SQL statements.
@Suite(.serialized)
struct SQLTests {
  let fm = FileManager.default
  let path = Bundle.module.path(forResource: "queries", ofType: "")!
  let regenerateGoldenFiles =
    ProcessInfo.processInfo.environment["SPARK_GENERATE_GOLDEN_FILES"] == "1"

  let regexID = /#\d+L?/
  let regexPlanId = /plan_id=\d+/
  let regexLocation = /file:[a-zA-Z0-9\.\-\/\\]+/
  let regexOwner = /(runner|185)/

  private func cleanUp(_ str: String) -> String {
    return removeOwner(removeID(removeLocation(replaceUserName(str))))
  }

  private func removeID(_ str: String) -> String {
    return str.replacing(regexPlanId, with: "plan_id=").replacing(regexID, with: "#")
  }

  private func removeLocation(_ str: String) -> String {
    return str.replacing(regexLocation, with: "*")
  }

  private func removeOwner(_ str: String) -> String {
    return str.replacing(regexOwner, with: "*")
  }

  private func replaceUserName(_ str: String) -> String {
    #if os(macOS) || os(Linux)
      return str.replacing(ProcessInfo.processInfo.userName, with: "spark")
    #else
      return str
    #endif
  }

  private func normalize(_ str: String) -> String {
    return str.replacing(/[-]+/, with: "-").replacing(/[ ]+/, with: " ")
  }

  @Test
  func testRemoveID() {
    #expect(removeID("123") == "123")
    #expect(removeID("123L") == "123L")
    #expect(removeID("#123") == "#")
    #expect(removeID("#123L") == "#")
    #expect(removeID("plan_id=123") == "plan_id=")
  }

  @Test
  func removeLocation() {
    #expect(removeLocation("file:/abc") == "*")
  }

  @Test
  func removeOwner() {
    #expect(removeOwner("runner") == "*")
    #expect(removeOwner("185") == "*")
  }

  @Test
  func testNormalize() {
    #expect(normalize("+------+------------------+") == "+-+-+")
    #expect(normalize("+      +                  +") == "+ + +")
  }

  let queriesForSpark4Only: [String] = [
    "create_scala_function.sql",
    "create_table_function.sql",
    "cast.sql",
    "decimal.sql",
    "pipesyntax.sql",
    "explain.sql",
    "variant.sql",
  ]

  let queriesForSpark41Only: [String] = [
    "time.sql"
  ]

  @Test
  func runAll() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let MAX = Int32.max
    for name in try! fm.contentsOfDirectory(atPath: path).sorted() {
      guard name.hasSuffix(".sql") else { continue }
      print(name)
      if await !spark.version.starts(with: "4.") && queriesForSpark4Only.contains(name) {
        print("Skip query \(name) due to the difference between Spark 3 and 4.")
        continue
      }
      if await !spark.version.starts(with: "4.1") && queriesForSpark41Only.contains(name) {
        print("Skip query \(name) due to the difference between Spark 4.0 and 4.1")
        continue
      }

      let sql = try String(contentsOf: URL(fileURLWithPath: "\(path)/\(name)"), encoding: .utf8)
      let result =
        try await spark.sql(sql).showString(MAX, MAX, false).collect()[0].get(0) as! String
      let answer = cleanUp(result.trimmingCharacters(in: .whitespacesAndNewlines))
      if regenerateGoldenFiles {
        let path =
          "\(FileManager.default.currentDirectoryPath)/Tests/SparkConnectTests/Resources/queries/\(name).answer"
        fm.createFile(atPath: path, contents: answer.data(using: .utf8)!, attributes: nil)
      } else {
        let expected = cleanUp(
          try String(contentsOf: URL(fileURLWithPath: "\(path)/\(name).answer"), encoding: .utf8)
        )
        .trimmingCharacters(in: .whitespacesAndNewlines)
        if answer != expected {
          print("Try to compare normalized result.")
          #expect(normalize(answer) == normalize(expected))
        }
      }
    }
    await spark.stop()
  }
}
