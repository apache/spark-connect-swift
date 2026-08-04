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

import Testing

@testable import SparkConnect

/// A test suite for `SparkConnectError`
@Suite(.serialized)
struct SparkConnectErrorTests {
  @Test
  func tableOrViewNotFound() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let error = try await #require(throws: SparkConnectError.self) {
      try await spark.sql("SELECT * FROM nonexistent_table_for_error_tests").count()
    }
    #expect(error == .TableOrViewNotFound)
    #expect(error.errorClass == "TABLE_OR_VIEW_NOT_FOUND")
    #expect(error.sqlState == "42P01")
    #expect(error.message.contains("nonexistent_table_for_error_tests"))
    #expect(
      error.messageParameters["relationName"]?.contains("nonexistent_table_for_error_tests")
        ?? false)
    await spark.stop()
  }

  @Test
  func parseSyntaxError() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let error = try await #require(throws: SparkConnectError.self) {
      try await spark.sql("SELECT 1 +").count()
    }
    #expect(error == .ParseSyntaxError)
    #expect(error.errorClass == "PARSE_SYNTAX_ERROR")
    #expect(error.sqlState == "42601")
    await spark.stop()
  }

  @Test
  func compatibilityPatterns() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    // The original case spellings continue to work in `#expect(throws:)` and `catch`.
    try await #require(throws: SparkConnectError.TableOrViewNotFound) {
      try await spark.sql("SELECT * FROM nonexistent_table_for_error_tests").count()
    }
    do {
      _ = try await spark.sql("SELECT * FROM nonexistent_table_for_error_tests").count()
      Issue.record("Expected SparkConnectError.TableOrViewNotFound")
    } catch SparkConnectError.TableOrViewNotFound {
      // Expected.
    }
    await spark.stop()
  }

  @Test
  func detailsPatternMatching() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    do {
      _ = try await spark.sql("SELECT * FROM nonexistent_table_for_error_tests").count()
      Issue.record("Expected SparkConnectError.tableOrViewNotFound")
    } catch SparkConnectError.tableOrViewNotFound(let details) {
      #expect(details.errorClass == "TABLE_OR_VIEW_NOT_FOUND")
      #expect(details.sqlState == "42P01")
    }
    await spark.stop()
  }
}
