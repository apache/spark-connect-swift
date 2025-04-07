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

/// A test utility
struct SQLHelper {
  public static func withDatabase(_ spark: SparkSession, _ dbNames: String...) -> (
    () async throws -> Void
  ) async throws -> Void {
    func body(_ f: () async throws -> Void) async throws {
      try await ErrorUtils.tryWithSafeFinally(
        f,
        {
          for name in dbNames {
            _ = try await spark.sql("DROP DATABASE IF EXISTS \(name) CASCADE").count()
          }
        })
    }
    return body
  }

  public static func withTable(_ spark: SparkSession, _ tableNames: String...) -> (
    () async throws -> Void
  ) async throws -> Void {
    func body(_ f: () async throws -> Void) async throws {
      try await ErrorUtils.tryWithSafeFinally(
        f,
        {
          for name in tableNames {
            _ = try await spark.sql("DROP TABLE IF EXISTS \(name)").count()
          }
        })
    }
    return body
  }
}
