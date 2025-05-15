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

import SparkConnect

/// A test suite for `Apache Iceberg` integration
@Suite(.serialized)
struct IcebergTests {
  let ICEBERG_DATABASE = "local.db"
  let icebergEnabled = ProcessInfo.processInfo.environment["SPARK_ICEBERG_TEST_ENABLED"] != nil

  @Test
  func test() async throws {
    guard icebergEnabled else { return }
    let t1 = "\(ICEBERG_DATABASE).TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    let t2 = "\(ICEBERG_DATABASE).TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")

    let spark = try await SparkSession.builder.getOrCreate()

    try await SQLHelper.withTable(spark, t1, t2)({
      try await spark.sql("CREATE TABLE \(t1) (id BIGINT, data STRING) USING ICEBERG").count()
      try await spark.sql("CREATE TABLE \(t2) (id BIGINT, data STRING) USING ICEBERG").count()

      #expect(try await spark.catalog.tableExists(t1))
      #expect(try await spark.catalog.tableExists(t2))

      #expect(try await spark.table(t1).count() == 0)
      #expect(try await spark.table(t2).count() == 0)

      try await spark.sql("INSERT INTO \(t1) VALUES (1, 'a'), (2, 'b'), (3, 'c')").count()
      #expect(try await spark.table(t1).count() == 3)
      #expect(try await spark.table(t2).count() == 0)

      try await spark.table(t1).writeTo(t2).append()
      #expect(try await spark.table(t2).count() == 3)

      try await spark.table(t1).writeTo(t2).append()
      #expect(try await spark.table(t2).count() == 6)

      try await spark.table(t1).writeTo(t2).replace()
      #expect(try await spark.table(t2).count() == 3)

      try await spark.table(t1).writeTo(t2).overwrite("true")
      #expect(try await spark.table(t2).count() == 3)

      try await spark.table(t1).writeTo(t2).overwrite("false")
      #expect(try await spark.table(t2).count() == 6)
    })

    await spark.stop()
  }
}
