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

/// A test suite for `PartitionTransforms`
@Suite(.serialized)
struct PartitionTransformsTests {
  let icebergEnabled = ProcessInfo.processInfo.environment["SPARK_ICEBERG_TEST_ENABLED"] != nil

  @Test
  func partitionTransforms() throws {
    for (column, name) in [
      (days(col("a")), "days"),
      (hours(col("a")), "hours"),
      (months(col("a")), "months"),
      (years(col("a")), "years"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func bucketFunction() throws {
    for column in [bucket(4, col("a")), bucket(lit(Int32(4)), col("a"))] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == "bucket")
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].literal.integer == 4)
      #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func partitionByTransform() async throws {
    guard icebergEnabled else { return }
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      let df = try await spark.sql("SELECT TIMESTAMP'2026-09-03 01:02:03' AS ts, 1 AS value")
      let write = await df.writeTo(tableName)
        .partitionBy(years(col("ts")), bucket(4, col("value")))
      try await write.create()
      #expect(try await spark.table(tableName).count() == 1)
      let ddl = try await spark.sql("SHOW CREATE TABLE \(tableName)").collect()[0].get(0) as! String
      #expect(ddl.contains("PARTITIONED BY (years(ts), bucket(4, value))"))
    })
    await spark.stop()
  }
}
