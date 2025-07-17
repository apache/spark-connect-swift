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
import SparkConnect
import Testing

/// A test suite for new syntaxes from SPARK-51207 (SPIP: Constraints in DSv2)
/// For now, only syntax test is here because Apache Spark 4.1 and the corresponding Apache Iceberg is not released yet.
@Suite(.serialized)
struct ConstraintTests {

  @Test
  func primary_key() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version.starts(with: "4.1") {
      let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
      try await SQLHelper.withTable(spark, tableName)({
        try await spark.sql("CREATE TABLE \(tableName)(a INT, PRIMARY KEY(a)) USING ORC").count()
        try await spark.sql("INSERT INTO \(tableName) VALUES (1), (2)").count()
      })
    }
    await spark.stop()
  }

  @Test
  func foreign_key() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version.starts(with: "4.1") {
      let tableName1 = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
      let tableName2 = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
      try await SQLHelper.withTable(spark, tableName1, tableName2)({
        try await spark.sql("CREATE TABLE \(tableName1)(id INT) USING ORC").count()
        try await spark.sql(
          "CREATE TABLE \(tableName2)(fk INT, FOREIGN KEY(fk) REFERENCES \(tableName2)(id)) USING ORC"
        ).count()
      })
    }
    await spark.stop()
  }

  @Test
  func unique() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version.starts(with: "4.1") {
      let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
      try await SQLHelper.withTable(spark, tableName)({
        try await spark.sql("CREATE TABLE \(tableName)(a INT UNIQUE) USING ORC").count()
        try await spark.sql("INSERT INTO \(tableName) VALUES (1), (2)").count()
      })
    }
    await spark.stop()
  }

  @Test
  func check() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version.starts(with: "4.1") {
      let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
      try await SQLHelper.withTable(spark, tableName)({
        try await spark.sql(
          "CREATE TABLE \(tableName)(a INT, CONSTRAINT c1 CHECK (a > 0)) USING ORC"
        ).count()
        try await spark.sql("INSERT INTO \(tableName) VALUES (-1)").count()
      })
    }
    await spark.stop()
  }
}
