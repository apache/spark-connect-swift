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

/// A test suite for `Catalog`
struct CatalogTests {
#if !os(Linux)
  @Test
  func currentCatalog() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.catalog.currentCatalog() == "spark_catalog")
    await spark.stop()
  }

  @Test
  func setCurrentCatalog() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.catalog.setCurrentCatalog("spark_catalog")
    try await #require(throws: Error.self) {
      try await spark.catalog.setCurrentCatalog("not_exist_catalog")
    }
    await spark.stop()
  }

  @Test
  func listCatalogs() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.catalog.listCatalogs() == [CatalogMetadata(name: "spark_catalog")])
    #expect(try await spark.catalog.listCatalogs(pattern: "*") == [CatalogMetadata(name: "spark_catalog")])
    #expect(try await spark.catalog.listCatalogs(pattern: "non_exist").count == 0)
    await spark.stop()
  }

  @Test
  func currentDatabase() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.catalog.currentDatabase() == "default")
    await spark.stop()
  }

  @Test
  func setCurrentDatabase() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.catalog.setCurrentDatabase("default")
    try await #require(throws: Error.self) {
      try await spark.catalog.setCurrentDatabase("not_exist_database")
    }
    await spark.stop()
  }

  @Test
  func listDatabases() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let dbs = try await spark.catalog.listDatabases()
    #expect(dbs.count == 1)
    #expect(dbs[0].name == "default")
    #expect(dbs[0].catalog == "spark_catalog")
    #expect(dbs[0].description == "default database")
    #expect(dbs[0].locationUri.hasSuffix("spark-warehouse"))
    #expect(try await spark.catalog.listDatabases(pattern: "*") == dbs)
    #expect(try await spark.catalog.listDatabases(pattern: "non_exist").count == 0)
    await spark.stop()
  }

  @Test
  func getDatabase() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let db = try await spark.catalog.getDatabase("default")
    #expect(db.name == "default")
    #expect(db.catalog == "spark_catalog")
    #expect(db.description == "default database")
    #expect(db.locationUri.hasSuffix("spark-warehouse"))
    try await #require(throws: Error.self) {
      try await spark.catalog.getDatabase("not_exist_database")
    }
    await spark.stop()
  }

  @Test
  func databaseExists() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.catalog.databaseExists("default"))

    let dbName = "DB_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    #expect(try await spark.catalog.databaseExists(dbName) == false)
    try await SQLHelper.withDatabase(spark, dbName) ({
      try await spark.sql("CREATE DATABASE \(dbName)").count()
      #expect(try await spark.catalog.databaseExists(dbName))
    })
    #expect(try await spark.catalog.databaseExists(dbName) == false)
    await spark.stop()
  }
#endif
}
