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
@Suite(.serialized)
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

  @Test
  func createTable() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(1).write.orc("/tmp/\(tableName)")
      #expect(try await spark.catalog.createTable(tableName, "/tmp/\(tableName)", source: "orc").count() == 1)
      #expect(try await spark.catalog.tableExists(tableName))
    })
    await spark.stop()
  }

  @Test
  func tableExists() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(1).write.parquet("/tmp/\(tableName)")
      #expect(try await spark.catalog.tableExists(tableName) == false)
      #expect(try await spark.catalog.createTable(tableName, "/tmp/\(tableName)").count() == 1)
      #expect(try await spark.catalog.tableExists(tableName))
      #expect(try await spark.catalog.tableExists("default", tableName))
      #expect(try await spark.catalog.tableExists("default2", tableName) == false)
    })
    #expect(try await spark.catalog.tableExists(tableName) == false)

    try await #require(throws: Error.self) {
      try await spark.catalog.tableExists("invalid table name")
    }
    await spark.stop()
  }

  @Test
  func listColumns() async throws {
    let spark = try await SparkSession.builder.getOrCreate()

    // Table
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    let path = "/tmp/\(tableName)"
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(2).write.orc(path)
      let expected = if await spark.version.starts(with: "4.") {
        [Row("id", nil, "bigint", true, false, false, false)]
      } else {
        [Row("id", nil, "bigint", true, false, false)]
      }
      #expect(try await spark.catalog.createTable(tableName, path, source: "orc").count() == 2)
      #expect(try await spark.catalog.listColumns(tableName).collect() == expected)
      #expect(try await spark.catalog.listColumns("default.\(tableName)").collect() == expected)
    })

    // View
    let viewName = "VIEW_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTempView(spark, viewName)({
      try await spark.range(1).createTempView(viewName)
      let expected = if await spark.version.starts(with: "4.") {
        [Row("id", nil, "bigint", false, false, false, false)]
      } else {
        [Row("id", nil, "bigint", false, false, false)]
      }
      #expect(try await spark.catalog.listColumns(viewName).collect() == expected)
    })

    await spark.stop()
  }

  @Test
  func functionExists() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    #expect(try await spark.catalog.functionExists("base64"))
    #expect(try await spark.catalog.functionExists("non_exist_function") == false)

    try await #require(throws: Error.self) {
      try await spark.catalog.functionExists("invalid function name")
    }
    await spark.stop()
  }

  @Test
  func createTempView() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let viewName = "VIEW_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTempView(spark, viewName)({
      #expect(try await spark.catalog.tableExists(viewName) == false)
      try await spark.range(1).createTempView(viewName)
      #expect(try await spark.catalog.tableExists(viewName))

      try await #require(throws: Error.self) {
        try await spark.range(1).createTempView(viewName)
      }
    })

    try await #require(throws: Error.self) {
      try await spark.range(1).createTempView("invalid view name")
    }

    await spark.stop()
  }

  @Test
  func createOrReplaceTempView() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let viewName = "VIEW_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTempView(spark, viewName)({
      #expect(try await spark.catalog.tableExists(viewName) == false)
      try await spark.range(1).createOrReplaceTempView(viewName)
      #expect(try await spark.catalog.tableExists(viewName))
      try await spark.range(1).createOrReplaceTempView(viewName)
    })

    try await #require(throws: Error.self) {
      try await spark.range(1).createOrReplaceTempView("invalid view name")
    }

    await spark.stop()
  }

  @Test
  func createGlobalTempView() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let viewName = "VIEW_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withGlobalTempView(spark, viewName)({
      #expect(try await spark.catalog.tableExists("global_temp.\(viewName)") == false)
      try await spark.range(1).createGlobalTempView(viewName)
      #expect(try await spark.catalog.tableExists("global_temp.\(viewName)"))

      try await #require(throws: Error.self) {
        try await spark.range(1).createGlobalTempView(viewName)
      }
    })
    #expect(try await spark.catalog.tableExists("global_temp.\(viewName)") == false)

    try await #require(throws: Error.self) {
      try await spark.range(1).createGlobalTempView("invalid view name")
    }

    await spark.stop()
  }

  @Test
  func createOrReplaceGlobalTempView() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let viewName = "VIEW_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withGlobalTempView(spark, viewName)({
      #expect(try await spark.catalog.tableExists("global_temp.\(viewName)") == false)
      try await spark.range(1).createOrReplaceGlobalTempView(viewName)
      #expect(try await spark.catalog.tableExists("global_temp.\(viewName)"))
      try await spark.range(1).createOrReplaceGlobalTempView(viewName)
    })
    #expect(try await spark.catalog.tableExists("global_temp.\(viewName)") == false)

    try await #require(throws: Error.self) {
      try await spark.range(1).createOrReplaceGlobalTempView("invalid view name")
    }

    await spark.stop()
  }

  @Test
  func dropTempView() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let viewName = "VIEW_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTempView(spark, viewName)({      #expect(try await spark.catalog.tableExists(viewName) == false)
      try await spark.range(1).createTempView(viewName)
      try await spark.catalog.dropTempView(viewName)
      #expect(try await spark.catalog.tableExists(viewName) == false)
    })

    #expect(try await spark.catalog.dropTempView("non_exist_view") == false)
    #expect(try await spark.catalog.dropTempView("invalid view name") == false)
    await spark.stop()
  }

  @Test
  func dropGlobalTempView() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let viewName = "VIEW_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTempView(spark, viewName)({      #expect(try await spark.catalog.tableExists(viewName) == false)
      try await spark.range(1).createGlobalTempView(viewName)
      #expect(try await spark.catalog.tableExists("global_temp.\(viewName)"))
      try await spark.catalog.dropGlobalTempView(viewName)
      #expect(try await spark.catalog.tableExists("global_temp.\(viewName)") == false)
    })

    #expect(try await spark.catalog.dropGlobalTempView("non_exist_view") == false)
    #expect(try await spark.catalog.dropGlobalTempView("invalid view name") == false)
    await spark.stop()
  }
#endif

  @Test
  func cacheTable() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(1).write.saveAsTable(tableName)
      try await spark.catalog.cacheTable(tableName)
      #expect(try await spark.catalog.isCached(tableName))
      try await spark.catalog.cacheTable(tableName, StorageLevel.MEMORY_ONLY)
    })

    try await #require(throws: Error.self) {
      try await spark.catalog.cacheTable("not_exist_table")
    }
    await spark.stop()
  }

  @Test
  func isCached() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(1).write.saveAsTable(tableName)
      #expect(try await spark.catalog.isCached(tableName) == false)
      try await spark.catalog.cacheTable(tableName)
      #expect(try await spark.catalog.isCached(tableName))
    })

    try await #require(throws: Error.self) {
      try await spark.catalog.isCached("not_exist_table")
    }
    await spark.stop()
  }

  @Test
  func refreshTable() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(1).write.saveAsTable(tableName)
      try await spark.catalog.refreshTable(tableName)
      #expect(try await spark.catalog.isCached(tableName) == false)

      try await spark.catalog.cacheTable(tableName)
      #expect(try await spark.catalog.isCached(tableName))
      try await spark.catalog.refreshTable(tableName)
      #expect(try await spark.catalog.isCached(tableName))
    })

    try await #require(throws: Error.self) {
      try await spark.catalog.refreshTable("not_exist_table")
    }
    await spark.stop()
  }

  @Test
  func refreshByPath() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(1).write.saveAsTable(tableName)
      try await spark.catalog.refreshByPath("/")
      #expect(try await spark.catalog.isCached(tableName) == false)

      try await spark.catalog.cacheTable(tableName)
      #expect(try await spark.catalog.isCached(tableName))
      try await spark.catalog.refreshByPath("/")
      #expect(try await spark.catalog.isCached(tableName))
    })
    await spark.stop()
  }

  @Test
  func uncacheTable() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(1).write.saveAsTable(tableName)
      try await spark.catalog.cacheTable(tableName)
      #expect(try await spark.catalog.isCached(tableName))
      try await spark.catalog.uncacheTable(tableName)
      #expect(try await spark.catalog.isCached(tableName) == false)
    })

    try await #require(throws: Error.self) {
      try await spark.catalog.uncacheTable("not_exist_table")
    }
    await spark.stop()
  }

  @Test
  func clearCache() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let tableName = "TABLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await SQLHelper.withTable(spark, tableName)({
      try await spark.range(1).write.saveAsTable(tableName)
      try await spark.catalog.cacheTable(tableName)
      #expect(try await spark.catalog.isCached(tableName))
      try await spark.catalog.clearCache()
      #expect(try await spark.catalog.isCached(tableName) == false)
    })
    await spark.stop()
  }
}
