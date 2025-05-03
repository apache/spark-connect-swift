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

public struct CatalogMetadata: Sendable, Equatable {
  public var name: String
  public var description: String? = nil
}

public struct Database: Sendable, Equatable {
  public var name: String
  public var catalog: String? = nil
  public var description: String? = nil
  public var locationUri: String
}

// TODO: Rename `SparkTable` to `Table` after removing Arrow and Flatbuffer
// from `SparkConnect` module. Currently, `SparkTable` is used to avoid the name conflict.
public struct SparkTable: Sendable, Equatable {
  public var name: String
  public var catalog: String?
  public var namespace: [String]?
  public var description: String?
  public var tableType: String
  public var isTemporary: Bool
  public var database: String? {
    get {
      guard let namespace else {
        return nil
      }
      if namespace.count == 1 {
        return namespace[0]
      } else {
        return nil
      }
    }
  }
}

public struct Column: Sendable, Equatable {
  public var name: String
  public var description: String?
  public var dataType: String
  public var nullable: Bool
  public var isPartition: Bool
  public var isBucket: Bool
  public var isCluster: Bool
}

public struct Function: Sendable, Equatable {
  public var name: String
  public var catalog: String?
  public var namespace: [String]?
  public var description: String?
  public var className: String
  public var isTemporary: Bool
}

/// Interface through which the user may create, drop, alter or query underlying databases, tables, functions etc.
/// To access this, use ``SparkSession.catalog``.
public actor Catalog: Sendable {
  var spark: SparkSession

  init(spark: SparkSession) {
    self.spark = spark
  }

  /// A helper method to create a `Spark_Connect_Catalog`-based plan.
  /// - Parameter f: A lambda function to create `Spark_Connect_Catalog`.
  /// - Returns: A ``DataFrame`` contains the result of the given catalog operation.
  private func getDataFrame(_ f: () -> Spark_Connect_Catalog) -> DataFrame {
    var relation = Relation()
    relation.catalog = f()
    var plan = Plan()
    plan.opType = .root(relation)
    return DataFrame(spark: spark, plan: plan)
  }

  /// Returns the current default catalog in this session.
  /// - Returns: A catalog name.
  public func currentCatalog() async throws -> String {
    let df = getDataFrame({
      var catalog = Spark_Connect_Catalog()
      catalog.catType = .currentCatalog(Spark_Connect_CurrentCatalog())
      return catalog
    })
    return try await df.collect()[0][0] as! String
  }

  /// Sets the current default catalog in this session.
  /// - Parameter catalogName: name of the catalog to set
  public func setCurrentCatalog(_ catalogName: String) async throws {
    let df = getDataFrame({
      var setCurrentCatalog = Spark_Connect_SetCurrentCatalog()
      setCurrentCatalog.catalogName = catalogName

      var catalog = Spark_Connect_Catalog()
      catalog.catType = .setCurrentCatalog(setCurrentCatalog)
      return catalog
    })
    try await df.count()
  }

  /// Returns a list of catalogs in this session.
  /// - Returns: A list of ``CatalogMetadata``.
  public func listCatalogs(pattern: String? = nil) async throws -> [CatalogMetadata] {
    let df = getDataFrame({
      var listCatalogs = Spark_Connect_ListCatalogs()
      if let pattern {
        listCatalogs.pattern = pattern
      }
      var catalog = Spark_Connect_Catalog()
      catalog.catType = .listCatalogs(listCatalogs)
      return catalog
    })
    return try await df.collect().map {
      try CatalogMetadata(name: $0[0] as! String, description: $0[1] as? String)
    }
  }

  /// Returns the current default database in this session.
  /// - Returns: The current default database name.
  public func currentDatabase() async throws -> String {
    let df = getDataFrame({
      var catalog = Spark_Connect_Catalog()
      catalog.catType = .currentDatabase(Spark_Connect_CurrentDatabase())
      return catalog
    })
    return try await df.collect()[0][0] as! String
  }

  /// Sets the current default database in this session.
  /// - Parameter dbName: name of the catalog to set
  public func setCurrentDatabase(_ dbName: String) async throws {
    let df = getDataFrame({
      var setCurrentDatabase = Spark_Connect_SetCurrentDatabase()
      setCurrentDatabase.dbName = dbName

      var catalog = Spark_Connect_Catalog()
      catalog.catType = .setCurrentDatabase(setCurrentDatabase)
      return catalog
    })
    try await df.count()
  }

  /// Returns a list of databases available across all sessions.
  /// - Parameter pattern: The pattern that the database name needs to match.
  /// - Returns: A list of ``Database``.
  public func listDatabases(pattern: String? = nil) async throws -> [Database] {
    let df = getDataFrame({
      var listDatabases = Spark_Connect_ListDatabases()
      if let pattern {
        listDatabases.pattern = pattern
      }
      var catalog = Spark_Connect_Catalog()
      catalog.catType = .listDatabases(listDatabases)
      return catalog
    })
    return try await df.collect().map {
      try Database(name: $0[0] as! String, catalog: $0[1] as? String, description: $0[2] as? String, locationUri: $0[3] as! String)
    }
  }

  /// Get the database with the specified name.
  /// - Parameter dbName: name of the database to get.
  /// - Returns: The database found by the name.
  public func getDatabase(_ dbName: String) async throws -> Database {
    let df = getDataFrame({
      var db = Spark_Connect_GetDatabase()
      db.dbName = dbName
      var catalog = Spark_Connect_Catalog()
      catalog.catType = .getDatabase(db)
      return catalog
    })
    return try await df.collect().map {
      try Database(name: $0[0] as! String, catalog: $0[1] as? String, description: $0[2] as? String, locationUri: $0[3] as! String)
    }.first!
  }

  /// Check if the database with the specified name exists.
  /// - Parameter dbName: name of the database to check existence
  /// - Returns: Indicating whether the database exists.
  public func databaseExists(_ dbName: String) async throws -> Bool {
    return try await self.listDatabases(pattern: dbName).count > 0
  }

  /// Creates a table from the given path and returns the corresponding ``DataFrame``.
  /// - Parameters:
  ///   - tableName: A qualified or unqualified name that designates a table. If no database
  ///   identifier is provided, it refers to a table in the current database.
  ///   - path: A path to load a table.
  ///   - source: A data source.
  ///   - description: A table description.
  ///   - options: A dictionary for table options
  /// - Returns: A ``DataFrame``.
  public func createTable(
    _ tableName: String,
    _ path: String? = nil,
    source: String? = nil,
    description: String? = nil,
    options: [String: String]? = nil
  ) -> DataFrame {
    let df = getDataFrame({
      var createTable = Spark_Connect_CreateTable()
      createTable.tableName = tableName
      if let source {
        createTable.source = source
      }
      createTable.description_p = description ?? ""
      if let options {
        for (k, v) in options {
          createTable.options[k] = v
        }
      }
      if let path {
        createTable.options["path"] = path
      }
      var catalog = Spark_Connect_Catalog()
      catalog.createTable = createTable
      return catalog
    })
    return df
  }

  /// Check if the table or view with the specified name exists. This can either be a temporary
  /// view or a table/view.
  /// - Parameter tableName: a qualified or unqualified name that designates a table/view. It follows the same
  /// resolution rule with SQL: search for temp views first then table/views in the current
  /// database (namespace).
  /// - Returns: Return true if it exists.
  public func tableExists(_ tableName: String) async throws -> Bool {
    let df = getDataFrame({
      var tableExists = Spark_Connect_TableExists()
      tableExists.tableName = tableName
      var catalog = Spark_Connect_Catalog()
      catalog.tableExists = tableExists
      return catalog
    })
    return try await df.collect()[0].getAsBool(0)
  }

  /// Check if the table or view with the specified name exists. This can either be a temporary
  /// view or a table/view.
  /// - Parameters:
  ///   - dbName: an unqualified name that designates a database.
  ///   - tableName: an unqualified name that designates a table.
  /// - Returns: Return true if it exists.
  public func tableExists(_ dbName: String, _ tableName: String) async throws -> Bool {
    let df = getDataFrame({
      var tableExists = Spark_Connect_TableExists()
      tableExists.tableName = tableName
      tableExists.dbName = dbName
      var catalog = Spark_Connect_Catalog()
      catalog.tableExists = tableExists
      return catalog
    })
    return try await df.collect()[0].getAsBool(0)
  }

  /// Returns a list of columns for the given table/view or temporary view.
  /// - Parameter tableName: a qualified or unqualified name that designates a table/view. It follows the same
  /// resolution rule with SQL: search for temp views first then table/views in the current
  /// database (namespace).
  /// - Returns: A ``DataFrame`` of ``Column``.
  public func listColumns(_ tableName: String) async throws -> DataFrame {
    let df = getDataFrame({
      var listColumns = Spark_Connect_ListColumns()
      listColumns.tableName = tableName
      var catalog = Spark_Connect_Catalog()
      catalog.listColumns = listColumns
      return catalog
    })
    return df
  }

  /// Check if the function with the specified name exists. This can either be a temporary function
  /// or a function.
  /// - Parameter functionName: a qualified or unqualified name that designates a function. It follows the same
  /// resolution rule with SQL: search for built-in/temp functions first then functions in the
  /// current database (namespace).
  /// - Returns: Return true if it exists.
  public func functionExists(_ functionName: String) async throws -> Bool {
    let df = getDataFrame({
      var functionExists = Spark_Connect_FunctionExists()
      functionExists.functionName = functionName
      var catalog = Spark_Connect_Catalog()
      catalog.functionExists = functionExists
      return catalog
    })
    return try await df.collect()[0].getAsBool(0)
  }

  /// Check if the function with the specified name exists in the specified database under the Hive
  /// Metastore.
  /// - Parameters:
  ///   - dbName: an unqualified name that designates a database.
  ///   - functionName: an unqualified name that designates a function.
  /// - Returns: Return true if it exists.
  public func functionExists(_ dbName: String, _ functionName: String) async throws -> Bool {
    let df = getDataFrame({
      var functionExists = Spark_Connect_FunctionExists()
      functionExists.functionName = functionName
      functionExists.dbName = dbName
      var catalog = Spark_Connect_Catalog()
      catalog.functionExists = functionExists
      return catalog
    })
    return try await df.collect()[0].getAsBool(0)
  }

  /// Caches the specified table in-memory.
  /// - Parameters:
  ///   - tableName: A qualified or unqualified name that designates a table/view.
  ///   If no database identifier is provided, it refers to a temporary view or a table/view in the current database.
  ///   - storageLevel: storage level to cache table.
  public func cacheTable(_ tableName: String, _ storageLevel: StorageLevel? = nil) async throws {
    let df = getDataFrame({
      var cacheTable = Spark_Connect_CacheTable()
      cacheTable.tableName = tableName
      if let storageLevel {
        cacheTable.storageLevel = storageLevel.toSparkConnectStorageLevel
      }
      var catalog = Spark_Connect_Catalog()
      catalog.cacheTable = cacheTable
      return catalog
    })
    try await df.count()
  }

  /// Returns true if the table is currently cached in-memory.
  /// - Parameter tableName: A qualified or unqualified name that designates a table/view.
  /// If no database identifier is provided, it refers to a temporary view or a table/view in the current database.
  public func isCached(_ tableName: String) async throws -> Bool {
    let df = getDataFrame({
      var isCached = Spark_Connect_IsCached()
      isCached.tableName = tableName
      var catalog = Spark_Connect_Catalog()
      catalog.isCached = isCached
      return catalog
    })
    return try await df.collect()[0].getAsBool(0)
  }

  /// Invalidates and refreshes all the cached data and metadata of the given table.
  /// - Parameter tableName: A qualified or unqualified name that designates a table/view.
  /// If no database identifier is provided, it refers to a temporary view or a table/view in the current database.
  public func refreshTable(_ tableName: String) async throws {
    let df = getDataFrame({
      var refreshTable = Spark_Connect_RefreshTable()
      refreshTable.tableName = tableName
      var catalog = Spark_Connect_Catalog()
      catalog.refreshTable = refreshTable
      return catalog
    })
    try await df.count()
  }

  /// Invalidates and refreshes all the cached data (and the associated metadata) for any ``DataFrame``
  /// that contains the given data source path. Path matching is by checking for sub-directories,
  /// i.e. "/" would invalidate everything that is cached and "/test/parent" would invalidate
  /// everything that is a subdirectory of "/test/parent".
  public func refreshByPath(_ path: String) async throws {
    let df = getDataFrame({
      var refreshByPath = Spark_Connect_RefreshByPath()
      refreshByPath.path = path
      var catalog = Spark_Connect_Catalog()
      catalog.refreshByPath = refreshByPath
      return catalog
    })
    try await df.count()
  }

  /// Removes the specified table from the in-memory cache.
  /// - Parameter tableName: A qualified or unqualified name that designates a table/view.
  /// If no database identifier is provided, it refers to a temporary view or a table/view in the current database.
  public func uncacheTable(_ tableName: String) async throws {
    let df = getDataFrame({
      var uncacheTable = Spark_Connect_UncacheTable()
      uncacheTable.tableName = tableName
      var catalog = Spark_Connect_Catalog()
      catalog.uncacheTable = uncacheTable
      return catalog
    })
    try await df.count()
  }

  /// Removes all cached tables from the in-memory cache.
  public func clearCache() async throws {
    let df = getDataFrame({
      var catalog = Spark_Connect_Catalog()
      catalog.clearCache_p = Spark_Connect_ClearCache()
      return catalog
    })
    try await df.count()
  }

  /// Drops the local temporary view with the given view name in the catalog. If the view has been
  /// cached before, then it will also be uncached.
  /// - Parameter viewName: The name of the temporary view to be dropped.
  /// - Returns: true if the view is dropped successfully, false otherwise.
  @discardableResult
  public func dropTempView(_ viewName: String) async throws -> Bool {
    let df = getDataFrame({
      var dropTempView = Spark_Connect_DropTempView()
      dropTempView.viewName = viewName
      var catalog = Spark_Connect_Catalog()
      catalog.dropTempView = dropTempView
      return catalog
    })
    return try await df.collect().first!.getAsBool(0)
  }

  /// Drops the global temporary view with the given view name in the catalog. If the view has been
  /// cached before, then it will also be uncached.
  /// - Parameter viewName: The unqualified name of the temporary view to be dropped.
  /// - Returns: true if the view is dropped successfully, false otherwise.
  @discardableResult
  public func dropGlobalTempView(_ viewName: String) async throws -> Bool {
    let df = getDataFrame({
      var dropGlobalTempView = Spark_Connect_DropGlobalTempView()
      dropGlobalTempView.viewName = viewName
      var catalog = Spark_Connect_Catalog()
      catalog.dropGlobalTempView = dropGlobalTempView
      return catalog
    })
    return try await df.collect()[0].getAsBool(0)
  }
}
