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
    return try await df.collect()[0][0]!
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
    _ = try await df.count()
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
      CatalogMetadata(name: $0[0]!, description: $0[1])
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
    return try await df.collect()[0][0]!
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
    _ = try await df.count()
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
      Database(name: $0[0]!, catalog: $0[1], description: $0[2], locationUri: $0[3]!)
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
      Database(name: $0[0]!, catalog: $0[1], description: $0[2], locationUri: $0[3]!)
    }.first!
  }

  /// Check if the database with the specified name exists.
  /// - Parameter dbName: name of the database to check existence
  /// - Returns: Indicating whether the database exists.
  public func databaseExists(_ dbName: String) async throws -> Bool {
    return try await self.listDatabases(pattern: dbName).count > 0
  }
}
