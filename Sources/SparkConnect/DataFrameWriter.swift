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
import Atomics
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import GRPCProtobuf
import NIOCore
import SwiftyTextTable
import Synchronization

/// An interface used to write a `DataFrame` to external storage systems
/// (e.g. file systems, key-value stores, etc). Use `DataFrame.write` to access this.
public actor DataFrameWriter: Sendable {
  var source: String? = nil

  var saveMode: String = "default"

  var extraOptions: CaseInsensitiveDictionary = CaseInsensitiveDictionary()

  var partitioningColumns: [String]? = nil

  var bucketColumnNames: [String]? = nil

  var numBuckets: Int32? = nil

  var sortColumnNames: [String]? = nil

  var clusteringColumns: [String]? = nil

  let df: DataFrame

  init(df: DataFrame) {
    self.df = df
  }

  /// Specifies the output data source format.
  /// - Parameter source: A string.
  /// - Returns: A `DataFrameReader`.
  public func format(_ source: String) -> DataFrameWriter {
    self.source = source
    return self
  }

  /// Specifies the behavior when data or table already exists. Options include:
  /// `overwrite`, `append`, `ignore`, `error` or `errorifexists` (default).
  ///
  /// - Parameter saveMode: A string for save mode.
  /// - Returns: A `DataFrameWriter`
  public func mode(_ saveMode: String) -> DataFrameWriter {
    self.saveMode = saveMode
    return self
  }

  /// Adds an output option for the underlying data source.
  /// - Parameters:
  ///   - key: A key string.
  ///   - value: A value string.
  /// - Returns: A `DataFrameWriter`.
  public func option(_ key: String, _ value: String) -> DataFrameWriter {
    self.extraOptions[key] = value
    return self
  }

  public func partitionBy(_ columns: String...) -> DataFrameWriter {
    self.partitioningColumns = columns
    return self
  }

  public func bucketBy(numBuckets: Int32, _ columnNames: String...) -> DataFrameWriter {
    self.numBuckets = numBuckets
    self.bucketColumnNames = columnNames
    return self
  }

  public func sortBy(_ columnNames: String...) -> DataFrameWriter {
    self.sortColumnNames = columnNames
    return self
  }

  public func clusterBy(_ columnNames: String...) -> DataFrameWriter {
    self.clusteringColumns = columnNames
    return self
  }

  /// Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
  /// key-value stores).
  public func save() async throws {
    return try await saveInternal(nil)
  }

  /// Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by a
  /// local or distributed file system).
  /// - Parameter path: A path string.
  public func save(_ path: String) async throws {
    try await saveInternal(path)
  }

  private func saveInternal(_ path: String?) async throws {
    try await executeWriteOperation({
      var write = WriteOperation()
      if let path = path {
        write.path = path
      }
      return write
    })
  }

  /// Saves the content of the ``DataFrame`` as the specified table.
  /// - Parameter tableName: A table name.
  public func saveAsTable(_ tableName: String) async throws {
    try await executeWriteOperation({
      var write = WriteOperation()
      write.table.tableName = tableName
      write.table.saveMethod = .saveAsTable
      return write
    })
  }

  /// Inserts the content of the ``DataFrame`` to the specified table. It requires that the schema of
  /// the ``DataFrame`` is the same as the schema of the table. Unlike ``saveAsTable``,
  /// ``insertInto`` ignores the column names and just uses position-based resolution.
  /// - Parameter tableName: A table name.
  public func insertInto(_ tableName: String) async throws {
    try await executeWriteOperation({
      var write = WriteOperation()
      write.table.tableName = tableName
      write.table.saveMethod = .insertInto
      return write
    })
  }

  private func executeWriteOperation(_ f: () -> WriteOperation) async throws {
    var write = f()

    // Cannot both be set
    assert(!(!write.path.isEmpty && !write.table.tableName.isEmpty))

    let plan = await self.df.getPlan() as! Plan
    write.input = plan.root
    write.mode = self.saveMode.toSaveMode

    if let source = self.source {
      write.source = source
    }
    if let sortColumnNames = self.sortColumnNames {
      write.sortColumnNames = sortColumnNames
    }
    if let partitioningColumns = self.partitioningColumns {
      write.partitioningColumns = partitioningColumns
    }
    if let clusteringColumns = self.clusteringColumns {
      write.clusteringColumns = clusteringColumns
    }
    if let numBuckets = self.numBuckets {
      var bucketBy = WriteOperation.BucketBy()
      bucketBy.numBuckets = numBuckets
      if let bucketColumnNames = self.bucketColumnNames {
        bucketBy.bucketColumnNames = bucketColumnNames
      }
      write.bucketBy = bucketBy
    }

    for option in self.extraOptions.toStringDictionary() {
      write.options[option.key] = option.value
    }

    var command = Spark_Connect_Command()
    command.writeOperation = write

    _ = try await df.spark.client.execute(df.spark.sessionID, command)
  }

  /// Saves the content of the `DataFrame` in CSV format at the specified path.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func csv(_ path: String) async throws {
    self.source = "csv"
    return try await save(path)
  }

  /// Saves the content of the `DataFrame` in JSON format at the specified path.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func json(_ path: String) async throws {
    self.source = "json"
    return try await save(path)
  }

  /// Saves the content of the `DataFrame` in XML format at the specified path.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func xml(_ path: String) async throws {
    self.source = "xml"
    return try await save(path)
  }

  /// Saves the content of the `DataFrame` in ORC format at the specified path.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func orc(_ path: String) async throws {
    self.source = "orc"
    return try await save(path)
  }

  /// Saves the content of the `DataFrame` in Parquet format at the specified path.
  /// - Parameter path: A path string
  public func parquet(_ path: String) async throws {
    self.source = "parquet"
    return try await save(path)
  }

  /// Saves the content of the `DataFrame` in a text file at the specified path.
  /// The DataFrame must have only one column that is of string type.
  /// Each row becomes a new line in the output file.
  ///
  /// - Parameter path: A path string
  public func text(_ path: String) async throws {
    self.source = "text"
    return try await save(path)
  }
}
