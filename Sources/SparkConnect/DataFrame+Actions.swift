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
import GRPCProtobuf
import Synchronization

/// Extension providing action operations on ``DataFrame``.
///
/// Actions are operations that trigger computation and return results.
/// They include row retrieval, metadata queries, persistence,
/// view creation, checkpointing, and I/O operations.
extension DataFrame {

  // MARK: - Execute and Collect

  /// Execute the plan and try to fill `schema` and `batches`.
  private func execute() async throws {
    // Clear all existing batches.
    self.batches.removeAll()

    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      try await service.executePlan(spark.client.getExecutePlanRequest(plan)) {
        response in
        for try await m in response.messages {
          if m.hasSchema {
            // The original schema should arrive before ArrowBatches
            await self.setSchema(m.schema)
          }
          let ipcStreamBytes = m.arrowBatch.data
          if !ipcStreamBytes.isEmpty && m.arrowBatch.rowCount > 0 {
            let IPC_CONTINUATION_TOKEN = Int32(-1)
            // Schema
            assert(ipcStreamBytes[0..<4].int32 == IPC_CONTINUATION_TOKEN)
            let schemaSize = Int64(ipcStreamBytes[4..<8].int32)
            let schema = Data(ipcStreamBytes[8..<(8 + schemaSize)])

            // Arrow IPC Data
            assert(
              ipcStreamBytes[(8 + schemaSize)..<(8 + schemaSize + 4)].int32
                == IPC_CONTINUATION_TOKEN)
            var pos: Int64 = 8 + schemaSize + 4
            let dataHeaderSize = Int64(ipcStreamBytes[pos..<(pos + 4)].int32)
            pos += 4
            let dataHeader = Data(ipcStreamBytes[pos..<(pos + dataHeaderSize)])
            pos += dataHeaderSize
            let dataBodySize = Int64(ipcStreamBytes.count) - pos - 8
            let dataBody = Data(ipcStreamBytes[pos..<(pos + dataBodySize)])

            // Read ArrowBatches
            let reader = ArrowReader()
            let arrowResult = ArrowReader.makeArrowReaderResult()
            _ = reader.fromMessage(schema, dataBody: Data(), result: arrowResult)
            _ = reader.fromMessage(dataHeader, dataBody: dataBody, result: arrowResult)
            await self.addBatches(arrowResult.batches)
          }
        }
      }
    }
  }

  /// Execute the plan and return the result as ``[Row]``.
  /// - Returns: ``[Row]``
  public func collect() async throws -> [Row] {
    try await execute()

    var result: [Row] = []
    for batch in self.batches {
      for i in 0..<batch.length {
        var values: [Sendable?] = []
        for column in batch.columns {
          if column.data.isNull(i) {
            values.append(nil)
          } else {
            let array = column.array
            switch column.data.type.info {
            case .primitiveInfo(.boolean):
              values.append(array.asAny(i) as? Bool)
            case .primitiveInfo(.int8):
              values.append(array.asAny(i) as? Int8)
            case .primitiveInfo(.int16):
              values.append(array.asAny(i) as? Int16)
            case .primitiveInfo(.int32):
              values.append(array.asAny(i) as? Int32)
            case .primitiveInfo(.int64):
              values.append(array.asAny(i) as! Int64)
            case .primitiveInfo(.float):
              values.append(array.asAny(i) as? Float)
            case .primitiveInfo(.double):
              values.append(array.asAny(i) as? Double)
            case .primitiveInfo(.decimal128):
              values.append(array.asAny(i) as? Decimal)
            case .primitiveInfo(.date32):
              values.append(array.asAny(i) as! Date)
            case .timeInfo(.timestamp):
              let timestampType = column.data.type as! ArrowTypeTimestamp
              let timestamp = array.asAny(i) as! Int64
              let timeInterval =
                switch timestampType.unit {
                case .seconds:
                  TimeInterval(timestamp)
                case .milliseconds:
                  TimeInterval(timestamp) / 1_000
                case .microseconds:
                  TimeInterval(timestamp) / 1_000_000
                case .nanoseconds:
                  TimeInterval(timestamp) / 1_000_000_000
                }
              values.append(Date(timeIntervalSince1970: timeInterval))
            case ArrowType.ArrowBinary:
              values.append((array as! AsString).asString(i).utf8)
            case .complexInfo(.strct):
              values.append((array as! AsString).asString(i))
            case .complexInfo(.list):
              values.append(array.asAny(i) as? [any Sendable])
            default:
              values.append(array.asAny(i) as? String)
            }
          }
        }
        result.append(Row(valueArray: values))
      }
    }

    return result
  }

  // MARK: - Count

  /// Return the total number of rows.
  /// - Returns: a `Int64` value.
  @discardableResult
  public func count() async throws -> Int64 {
    let counter = Atomic(Int64(0))

    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      try await service.executePlan(spark.client.getExecutePlanRequest(plan)) {
        response in
        for try await m in response.messages {
          counter.add(m.arrowBatch.rowCount, ordering: .relaxed)
        }
      }
    }
    return counter.load(ordering: .relaxed)
  }

  // MARK: - Show

  /// Displays the top 20 rows of ``DataFrame`` in a tabular form.
  public func show() async throws {
    try await show(20)
  }

  /// Displays the top 20 rows of ``DataFrame`` in a tabular form.
  /// - Parameter truncate: Whether truncate long strings. If true, strings more than 20 characters will be truncated
  /// and all cells will be aligned right
  public func show(_ truncate: Bool) async throws {
    try await show(20, truncate)
  }

  /// Displays the ``DataFrame`` in a tabular form.
  /// - Parameters:
  ///   - numRows: Number of rows to show
  ///   - truncate: Whether truncate long strings. If true, strings more than 20 characters will be truncated
  ///   and all cells will be aligned right
  public func show(_ numRows: Int32 = 20, _ truncate: Bool = true) async throws {
    try await show(numRows, truncate ? 20 : 0)
  }

  /// Displays the ``DataFrame`` in a tabular form.
  /// - Parameters:
  ///   - numRows: Number of rows to show
  ///   - truncate: If set to more than 0, truncates strings to `truncate` characters and all cells will be aligned right.
  ///   - vertical: If set to true, prints output rows vertically (one line per column value).
  public func show(_ numRows: Int32, _ truncate: Int32, _ vertical: Bool = false) async throws {
    let rows = try await showString(numRows, truncate, vertical).collect()
    assert(rows.count == 1)
    assert(rows[0].length == 1)
    print(try rows[0].get(0) as! String)
  }

  func showString(_ numRows: Int32, _ truncate: Int32, _ vertical: Bool) -> DataFrame {
    let plan = SparkConnectClient.getShowString(self.plan.root, numRows, truncate, vertical)
    return DataFrame(spark: self.spark, plan: plan)
  }

  // MARK: - Row Retrieval

  /// Returns the first n rows.
  ///
  /// This method is useful for quickly examining the contents of a DataFrame.
  ///
  /// ```swift
  /// // Get the first row
  /// let firstRow = try await df.head()
  ///
  /// // Get the first 5 rows
  /// let firstFive = try await df.head(5)
  /// ```
  ///
  /// - Parameter n: Number of rows to return.
  /// - Returns: An array of ``Row`` objects
  /// - Throws: `SparkConnectError` if the operation fails
  public func head(_ n: Int32) async throws -> [Row] {
    return try await limit(n).collect()
  }

  /// Returns the first row.
  /// - Returns: A ``Row``.
  public func head() async throws -> Row {
    return try await head(1)[0]
  }

  /// Returns the first row. Alias for head().
  /// - Returns: A ``Row``.
  public func first() async throws -> Row {
    return try await head()
  }

  /// Returns the first n rows.
  /// - Parameter n: Number of rows to return.
  /// - Returns: An array of ``Row`` objects
  /// - Throws: `SparkConnectError` if the operation fails
  public func take(_ n: Int32) async throws -> [Row] {
    return try await head(n)
  }

  /// Returns the last `n` rows.
  /// - Parameter n: The number of rows.
  /// - Returns: ``[Row]``
  public func tail(_ n: Int32) async throws -> [Row] {
    let lastN = DataFrame(spark: spark, plan: SparkConnectClient.getTail(self.plan.root, n))
    return try await lastN.collect()
  }

  // MARK: - Metadata Queries

  /// Returns true if the `collect` and `take` methods can be run locally
  /// (without any Spark executors).
  /// - Returns: True if the plan is local.
  public func isLocal() async throws -> Bool {
    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(spark.client.getIsLocal(spark.sessionID, plan))
      return response.isLocal.isLocal
    }
  }

  /// Returns true if this `DataFrame` contains one or more sources that continuously return data as it
  /// arrives.
  /// - Returns: True if a plan is streaming.
  public func isStreaming() async throws -> Bool {
    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(
        spark.client.getIsStreaming(spark.sessionID, plan))
      return response.isStreaming.isStreaming
    }
  }

  /// Checks if the ``DataFrame`` is empty and returns a boolean value.
  /// - Returns: `true` if the ``DataFrame`` is empty, `false` otherwise.
  public func isEmpty() async throws -> Bool {
    return try await select().limit(1).count() == 0
  }

  // MARK: - Persistence

  /// Persists this DataFrame with the default storage level (MEMORY_AND_DISK).
  ///
  /// Caching can significantly improve performance when a DataFrame is accessed multiple times.
  /// The cached data is stored in memory and/or disk depending on the storage level.
  ///
  /// ```swift
  /// // Cache a frequently used DataFrame
  /// let cachedDf = try await df.cache()
  ///
  /// // Use the cached DataFrame multiple times
  /// let count1 = try await cachedDf.count()
  /// let count2 = try await cachedDf.filter("age > 30").count()
  /// ```
  ///
  /// - Returns: The cached DataFrame
  /// - Throws: `SparkConnectError` if the operation fails
  public func cache() async throws -> DataFrame {
    return try await persist()
  }

  /// Persist this `DataFrame` with the given storage level.
  /// - Parameter storageLevel: A storage level to apply.
  @discardableResult
  public func persist(storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) async throws
    -> DataFrame
  {
    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      _ = try await service.analyzePlan(
        spark.client.getPersist(spark.sessionID, plan, storageLevel))
    }

    return self
  }

  /// Mark the `DataFrame` as non-persistent, and remove all blocks for it from memory and disk.
  /// This will not un-persist any cached data that is built upon this `DataFrame`.
  /// - Parameter blocking: Whether to block until all blocks are deleted.
  /// - Returns: A `DataFrame`
  @discardableResult
  public func unpersist(blocking: Bool = false) async throws -> DataFrame {
    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      _ = try await service.analyzePlan(spark.client.getUnpersist(spark.sessionID, plan, blocking))
    }

    return self
  }

  public var storageLevel: StorageLevel {
    get async throws {
      try await withGPRC { client in
        let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
        return
          try await service
          .analyzePlan(spark.client.getStorageLevel(spark.sessionID, plan)).getStorageLevel
          .storageLevel.toStorageLevel
      }
    }
  }

  /// Returns a `hashCode` of the logical query plan against this ``DataFrame``.
  /// - Returns: A hashcode value.
  public func semanticHash() async throws -> Int32 {
    return try await self.spark.semanticHash(self.plan)
  }

  /// Returns `true` when the logical query plans inside both ``Dataset``s are equal and therefore
  /// return same results.
  /// - Parameter other: A ``DataFrame`` to compare.
  /// - Returns: Whether the both logical plans are equal.
  public func sameSemantics(other: DataFrame) async throws -> Bool {
    return try await self.spark.sameSemantics(self.plan, other.getPlan() as! Plan)
  }

  // MARK: - Explain and Schema Debug

  /// Prints the physical plan to the console for debugging purposes.
  public func explain() async throws {
    try await explain("simple")
  }

  /// Prints the plans (logical and physical) to the console for debugging purposes.
  /// - Parameter extended: If `false`, prints only the physical plan.
  public func explain(_ extended: Bool) async throws {
    if extended {
      try await explain("extended")
    } else {
      try await explain("simple")
    }
  }

  /// Prints the plans (logical and physical) with a format specified by a given explain mode.
  /// - Parameter mode: the expected output format of plans;
  /// `simple`, `extended`,  `codegen`, `cost`,  `formatted`.
  public func explain(_ mode: String) async throws {
    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(
        spark.client.getExplain(spark.sessionID, plan, mode))
      print(response.explain.explainString)
    }
  }

  /// Returns a best-effort snapshot of the files that compose this Dataset. This method simply
  /// asks each constituent BaseRelation for its respective files and takes the union of all
  /// results. Depending on the source relations, this may not find all input files. Duplicates are removed.
  /// - Returns: An array of file path strings.
  public func inputFiles() async throws -> [String] {
    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(
        spark.client.getInputFiles(spark.sessionID, plan))
      return response.inputFiles.files
    }
  }

  /// Prints the schema to the console in a nice tree format.
  public func printSchema() async throws {
    try await printSchema(Int32.max)
  }

  /// Prints the schema up to the given level to the console in a nice tree format.
  /// - Parameter level: A level to be printed.
  public func printSchema(_ level: Int32) async throws {
    try await withGPRC { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(
        spark.client.getTreeString(spark.sessionID, plan, level))
      print(response.treeString.treeString)
    }
  }

  // MARK: - View Creation

  /// Creates a local temporary view using the given name. The lifetime of this temporary view is
  /// tied to the `SparkSession` that was used to create this ``DataFrame``.
  /// - Parameter viewName: A view name.
  public func createTempView(_ viewName: String) async throws {
    try await createTempView(viewName, replace: false, global: false)
  }

  /// Creates a local temporary view using the given name. The lifetime of this temporary view is
  /// tied to the `SparkSession` that was used to create this ``DataFrame``.
  /// - Parameter viewName: A view name.
  public func createOrReplaceTempView(_ viewName: String) async throws {
    try await createTempView(viewName, replace: true, global: false)
  }

  /// Creates a global temporary view using the given name. The lifetime of this temporary view is
  /// tied to this Spark application, but is cross-session.
  /// - Parameter viewName: A view name.
  public func createGlobalTempView(_ viewName: String) async throws {
    try await createTempView(viewName, replace: false, global: true)
  }

  /// Creates a global temporary view using the given name. The lifetime of this temporary view is
  /// tied to this Spark application, but is cross-session.
  /// - Parameter viewName: A view name.
  public func createOrReplaceGlobalTempView(_ viewName: String) async throws {
    try await createTempView(viewName, replace: true, global: true)
  }

  func createTempView(_ viewName: String, replace: Bool, global: Bool) async throws {
    try await spark.client.createTempView(
      self.plan.root, viewName, replace: replace, isGlobal: global)
  }

  // MARK: - Checkpointing

  /// Eagerly checkpoint a ``DataFrame`` and return the new ``DataFrame``.
  /// Checkpointing can be used to truncate the logical plan of this ``DataFrame``,
  /// which is especially useful in iterative algorithms where the plan may grow exponentially.
  /// It will be saved to files inside the checkpoint directory.
  /// - Parameters:
  ///   - eager: Whether to checkpoint this dataframe immediately
  ///   - reliableCheckpoint: Whether to create a reliable checkpoint saved to files inside the checkpoint directory.
  ///   If false creates a local checkpoint using the caching subsystem
  ///   - storageLevel: StorageLevel with which to checkpoint the data.
  /// - Returns: A ``DataFrame``.
  public func checkpoint(
    _ eager: Bool = true,
    _ reliableCheckpoint: Bool = true,
    _ storageLevel: StorageLevel? = nil
  ) async throws -> DataFrame {
    let plan = try await spark.client.getCheckpoint(
      self.plan.root, eager, reliableCheckpoint, storageLevel)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Locally checkpoints a ``DataFrame`` and return the new ``DataFrame``.
  /// Checkpointing can be used to truncate the logical plan of this ``DataFrame``,
  /// which is especially useful in iterative algorithms where the plan may grow exponentially.
  /// Local checkpoints are written to executor storage and despite potentially faster they
  /// are unreliable and may compromise job completion.
  /// - Parameters:
  ///   - eager: Whether to checkpoint this dataframe immediately
  ///   - storageLevel: StorageLevel with which to checkpoint the data.
  /// - Returns: A ``DataFrame``.
  public func localCheckpoint(
    _ eager: Bool = true,
    _ storageLevel: StorageLevel? = nil
  ) async throws -> DataFrame {
    try await checkpoint(eager, false, storageLevel)
  }

  // MARK: - I/O

  /// Returns a ``DataFrameWriter`` that can be used to write non-streaming data.
  public var write: DataFrameWriter {
    DataFrameWriter(df: self)
  }

  /// Create a write configuration builder for v2 sources.
  /// - Parameter table: A table name, e.g., `catalog.db.table`.
  /// - Returns: A ``DataFrameWriterV2`` instance.
  public func writeTo(_ table: String) -> DataFrameWriterV2 {
    return DataFrameWriterV2(table, self)
  }

  /// Merges a set of updates, insertions, and deletions based on a source table into a target table.
  /// - Parameters:
  ///   - table: A target table name.
  ///   - condition: A condition expression.
  /// - Returns: A ``MergeIntoWriter`` instance.
  public func mergeInto(_ table: String, _ condition: String) async -> MergeIntoWriter {
    return MergeIntoWriter(table, self, condition)
  }

  /// Returns a ``DataStreamWriter`` that can be used to write streaming data.
  public var writeStream: DataStreamWriter {
    DataStreamWriter(df: self)
  }
}
