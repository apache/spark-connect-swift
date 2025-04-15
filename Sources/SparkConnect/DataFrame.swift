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

/// A DataFrame which supports only SQL queries
public actor DataFrame: Sendable {
  var spark: SparkSession
  var plan: Plan
  private var _schema: DataType? = nil
  private var batches: [RecordBatch] = [RecordBatch]()

  /// Create a new `DataFrame`instance with the given Spark session and plan.
  /// - Parameters:
  ///   - spark: A ``SparkSession`` instance to use.
  ///   - plan: A plan to execute.
  init(spark: SparkSession, plan: Plan) {
    self.spark = spark
    self.plan = plan
  }

  /// Create a new `DataFrame` instance with the given SparkSession and a SQL statement.
  /// - Parameters:
  ///   - spark: A `SparkSession` instance to use.
  ///   - sqlText: A SQL statement.
  init(spark: SparkSession, sqlText: String) async throws {
    self.spark = spark
    self.plan = sqlText.toSparkConnectPlan
  }

  public func getPlan() -> Sendable {
    return self.plan
  }

  /// Set the schema. This is used to store the analized schema response from `Spark Connect` server.
  /// - Parameter schema: <#schema description#>
  private func setSchema(_ schema: DataType) {
    self._schema = schema
  }

  /// Add `Apache Arrow`'s `RecordBatch`s to the internal array.
  /// - Parameter batches: An array of ``RecordBatch``.
  private func addBatches(_ batches: [RecordBatch]) {
    self.batches.append(contentsOf: batches)
  }

  /// Return the `SparkSession` of this `DataFrame`.
  public var sparkSession: SparkSession {
    get async throws {
      return self.spark
    }
  }

  /// A method to access the underlying Spark's `RDD`.
  /// In `Spark Connect`, this feature is not allowed by design.
  public func rdd() throws {
    // SQLSTATE: 0A000
    // [UNSUPPORTED_CONNECT_FEATURE.RDD]
    // Feature is not supported in Spark Connect: Resilient Distributed Datasets (RDDs).
    throw SparkConnectError.UnsupportedOperationException
  }

  /// Return an array of column name strings
  public var columns: [String] {
    get async throws {
      var columns: [String] = []
      try await analyzePlanIfNeeded()
      for field in self._schema!.struct.fields {
        columns.append(field.name)
      }
      return columns
    }
  }

  /// Return a `JSON` string of data type because we cannot expose the internal type ``DataType``.
  public var schema: String {
    get async throws {
      try await analyzePlanIfNeeded()
      return try self._schema!.jsonString()
    }
  }

  /// Returns all column names and their data types as an array.
  public var dtypes: [(String, String)] {
    get async throws {
      try await analyzePlanIfNeeded()
      return try self._schema!.struct.fields.map { ($0.name, try $0.dataType.simpleString) }
    }
  }

  private func analyzePlanIfNeeded() async throws {
    if self._schema != nil {
      return
    }
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(
        spark.client.getAnalyzePlanRequest(spark.sessionID, plan))
      self.setSchema(response.schema.schema)
    }
  }

  /// Return the total number of rows.
  /// - Returns: a `Int64` value.
  public func count() async throws -> Int64 {
    let counter = Atomic(Int64(0))

    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
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

  /// Execute the plan and try to fill `schema` and `batches`.
  private func execute() async throws {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
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

  /// Execute the plan and return the result as ``[[String?]]``.
  /// - Returns: ``[[String?]]``
  public func collect() async throws -> [[String?]] {
    try await execute()

    var result: [[String?]] = []
    for batch in self.batches {
      for i in 0..<batch.length {
        var values: [String?] = []
        for column in batch.columns {
          let str = column.array as! AsString
          if column.data.isNull(i) {
            values.append(nil)
          } else if column.data.type.info == ArrowType.ArrowBinary {
            let binary = str.asString(i).utf8.map { String(format: "%02x", $0) }.joined(separator: " ")
            values.append("[\(binary)]")
          } else {
            values.append(str.asString(i))
          }
        }
        result.append(values)
      }
    }

    return result
  }

  /// Execute the plan and show the result.
  public func show() async throws {
    try await execute()

    if let schema = self._schema {
      var columns: [TextTableColumn] = []
      for f in schema.struct.fields {
        columns.append(TextTableColumn(header: f.name))
      }
      var table = TextTable(columns: columns)
      for batch in self.batches {
        for i in 0..<batch.length {
          var values: [String] = []
          for column in batch.columns {
            let str = column.array as! AsString
            if column.data.isNull(i) {
              values.append("NULL")
            } else if column.data.type.info == ArrowType.ArrowBinary {
              let binary = str.asString(i).utf8.map { String(format: "%02x", $0) }.joined(separator: " ")
              values.append("[\(binary)]")
            } else {
              values.append(str.asString(i))
            }
          }
          table.addRow(values: values)
        }
      }
      print(table.render())
    }
  }

  /// Projects a set of expressions and returns a new ``DataFrame``.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func select(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getProject(self.plan.root, cols))
  }

  /// Returns a new Dataset with a column dropped. This is a no-op if schema doesn't contain column name.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func drop(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getDrop(self.plan.root, cols))
  }

  /// Returns a new Dataset with a column renamed. This is a no-op if schema doesn't contain existingName.
  /// - Parameters:
  ///   - existingName: A existing column name to be renamed.
  ///   - newName: A new column name.
  /// - Returns: A ``DataFrame`` with the renamed column.
  public func withColumnRenamed(_ existingName: String, _ newName: String) -> DataFrame {
    return withColumnRenamed([existingName: newName])
  }

  /// Returns a new Dataset with columns renamed. This is a no-op if schema doesn't contain existingName.
  /// - Parameters:
  ///   - colNames: A list of existing colum names to be renamed.
  ///   - newColNames: A list of new column names.
  /// - Returns: A ``DataFrame`` with the renamed columns.
  public func withColumnRenamed(_ colNames: [String], _ newColNames: [String]) -> DataFrame {
    let dic = Dictionary(uniqueKeysWithValues: zip(colNames, newColNames))
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getWithColumnRenamed(self.plan.root, dic))
  }

  /// Returns a new Dataset with columns renamed. This is a no-op if schema doesn't contain existingName.
  /// - Parameter colsMap: A dictionary of existing column name and new column name.
  /// - Returns: A ``DataFrame`` with the renamed columns.
  public func withColumnRenamed(_ colsMap: [String: String]) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getWithColumnRenamed(self.plan.root, colsMap))
  }

  /// Return a new ``DataFrame`` with filtered rows using the given expression.
  /// - Parameter conditionExpr: A string to filter.
  /// - Returns: A ``DataFrame`` with subset of rows.
  public func filter(_ conditionExpr: String) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getFilter(self.plan.root, conditionExpr))
  }

  /// Return a new ``DataFrame`` with filtered rows using the given expression.
  /// - Parameter conditionExpr: A string to filter.
  /// - Returns: A ``DataFrame`` with subset of rows.
  public func `where`(_ conditionExpr: String) -> DataFrame {
    return filter(conditionExpr)
  }

  /// Return a new ``DataFrame`` sorted by the specified column(s).
  /// - Parameter cols: Column names.
  /// - Returns: A sorted ``DataFrame``
  public func sort(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getSort(self.plan.root, cols))
  }

  /// Return a new ``DataFrame`` sorted by the specified column(s).
  /// - Parameter cols: Column names.
  /// - Returns: A sorted ``DataFrame``
  public func orderBy(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getSort(self.plan.root, cols))
  }

  /// Limits the result count to the number specified.
  /// - Parameter n: Number of records to return. Will return this number of records or all records if the ``DataFrame`` contains less than this number of records.
  /// - Returns: A subset of the records
  public func limit(_ n: Int32) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getLimit(self.plan.root, n))
  }

  /// Returns a new Dataset by skipping the first `n` rows.
  /// - Parameter n: Number of rows to skip.
  /// - Returns: A subset of the rows
  public func offset(_ n: Int32) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getOffset(self.plan.root, n))
  }

  /// Returns a new ``Dataset`` by sampling a fraction of rows, using a user-supplied seed.
  /// - Parameters:
  ///   - withReplacement: Sample with replacement or not.
  ///   - fraction: Fraction of rows to generate, range [0.0, 1.0].
  ///   - seed: Seed for sampling.
  /// - Returns: A subset of the records.
  public func sample(_ withReplacement: Bool, _ fraction: Double, _ seed: Int64) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getSample(self.plan.root, withReplacement, fraction, seed))
  }

  /// Returns a new ``Dataset`` by sampling a fraction of rows, using a random seed.
  /// - Parameters:
  ///   - withReplacement: Sample with replacement or not.
  ///   - fraction: Fraction of rows to generate, range [0.0, 1.0].
  /// - Returns: A subset of the records.
  public func sample(_ withReplacement: Bool, _ fraction: Double) -> DataFrame {
    return sample(withReplacement, fraction, Int64.random(in: Int64.min...Int64.max))
  }

  /// Returns a new ``Dataset`` by sampling a fraction of rows (without replacement), using a
  /// user-supplied seed.
  /// - Parameters:
  ///   - fraction: Fraction of rows to generate, range [0.0, 1.0].
  ///   - seed: Seed for sampling.
  /// - Returns: A subset of the records.
  public func sample(_ fraction: Double, _ seed: Int64) -> DataFrame {
    return sample(false, fraction, seed)
  }

  /// Returns a new ``Dataset`` by sampling a fraction of rows (without replacement), using a
  /// random seed.
  /// - Parameters:
  ///   - fraction: Fraction of rows to generate, range [0.0, 1.0].
  /// - Returns: A subset of the records.
  public func sample(_ fraction: Double) -> DataFrame {
    return sample(false, fraction)
  }

  /// Returns the first `n` rows.
  /// - Parameter n: The number of rows. (default: 1)
  /// - Returns: ``[[String?]]``
  public func head(_ n: Int32 = 1) async throws -> [[String?]] {
    return try await limit(n).collect()
  }

  /// Returns the last `n` rows.
  /// - Parameter n: The number of rows.
  /// - Returns: ``[[String?]]``
  public func tail(_ n: Int32) async throws -> [[String?]] {
    let lastN = DataFrame(spark:spark, plan: SparkConnectClient.getTail(self.plan.root, n))
    return try await lastN.collect()
  }

  /// Checks if the ``DataFrame`` is empty and returns a boolean value.
  /// - Returns: `true` if the ``DataFrame`` is empty, `false` otherwise.
  public func isEmpty() async throws -> Bool {
    return try await select().limit(1).count() == 0
  }

  /// Persist this `DataFrame` with the default storage level (`MEMORY_AND_DISK`).
  /// - Returns: A `DataFrame`.
  public func cache() async throws -> DataFrame {
    return try await persist()
  }

  /// Persist this `DataFrame` with the given storage level.
  /// - Parameter storageLevel: A storage level to apply.
  public func persist(storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) async throws
    -> DataFrame
  {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
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
  public func unpersist(blocking: Bool = false) async throws -> DataFrame {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      _ = try await service.analyzePlan(spark.client.getUnpersist(spark.sessionID, plan, blocking))
    }

    return self
  }

  public var storageLevel: StorageLevel {
    get async throws {
      try await withGRPCClient(
        transport: .http2NIOPosix(
          target: .dns(host: spark.client.host, port: spark.client.port),
          transportSecurity: .plaintext
        )
      ) { client in
        let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
        return try await service
          .analyzePlan(spark.client.getStorageLevel(spark.sessionID, plan)).getStorageLevel.storageLevel.toStorageLevel
      }
    }
  }

  public func explain() async throws {
    try await explain("simple")
  }

  public func explain(_ extended: Bool) async throws {
    if (extended) {
      try await explain("extended")
    } else {
      try await explain("simple")
    }
  }

  public func explain(_ mode: String) async throws {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(spark.client.getExplain(spark.sessionID, plan, mode))
      print(response.explain.explainString)
    }
  }

  /// Prints the schema to the console in a nice tree format.
  public func printSchema() async throws {
    try await printSchema(Int32.max)
  }

  /// Prints the schema up to the given level to the console in a nice tree format.
  /// - Parameter level: A level to be printed.
  public func printSchema(_ level: Int32) async throws {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(spark.client.getTreeString(spark.sessionID, plan, level))
      print(response.treeString.treeString)
    }
  }

  /// Returns a ``DataFrameWriter`` that can be used to write non-streaming data.
  public var write: DataFrameWriter {
    get {
      return DataFrameWriter(df: self)
    }
  }
}
