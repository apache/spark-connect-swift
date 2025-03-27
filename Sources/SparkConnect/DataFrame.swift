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
  var schema: DataType? = nil
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
    self.schema = schema
  }

  /// Add `Apache Arrow`'s `RecordBatch`s to the internal array.
  /// - Parameter batches: An array of ``RecordBatch``.
  private func addBatches(_ batches: [RecordBatch]) {
    self.batches.append(contentsOf: batches)
  }

  /// Return the `SparkSession` of this `DataFrame`.
  /// - Returns: A `SparkSession`
  public func sparkSession() -> SparkSession {
    return self.spark
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
  /// - Returns: a string array
  public func columns() async throws -> [String] {
    var columns: [String] = []
    try await analyzePlanIfNeeded()
    for field in self.schema!.struct.fields {
      columns.append(field.name)
    }
    return columns
  }

  /// Return a `JSON` string of data type because we cannot expose the internal type ``DataType``.
  /// - Returns: a `JSON` string.
  public func schema() async throws -> String {
    try await analyzePlanIfNeeded()
    return try self.schema!.jsonString()
  }

  var dtypes: [(String, String)] {
    get async throws {
      try await analyzePlanIfNeeded()
      return try self.schema!.struct.fields.map { ($0.name, try $0.dataType.simpleString) }
    }
  }

  private func analyzePlanIfNeeded() async throws {
    if self.schema != nil {
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
      try await service.executePlan(spark.client.getExecutePlanRequest(spark.sessionID, plan)) {
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
      try await service.executePlan(spark.client.getExecutePlanRequest(spark.sessionID, plan)) {
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

    if let schema = self.schema {
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
  var write: DataFrameWriter {
    get {
      return DataFrameWriter(df: self)
    }
  }
}
