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

  private func withGPRC<Result: Sendable>(
    _ f: (GRPCClient<GRPCNIOTransportHTTP2.HTTP2ClientTransport.Posix>) async throws -> Result
  ) async throws -> Result {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      ),
      interceptors: spark.client.getIntercepters()
    ) { client in
      return try await f(client)
    }
  }

  private func analyzePlanIfNeeded() async throws {
    if self._schema != nil {
      return
    }
    try await withGPRC { client in
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
        result.append(Row(valueArray: values))
      }
    }

    return result
  }

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
  /// - Returns: ``[Row]``
  public func head(_ n: Int32 = 1) async throws -> [Row] {
    return try await limit(n).collect()
  }

  /// Returns the last `n` rows.
  /// - Parameter n: The number of rows.
  /// - Returns: ``[Row]``
  public func tail(_ n: Int32) async throws -> [Row] {
    let lastN = DataFrame(spark:spark, plan: SparkConnectClient.getTail(self.plan.root, n))
    return try await lastN.collect()
  }

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
      let response = try await service.analyzePlan(spark.client.getIsStreaming(spark.sessionID, plan))
      return response.isStreaming.isStreaming
    }
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
        return try await service
          .analyzePlan(spark.client.getStorageLevel(spark.sessionID, plan)).getStorageLevel.storageLevel.toStorageLevel
      }
    }
  }

  /// Prints the physical plan to the console for debugging purposes.
  public func explain() async throws {
    try await explain("simple")
  }

  /// Prints the plans (logical and physical) to the console for debugging purposes.
  /// - Parameter extended: If `false`, prints only the physical plan.
  public func explain(_ extended: Bool) async throws {
    if (extended) {
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
      let response = try await service.analyzePlan(spark.client.getExplain(spark.sessionID, plan, mode))
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
      let response = try await service.analyzePlan(spark.client.getInputFiles(spark.sessionID, plan))
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
      let response = try await service.analyzePlan(spark.client.getTreeString(spark.sessionID, plan, level))
      print(response.treeString.treeString)
    }
  }

  /// Returns a new `DataFrame` containing rows in this `DataFrame` but not in another `DataFrame`.
  /// This is equivalent to `EXCEPT DISTINCT` in SQL.
  /// - Parameter other: A `DataFrame` to exclude.
  /// - Returns: A `DataFrame`.
  public func except(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(self.plan.root, right, SetOpType.except)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing rows in this `DataFrame` but not in another `DataFrame` while
  /// preserving the duplicates. This is equivalent to `EXCEPT ALL` in SQL.
  /// - Parameter other: A `DataFrame` to exclude.
  /// - Returns: A `DataFrame`.
  public func exceptAll(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(self.plan.root, right, SetOpType.except, isAll: true)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing rows only in both this `DataFrame` and another `DataFrame`.
  /// This is equivalent to `INTERSECT` in SQL.
  /// - Parameter other: A `DataFrame` to intersect with.
  /// - Returns: A `DataFrame`.
  public func intersect(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(self.plan.root, right, SetOpType.intersect)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing rows only in both this `DataFrame` and another `DataFrame` while
  /// preserving the duplicates. This is equivalent to `INTERSECT ALL` in SQL.
  /// - Parameter other: A `DataFrame` to intersect with.
  /// - Returns: A `DataFrame`.
  public func intersectAll(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(self.plan.root, right, SetOpType.intersect, isAll: true)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing union of rows in this `DataFrame` and another `DataFrame`.
  /// This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union (that does
  /// deduplication of elements), use this function followed by a [[distinct]].
  /// Also as standard in SQL, this function resolves columns by position (not by name)
  /// - Parameter other: A `DataFrame` to union with.
  /// - Returns: A `DataFrame`.
  public func union(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(self.plan.root, right, SetOpType.union, isAll: true)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing union of rows in this `DataFrame` and another `DataFrame`.
  /// This is an alias of `union`.
  /// - Parameter other: A `DataFrame` to union with.
  /// - Returns: A `DataFrame`.
  public func unionAll(_ other: DataFrame) async -> DataFrame {
    return await union(other)
  }

  /// Returns a new `DataFrame` containing union of rows in this `DataFrame` and another `DataFrame`.
  /// The difference between this function and [[union]] is that this function resolves columns by
  /// name (not by position).
  /// When the parameter `allowMissingColumns` is `true`, the set of column names in this and other
  /// `DataFrame` can differ; missing columns will be filled with null. Further, the missing columns
  /// of this `DataFrame` will be added at the end in the schema of the union result
  /// - Parameter other: A `DataFrame` to union with.
  /// - Returns: A `DataFrame`.
  public func unionByName(_ other: DataFrame, _ allowMissingColumns: Bool = false) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(
      self.plan.root,
      right,
      SetOpType.union,
      isAll: true,
      byName: true,
      allowMissingColumns: allowMissingColumns
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a ``DataFrameWriter`` that can be used to write non-streaming data.
  public var write: DataFrameWriter {
    get {
      return DataFrameWriter(df: self)
    }
  }
}
