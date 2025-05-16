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
import Synchronization

/// A distributed collection of data organized into named columns.
///
/// A DataFrame is equivalent to a relational table in Spark SQL, and can be created using various
/// functions in ``SparkSession``. Once created, it can be manipulated using the various domain-specific
/// language (DSL) functions defined in: ``DataFrame``, ``Column``, and functions.
///
/// ## Creating DataFrames
///
/// DataFrames can be created from various sources:
///
/// ```swift
/// // From a range
/// let df1 = try await spark.range(1, 100)
///
/// // From a SQL query
/// let df2 = try await spark.sql("SELECT * FROM users")
///
/// // From files
/// let df3 = try await spark.read.csv("data.csv")
/// ```
///
/// ## Common Operations
///
/// ### Transformations
///
/// ```swift
/// // Select specific columns
/// let names = try await df.select("name", "age")
///
/// // Filter rows
/// let adults = try await df.filter("age >= 18")
///
/// // Group and aggregate
/// let stats = try await df.groupBy("department").agg("avg(salary)", "count(*)")
/// ```
///
/// ### Actions
///
/// ```swift
/// // Show the first 20 rows
/// try await df.show()
///
/// // Collect all data to the driver
/// let rows = try await df.collect()
///
/// // Count rows
/// let count = try await df.count()
/// ```
///
/// ## Topics
///
/// ### Basic Information
/// - ``columns``
/// - ``schema``
/// - ``dtypes``
/// - ``sparkSession``
///
/// ### Data Collection
/// - ``count()``
/// - ``collect()``
/// - ``first()``
/// - ``head()``
/// - ``head(_:)``
/// - ``take(_:)``
/// - ``tail(_:)``
/// - ``show()``
/// - ``show(_:)``
/// - ``show(_:_:)``
/// - ``show(_:_:_:)``
///
/// ### Transformation Operations
/// - ``to(_:)``
/// - ``toDF(_:)``
/// - ``toJSON()``
/// - ``select(_:)``
/// - ``selectExpr(_:)``
/// - ``filter(_:)``
/// - ``where(_:)``
/// - ``sort(_:)``
/// - ``orderBy(_:)``
/// - ``limit(_:)``
/// - ``offset(_:)``
/// - ``drop(_:)``
/// - ``dropDuplicates(_:)``
/// - ``dropDuplicatesWithinWatermark(_:)``
/// - ``distinct()``
/// - ``withColumnRenamed(_:_:)``
/// - ``unpivot(_:_:_:)``
/// - ``unpivot(_:_:_:_:)``
/// - ``melt(_:_:_:)``
/// - ``melt(_:_:_:_:)``
/// - ``transpose()``
/// - ``transpose(_:)``
/// - ``hint(_:_:)``
/// - ``withWatermark(_:_:)``
///
/// ### Join Operations
/// - ``join(_:)``
/// - ``join(_:_:_:)``
/// - ``join(_:joinExprs:)``
/// - ``join(_:joinExprs:joinType:)``
/// - ``crossJoin(_:)``
/// - ``lateralJoin(_:)``
/// - ``lateralJoin(_:joinType:)``
/// - ``lateralJoin(_:joinExprs:)``
/// - ``lateralJoin(_:joinExprs:joinType:)``
///
/// ### Set Operations
/// - ``union(_:)``
/// - ``unionAll(_:)``
/// - ``unionByName(_:_:)``
/// - ``intersect(_:)``
/// - ``intersectAll(_:)``
/// - ``except(_:)``
/// - ``exceptAll(_:)``
///
/// ### Partitioning
/// - ``repartition(_:)``
/// - ``repartition(_:_:)``
/// - ``repartitionByExpression(_:_:)``
/// - ``coalesce(_:)``
///
/// ### Grouping Operations
/// - ``groupBy(_:)``
/// - ``rollup(_:)``
/// - ``cube(_:)``
///
/// ### Persistence
/// - ``cache()``
/// - ``checkpoint(_:_:_:)``
/// - ``localCheckpoint(_:_:)``
/// - ``persist(storageLevel:)``
/// - ``unpersist(blocking:)``
/// - ``storageLevel``
///
/// ### Schema Information
/// - ``printSchema()``
/// - ``printSchema(_:)``
/// - ``explain()``
/// - ``explain(_:)``
///
/// ### View Creation
/// - ``createTempView(_:)``
/// - ``createOrReplaceTempView(_:)``
/// - ``createGlobalTempView(_:)``
/// - ``createOrReplaceGlobalTempView(_:)``
///
/// ### Write Operations
/// - ``write``
/// - ``writeTo(_:)``
/// - ``writeStream``
///
/// ### Sampling
/// - ``sample(_:_:_:)``
/// - ``sample(_:_:)``
/// - ``sample(_:)``
///
/// ### Statistics
/// - ``describe(_:)``
/// - ``summary(_:)``
///
/// ### Utility Methods
/// - ``isEmpty()``
/// - ``isLocal()``
/// - ``isStreaming()``
/// - ``inputFiles()``
/// - ``semanticHash()``
/// - ``sameSemantics(other:)``
///
/// ### Internal Methods
/// - ``rdd()``
/// - ``getPlan()``
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
  ///   - posArgs: An array of strings.
  init(spark: SparkSession, sqlText: String, _ posArgs: [Sendable]? = nil) async throws {
    self.spark = spark
    if let posArgs {
      self.plan = sqlText.toSparkConnectPlan(posArgs)
    } else {
      self.plan = sqlText.toSparkConnectPlan
    }
  }

  init(spark: SparkSession, sqlText: String, _ args: [String: Sendable]) async throws {
    self.spark = spark
    self.plan = sqlText.toSparkConnectPlan(args)
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
        transportSecurity: spark.client.transportSecurity
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
            case .primitiveInfo(.date32):
              values.append(array.asAny(i) as! Date)
            case ArrowType.ArrowBinary:
              values.append((array as! AsString).asString(i).utf8)
            case .complexInfo(.strct):
              values.append((array as! AsString).asString(i))
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

  /// Selects a subset of existing columns using column names.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func select(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getProject(self.plan.root, cols))
  }

  /// Selects a subset of existing columns using column names.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func toDF(_ cols: String...) -> DataFrame {
    let df = if cols.isEmpty {
      DataFrame(spark: self.spark, plan: self.plan)
    } else {
      DataFrame(spark: self.spark, plan: SparkConnectClient.getProject(self.plan.root, cols))
    }
    return df
  }

  /// Returns a new DataFrame where each row is reconciled to match the specified schema.
  /// - Parameter schema: The given schema.
  /// - Returns: A ``DataFrame`` with the given schema.
  public func to(_ schema: String) async throws -> DataFrame {
    // Validate by parsing.
    do {
      let dataType = try await sparkSession.client.ddlParse(schema)
      return DataFrame(spark: self.spark, plan: SparkConnectClient.getToSchema(self.plan.root, dataType))
    } catch {
      throw SparkConnectError.InvalidTypeException
    }
  }

  /// Returns the content of the Dataset as a Dataset of JSON strings.
  /// - Returns: A ``DataFrame`` with a single string column whose content is JSON.
  public func toJSON() -> DataFrame {
    return selectExpr("to_json(struct(*))")
  }

  /// Projects a set of expressions and returns a new ``DataFrame``.
  /// - Parameter exprs: Expression strings
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func selectExpr(_ exprs: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getProjectExprs(self.plan.root, exprs))
  }

  /// Returns a new Dataset with a column dropped. This is a no-op if schema doesn't contain column name.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func drop(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getDrop(self.plan.root, cols))
  }

  /// Returns a new ``DataFrame`` that contains only the unique rows from this ``DataFrame``.
  /// This is an alias for `distinct`. If column names are given, Spark considers only those columns.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame``.
  public func dropDuplicates(_ cols: String...) -> DataFrame {
    let plan = SparkConnectClient.getDropDuplicates(self.plan.root, cols, withinWatermark: false)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new Dataset with duplicates rows removed, within watermark.
  /// If column names are given, Spark considers only those columns.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame``.
  public func dropDuplicatesWithinWatermark(_ cols: String...) -> DataFrame {
    let plan = SparkConnectClient.getDropDuplicates(self.plan.root, cols, withinWatermark: true)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Computes basic statistics for numeric and string columns, including count, mean, stddev, min,
  /// and max. If no columns are given, this function computes statistics for all numerical or
  /// string columns.
  /// - Parameter cols: Column names.
  /// - Returns: A ``DataFrame`` containing basic statistics.
  public func describe(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getDescribe(self.plan.root, cols))
  }

  /// Computes specified statistics for numeric and string columns. Available statistics are:
  /// count, mean, stddev, min, max, arbitrary approximate percentiles specified as a percentage (e.g. 75%)
  /// count_distinct, approx_count_distinct . If no statistics are given, this function computes count, mean,
  /// stddev, min, approximate quartiles (percentiles at 25%, 50%, and 75%), and max.
  /// - Parameter statistics: Statistics names.
  /// - Returns: A ``DataFrame`` containing specified statistics.
  public func summary(_ statistics: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getSummary(self.plan.root, statistics))
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

  /// Filters rows using the given condition.
  ///
  /// The condition should be a SQL expression that evaluates to a boolean value.
  ///
  /// ```swift
  /// // Filter with simple condition
  /// let adults = df.filter("age >= 18")
  ///
  /// // Filter with complex condition
  /// let qualifiedUsers = df.filter("age >= 21 AND department = 'Engineering'")
  ///
  /// // Filter with SQL functions
  /// let recent = df.filter("date_diff(current_date(), join_date) < 30")
  /// ```
  ///
  /// - Parameter conditionExpr: A SQL expression string for filtering
  /// - Returns: A new DataFrame containing only rows that match the condition
  public func filter(_ conditionExpr: String) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getFilter(self.plan.root, conditionExpr))
  }

  /// Filters rows using the given condition (alias for filter).
  ///
  /// This method is an alias for ``filter(_:)`` and behaves identically.
  ///
  /// ```swift
  /// let highSalary = df.where("salary > 100000")
  /// ```
  ///
  /// - Parameter conditionExpr: A SQL expression string for filtering
  /// - Returns: A new DataFrame containing only rows that match the condition
  public func `where`(_ conditionExpr: String) -> DataFrame {
    return filter(conditionExpr)
  }

  /// Returns a new DataFrame sorted by the specified columns.
  ///
  /// By default, sorts in ascending order. Use `desc("column")` for descending order.
  ///
  /// ```swift
  /// // Sort by single column (ascending)
  /// let sorted = df.sort("age")
  ///
  /// // Sort by multiple columns
  /// let multiSort = df.sort("department", "salary")
  ///
  /// // Sort with mixed order
  /// let mixedSort = df.sort("department", "desc(salary)")
  /// ```
  ///
  /// - Parameter cols: Column names or expressions to sort by
  /// - Returns: A new DataFrame sorted by the specified columns
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
  ///
  /// This transformation is often used for:
  /// - Previewing data
  /// - Reducing data size for testing
  /// - Implementing pagination
  ///
  /// ```swift
  /// // Get top 10 records
  /// let top10 = df.limit(10)
  ///
  /// // Preview data
  /// let preview = df.filter("status = 'active'").limit(100)
  /// ```
  ///
  /// - Parameter n: Maximum number of rows to return
  /// - Returns: A new DataFrame with at most n rows
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
        return try await service
          .analyzePlan(spark.client.getStorageLevel(spark.sessionID, plan)).getStorageLevel.storageLevel.toStorageLevel
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

  /// Join with another DataFrame.
  ///
  /// This performs an inner join and requires a subsequent join predicate.
  /// For other join types, use the overloaded methods with join type parameter.
  ///
  /// ```swift
  /// // Basic join (requires join condition)
  /// let joined = df1.join(df2)
  ///     .where("df1.id = df2.user_id")
  ///
  /// // Join with condition
  /// let result = users.join(orders, "id")
  /// ```
  ///
  /// - Parameter right: The DataFrame to join with
  /// - Returns: A new DataFrame representing the join result
  public func join(_ right: DataFrame) async -> DataFrame {
    let right = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getJoin(self.plan.root, right, JoinType.inner)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Equi-join with another DataFrame using the given column.
  ///
  /// This method performs an equi-join on a single column that exists in both DataFrames.
  ///
  /// ```swift
  /// // Inner join on a single column
  /// let joined = users.join(orders, "user_id")
  ///
  /// // Left outer join
  /// let leftJoined = users.join(orders, "user_id", "left")
  ///
  /// // Other join types: "inner", "outer", "left", "right", "semi", "anti"
  /// ```
  ///
  /// - Parameters:
  ///   - right: The DataFrame to join with
  ///   - usingColumn: Column name that exists in both DataFrames
  ///   - joinType: Type of join (default: "inner")
  /// - Returns: A new DataFrame with the join result
  public func join(_ right: DataFrame, _ usingColumn: String, _ joinType: String = "inner") async -> DataFrame {
    await join(right, [usingColumn], joinType)
  }

  /// Inner equi-join with another `DataFrame` using the given columns.
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - usingColumn: Names of the columns to join on. These columns must exist on both sides.
  ///   - joinType: A join type name.
  /// - Returns: A `DataFrame`.
  public func join(_ other: DataFrame, _ usingColumns: [String], _ joinType: String = "inner") async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getJoin(
      self.plan.root,
      right,
      joinType.toJoinType,
      usingColumns: usingColumns
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Inner equi-join with another `DataFrame` using the given columns.
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinExprs:A join expression string.
  /// - Returns: A `DataFrame`.
  public func join(_ right: DataFrame, joinExprs: String) async -> DataFrame {
    return await join(right, joinExprs: joinExprs, joinType: "inner")
  }

  /// Inner equi-join with another `DataFrame` using the given columns.
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinExprs:A join expression string.
  ///   - joinType: A join type name.
  /// - Returns: A `DataFrame`.
  public func join(_ right: DataFrame, joinExprs: String, joinType: String) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getJoin(
      self.plan.root,
      rightPlan,
      joinType.toJoinType,
      joinCondition: joinExprs
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Explicit cartesian join with another `DataFrame`.
  /// - Parameter right: Right side of the join operation.
  /// - Returns: A `DataFrame`.
  public func crossJoin(_ right: DataFrame) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getJoin(self.plan.root, rightPlan, JoinType.cross)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Lateral join with another ``DataFrame``.
  ///
  /// Behaves as an JOIN LATERAL.
  ///
  /// - Parameters:
  ///   - right: Right side of the join operation.
  /// - Returns: A ``DataFrame``.
  public func lateralJoin(_ right: DataFrame) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getLateralJoin(
      self.plan.root,
      rightPlan,
      JoinType.inner
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Lateral join with another ``DataFrame``.
  ///
  /// Behaves as an JOIN LATERAL.
  ///
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinType: One of `inner` (default), `cross`, `left`, `leftouter`, `left_outer`.
  /// - Returns: A ``DataFrame``.
  public func lateralJoin(_ right: DataFrame, joinType: String) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getLateralJoin(
      self.plan.root,
      rightPlan,
      joinType.toJoinType
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Lateral join with another ``DataFrame``.
  ///
  /// Behaves as an JOIN LATERAL.
  ///
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinExprs: A join expression string.
  /// - Returns: A ``DataFrame``.
  public func lateralJoin(_ right: DataFrame, joinExprs: String) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getLateralJoin(
      self.plan.root,
      rightPlan,
      JoinType.inner,
      joinCondition: joinExprs
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Lateral join with another ``DataFrame``.
  ///
  /// Behaves as an JOIN LATERAL.
  ///
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinType: One of `inner` (default), `cross`, `left`, `leftouter`, `left_outer`.
  ///   - joinExprs: A join expression string.
  /// - Returns: A ``DataFrame``.
  public func lateralJoin(
    _ right: DataFrame, joinExprs: String, joinType: String = "inner"
  ) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getLateralJoin(
      self.plan.root,
      rightPlan,
      joinType.toJoinType,
      joinCondition: joinExprs
    )
    return DataFrame(spark: self.spark, plan: plan)
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

  private func buildRepartition(numPartitions: Int32, shuffle: Bool) -> DataFrame {
    let plan = SparkConnectClient.getRepartition(self.plan.root, numPartitions, shuffle)
    return DataFrame(spark: self.spark, plan: plan)
  }

  private func buildRepartitionByExpression(numPartitions: Int32?, partitionExprs: [String]) -> DataFrame {
    let plan = SparkConnectClient.getRepartitionByExpression(self.plan.root, partitionExprs, numPartitions)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new ``DataFrame`` that has exactly `numPartitions` partitions.
  /// - Parameter numPartitions: The number of partitions.
  /// - Returns: A `DataFrame`.
  public func repartition(_ numPartitions: Int32) -> DataFrame {
    return buildRepartition(numPartitions: numPartitions, shuffle: true)
  }

  /// Returns a new ``DataFrame`` partitioned by the given partitioning expressions, using
  /// `spark.sql.shuffle.partitions` as number of partitions. The resulting Dataset is hash
  /// partitioned.
  /// - Parameter partitionExprs: The partition expression strings.
  /// - Returns: A `DataFrame`.
  public func repartition(_ partitionExprs: String...) -> DataFrame {
    return buildRepartitionByExpression(numPartitions: nil, partitionExprs: partitionExprs)
  }

  /// Returns a new ``DataFrame`` partitioned by the given partitioning expressions, using
  /// `spark.sql.shuffle.partitions` as number of partitions. The resulting Dataset is hash
  /// partitioned.
  /// - Parameters:
  ///   - numPartitions: The number of partitions.
  ///   - partitionExprs: The partition expression strings.
  /// - Returns: A `DataFrame`.
  public func repartition(_ numPartitions: Int32, _ partitionExprs: String...) -> DataFrame {
    return buildRepartitionByExpression(numPartitions: numPartitions, partitionExprs: partitionExprs)
  }

  /// Returns a new ``DataFrame`` partitioned by the given partitioning expressions, using
  /// `spark.sql.shuffle.partitions` as number of partitions. The resulting Dataset is hash
  /// partitioned.
  /// - Parameter partitionExprs: The partition expression strings.
  /// - Returns: A `DataFrame`.
  public func repartitionByExpression(_ numPartitions: Int32?, _ partitionExprs: String...) -> DataFrame {
    return buildRepartitionByExpression(numPartitions: numPartitions, partitionExprs: partitionExprs)
  }

  /// Returns a new ``DataFrame`` that has exactly `numPartitions` partitions, when the fewer partitions
  /// are requested. If a larger number of partitions is requested, it will stay at the current
  /// number of partitions. Similar to coalesce defined on an `RDD`, this operation results in a
  /// narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a
  /// shuffle, instead each of the 100 new partitions will claim 10 of the current partitions.
  /// - Parameter numPartitions: The number of partitions.
  /// - Returns: A `DataFrame`.
  public func coalesce(_ numPartitions: Int32) -> DataFrame {
    return buildRepartition(numPartitions: numPartitions, shuffle: false)
  }

  /// Returns a new ``Dataset`` that contains only the unique rows from this ``Dataset``.
  /// This is an alias for `dropDuplicates`.
  /// - Returns: A `DataFrame`.
  public func distinct() -> DataFrame {
    return dropDuplicates()
  }

  /// Transposes a DataFrame, switching rows to columns. This function transforms the DataFrame
  /// such that the values in the first column become the new columns of the DataFrame.
  /// - Returns: A transposed ``DataFrame``.
  public func transpose() -> DataFrame {
    return buildTranspose([])
  }

  /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
  /// set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
  /// which cannot be reversed. This is an alias for `unpivot`.
  /// - Parameters:
  ///   - ids: ID column names
  ///   - values: Value column names to unpivot
  ///   - variableColumnName: Name of the variable column
  ///   - valueColumnName: Name of the value column
  /// - Returns: A ``DataFrame``.
  public func melt(
    _ ids: [String],
    _ values: [String],
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    return unpivot(ids, values, variableColumnName, valueColumnName)
  }

  /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
  /// set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
  /// which cannot be reversed. This is an alias for `unpivot`.
  /// - Parameters:
  ///   - ids: ID column names
  ///   - variableColumnName: Name of the variable column
  ///   - valueColumnName: Name of the value column
  /// - Returns: A ``DataFrame``.
  public func melt(
    _ ids: [String],
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    return unpivot(ids, variableColumnName, valueColumnName)
  }

  /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
  /// set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
  /// which cannot be reversed.
  /// - Parameters:
  ///   - ids: ID column names
  ///   - values: Value column names to unpivot
  ///   - variableColumnName: Name of the variable column
  ///   - valueColumnName: Name of the value column
  /// - Returns: A ``DataFrame``.
  public func unpivot(
    _ ids: [String],
    _ values: [String],
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    return buildUnpivot(ids, values, variableColumnName, valueColumnName)
  }

  /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
  /// set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
  /// which cannot be reversed.
  /// - Parameters:
  ///   - ids: ID column names
  ///   - variableColumnName: Name of the variable column
  ///   - valueColumnName: Name of the value column
  /// - Returns: A ``DataFrame``.
  public func unpivot(
    _ ids: [String],
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    return buildUnpivot(ids, nil, variableColumnName, valueColumnName)
  }

  func buildUnpivot(
    _ ids: [String],
    _ values: [String]?,
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    let plan = SparkConnectClient.getUnpivot(self.plan.root, ids, values, variableColumnName, valueColumnName)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Transposes a ``DataFrame`` such that the values in the specified index column become the new
  /// columns of the ``DataFrame``.
  /// - Parameter indexColumn: The single column that will be treated as the index for the transpose operation.
  /// This column will be used to pivot the data, transforming the DataFrame such that the values of
  /// the indexColumn become the new columns in the transposed DataFrame.
  /// - Returns: A transposed ``DataFrame``.
  public func transpose(_ indexColumn: String) -> DataFrame {
    return buildTranspose([indexColumn])
  }

  func buildTranspose(_ indexColumn: [String]) -> DataFrame {
    let plan = SparkConnectClient.getTranspose(self.plan.root, indexColumn)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Groups the DataFrame using the specified columns.
  ///
  /// This method is used to perform aggregations on groups of data.
  /// After grouping, you can apply aggregation functions like count(), sum(), avg(), etc.
  ///
  /// ```swift
  /// // Group by single column
  /// let byDept = df.groupBy("department")
  ///     .agg(count("*").alias("employee_count"))
  ///
  /// // Group by multiple columns
  /// let byDeptAndLocation = df.groupBy("department", "location")
  ///     .agg(
  ///         avg("salary").alias("avg_salary"),
  ///         max("salary").alias("max_salary")
  ///     )
  /// ```
  ///
  /// - Parameter cols: Column names to group by
  /// - Returns: A ``GroupedData`` object for aggregation operations
  public func groupBy(_ cols: String...) -> GroupedData {
    return GroupedData(self, GroupType.groupby, cols)
  }

  /// Create a multi-dimensional rollup for the current ``DataFrame`` using the specified columns, so we
  /// can run aggregation on them.
  /// - Parameter cols: Grouping column names.
  /// - Returns: A ``GroupedData``.
  public func rollup(_ cols: String...) -> GroupedData {
    return GroupedData(self, GroupType.rollup, cols)
  }

  /// Create a multi-dimensional cube for the current ``DataFrame`` using the specified columns, so we
  /// can run aggregation on them.
  /// - Parameter cols: Grouping column names.
  /// - Returns: A ``GroupedData``.
  public func cube(_ cols: String...) -> GroupedData {
    return GroupedData(self, GroupType.cube, cols)
  }

  /// Specifies some hint on the current Dataset.
  /// - Parameters:
  ///   - name: The hint name.
  ///   - parameters: The parameters of the hint
  /// - Returns: A ``DataFrame``.
  @discardableResult
  public func hint(_ name: String, _ parameters: Sendable...) -> DataFrame {
    let plan = SparkConnectClient.getHint(self.plan.root, name, parameters)
    return DataFrame(spark: self.spark, plan: plan)
  }

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
    try await spark.client.createTempView(self.plan.root, viewName, replace: replace, isGlobal: global)
  }

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
    let plan = try await spark.client.getCheckpoint(self.plan.root, eager, reliableCheckpoint, storageLevel)
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

  /// Defines an event time watermark for this ``DataFrame``.  A watermark tracks a point in time
  /// before which we assume no more late data is going to arrive.
  /// - Parameters:
  ///   - eventTime: the name of the column that contains the event time of the row.
  ///   - delayThreshold: the minimum delay to wait to data to arrive late, relative to
  ///   the latest record that has been processed in the form of an interval (e.g. "1 minute" or "5 hours").
  ///   NOTE: This should not be negative.
  /// - Returns: A ``DataFrame`` instance.
  public func withWatermark(_ eventTime: String, _ delayThreshold: String) -> DataFrame {
    let plan = SparkConnectClient.getWithWatermark(self.plan.root, eventTime, delayThreshold)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a ``DataFrameWriter`` that can be used to write non-streaming data.
  public var write: DataFrameWriter {
    get {
      DataFrameWriter(df: self)
    }
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
    get {
      DataStreamWriter(df: self)
    }
  }
}
