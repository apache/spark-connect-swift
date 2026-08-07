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
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
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
  var _schema: StructType? = nil
  var batches: [RecordBatch] = [RecordBatch]()

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
      self.plan = try sqlText.toSparkConnectPlan(posArgs)
    } else {
      self.plan = sqlText.toSparkConnectPlan
    }
  }

  init(spark: SparkSession, sqlText: String, _ args: [String: Sendable]) async throws {
    self.spark = spark
    self.plan = try sqlText.toSparkConnectPlan(args)
  }

  deinit {
    // `deinit` cannot be `async`, so send `RemoveCachedRemoteRelationCommand` best-effort
    // in a detached task like Scala and Python Spark Connect clients.
    if case .cachedRemoteRelation(let cachedRemoteRelation) = plan.root.relType {
      let client = spark.client
      Task {
        try? await client.removeCachedRemoteRelation(cachedRemoteRelation)
      }
    }
  }

  public func getPlan() -> Sendable {
    return self.plan
  }

  /// Set the schema. This is used to store the analized schema response from `Spark Connect` server.
  /// - Parameter schema: A ``DataType`` which is expected to be a struct.
  func setSchema(_ schema: DataType) throws {
    guard case .struct(let structType) = schema else {
      throw SparkConnectError.InvalidType
    }
    self._schema = structType
  }

  /// Add `Apache Arrow`'s `RecordBatch`s to the internal array.
  /// - Parameter batches: An array of ``RecordBatch``.
  func addBatches(_ batches: [RecordBatch]) {
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
    throw SparkConnectError.UnsupportedOperation
  }

  /// Return an array of column name strings
  public var columns: [String] {
    get async throws {
      try await analyzePlanIfNeeded()
      return self._schema!.fieldNames
    }
  }

  /// Return the schema of this ``DataFrame`` as a ``StructType``.
  public var schema: StructType {
    get async throws {
      try await analyzePlanIfNeeded()
      return self._schema!
    }
  }

  /// Returns all column names and their data types as an array.
  public var dtypes: [(String, String)] {
    get async throws {
      try await analyzePlanIfNeeded()
      return self._schema!.fields.map { ($0.name, $0.dataType.simpleString) }
    }
  }

  func withGPRC<Result: Sendable>(
    _ f: (GRPCClient<GRPCNIOTransportHTTP2.HTTP2ClientTransport.Posix>) async throws -> Result
  ) async throws -> Result {
    try await withRetry {
      try await withGRPCClient(
        transport: .http2NIOPosix(
          target: .dns(host: spark.client.host, port: spark.client.port),
          transportSecurity: spark.client.transportSecurity
        ),
        interceptors: spark.client.getIntercepters()
      ) { client in
        do {
          return try await f(client)
        } catch let error as RPCError where error.code == .internalError {
          throw await GrpcErrorConverter.convert(
            error, fetchingDetailsWith: client, sessionID: spark.client.sessionID,
            userContext: spark.client.userContext, clientType: spark.client.clientType) ?? error
        }
      }
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
      try self.setSchema(DataType(response.schema.schema))
    }
  }

}
