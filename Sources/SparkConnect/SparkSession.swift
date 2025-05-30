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

/// The entry point to programming Spark with ``DataFrame`` API.
///
/// Use the builder to get a session.
///
/// ```swift
/// let spark = try await SparkSession.builder.getOrCreate()
/// ```
public actor SparkSession {

  public static let builder: Builder = Builder()

  let client: SparkConnectClient

  /// Runtime configuration interface for Spark.
  public let conf: RuntimeConf

  let regexSessionID = /;session_id=([a-zA-Z0-9-]+)/

  /// Create a session that uses the specified connection string and userID.
  /// - Parameters:
  ///   - connection: a string in a patter, `sc://{host}:{port}`
  init(_ connection: String) {
    self.client = SparkConnectClient(remote: connection)
    // Since `Session ID` belongs to `SparkSession`, we handle this here.
    if connection.contains(regexSessionID) {
      self.sessionID = connection.firstMatch(of: regexSessionID)!.1.uppercased()
    } else {
      self.sessionID = UUID().uuidString
    }
    self.conf = RuntimeConf(self.client)
  }

  /// The Spark version of Spark Connect Servier. This is supposed to be overwritten during establishing connections.
  public var version: String = ""

  /// Start a new session with isolated SQL configurations, temporary tables, registered functions
  /// are isolated, but sharing the underlying `SparkContext` and cached data.
  /// - Returns: a new ``SparkSession`` instance.
  public func newSession() async throws -> SparkSession {
    return try await SparkSession.builder.create()
  }

  func setVersion(_ version: String) {
    self.version = version
  }

  /// A unique session ID for this session from client.
  nonisolated let sessionID: String

  /// Get the current session ID
  /// - Returns: the current session ID
  func getSessionID() -> String {
    sessionID
  }

  /// A server-side generated session ID. This is supposed to be overwritten during establishing connections.
  var serverSideSessionID: String = ""

  /// A variable for ``SparkContext``. This is designed to throw exceptions by Apache Spark.
  public var sparkContext: SparkContext {
    get throws {
      // SQLSTATE: 0A000
      // [UNSUPPORTED_CONNECT_FEATURE.SESSION_SPARK_CONTEXT]
      // Feature is not supported in Spark Connect: Access to the SparkContext.
      throw SparkConnectError.UnsupportedOperation
    }
  }

  /// Interface through which the user may create, drop, alter or query underlying databases, tables, functions etc.
  public var catalog: Catalog {
    get {
      return Catalog(spark: self)
    }
  }

  /// Stop the current client.
  public func stop() async {
    await client.stop()
  }
  
  /// Returns a ``DataFrame`` with no rows or columns.
  public var emptyDataFrame: DataFrame {
    get async {
      return await DataFrame(spark: self, plan: client.getLocalRelation())
    }
  }

  /// Create a ``DataFrame`` with a single `Int64` column name `id`, containing elements in a
  /// range from 0 to `end` (exclusive) with step value 1.
  ///
  /// - Parameter end: A value for the end of range.
  /// - Returns: A ``DataFrame`` instance.
  public func range(_ end: Int64) async throws -> DataFrame {
    return try await range(0, end)
  }

  /// Create a ``DataFrame`` with a single `Int64` column named `id`, containing elements in a
  /// range from `start` to `end` (exclusive) with a step value (default: 1).
  ///
  /// - Parameters:
  ///   - start: A value for the start of range.
  ///   - end: A value for the end of range.
  ///   - step: A value for the step.
  /// - Returns: A ``DataFrame`` instance.
  public func range(_ start: Int64, _ end: Int64, _ step: Int64 = 1) async throws -> DataFrame {
    return await DataFrame(spark: self, plan: client.getPlanRange(start, end, step))
  }

  /// Executes a SQL query and returns the result as a DataFrame.
  ///
  /// This method allows you to run SQL queries against tables and views registered in the Spark catalog.
  ///
  /// ```swift
  /// // Simple query
  /// let users = try await spark.sql("SELECT * FROM users")
  ///
  /// // Query with filtering and aggregation
  /// let stats = try await spark.sql("""
  ///     SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
  ///     FROM employees
  ///     GROUP BY department
  ///     ORDER BY avg_salary DESC
  /// """)
  /// ```
  ///
  /// - Parameter sqlText: A SQL query string
  /// - Returns: A DataFrame containing the query results
  /// - Throws: ``SparkConnectError`` if the query execution fails
  public func sql(_ sqlText: String) async throws -> DataFrame {
    return try await DataFrame(spark: self, sqlText: sqlText)
  }

  /// Executes a SQL query with positional parameters.
  ///
  /// This method allows you to execute parameterized SQL queries using positional placeholders (`?`).
  /// Parameters are automatically converted to SQL literal expressions.
  ///
  /// ```swift
  /// // Query with positional parameters
  /// let result = try await spark.sql(
  ///     "SELECT * FROM users WHERE age > ? AND department = ?",
  ///     21,
  ///     "Engineering"
  /// )
  /// ```
  ///
  /// - Parameters:
  ///   - sqlText: A SQL query with positional parameter placeholders (`?`)
  ///   - args: Parameter values to substitute for the placeholders
  /// - Returns: A DataFrame containing the query results
  /// - Throws: ``SparkConnectError`` if the query execution fails or if parameter conversion fails
  public func sql(_ sqlText: String, _ args: Sendable...) async throws -> DataFrame {
    return try await DataFrame(spark: self, sqlText: sqlText, args)
  }

  /// Executes a SQL query with named parameters.
  ///
  /// This method allows you to execute parameterized SQL queries using named placeholders.
  /// Named parameters provide better readability and maintainability for complex queries.
  ///
  /// ```swift
  /// // Query with named parameters
  /// let result = try await spark.sql(
  ///     "SELECT * FROM users WHERE age > :minAge AND department = :dept",
  ///     args: [
  ///         "minAge": 21,
  ///         "dept": "Engineering"
  ///     ]
  /// )
  /// ```
  ///
  /// - Parameters:
  ///   - sqlText: A SQL query with named parameter placeholders (`:paramName`)
  ///   - args: A dictionary mapping parameter names to values
  /// - Returns: A DataFrame containing the query results
  /// - Throws: ``SparkConnectError`` if the query execution fails or if parameter conversion fails
  public func sql(_ sqlText: String, args: [String: Sendable]) async throws -> DataFrame {
    return try await DataFrame(spark: self, sqlText: sqlText, args)
  }

  /// Returns a ``DataFrameReader`` for reading data in various formats.
  ///
  /// The ``DataFrameReader`` provides methods to load data from external storage systems
  /// such as file systems and databases.
  ///
  /// ```swift
  /// // Read a CSV file
  /// let csvData = spark.read
  ///     .option("header", "true")
  ///     .option("inferSchema", "true")
  ///     .csv("path/to/file.csv")
  ///
  /// // Read a JSON file
  /// let jsonData = spark.read
  ///     .json("path/to/file.json")
  ///
  /// // Read an ORC file
  /// let orcData = spark.read
  ///     .orc("path/to/file.orc")
  /// ```
  ///
  /// - Returns: A ``DataFrameReader`` instance configured for this session
  public var read: DataFrameReader {
    get {
      DataFrameReader(sparkSession: self)
    }
  }

  /// Returns a ``DataStreamReader`` that can be used to read streaming data in as a ``DataFrame``.
  ///
  /// The ``DataFrameReader`` provides methods to load data from external storage systems
  /// such as file systems, databases, and streaming sources.
  ///
  /// ```swift
  /// // Read an ORC file
  /// let orcData = spark.readStream.orc("path/to/file.orc")
  /// ```
  ///
  /// - Returns: A ``DataFrameReader`` instance configured for this session
  public var readStream: DataStreamReader {
    get {
      DataStreamReader(sparkSession: self)
    }
  }

  /// Returns a ``DataFrame`` representing the specified table or view.
  ///
  /// This method retrieves a table or view from the Spark catalog and returns it as a ``DataFrame``.
  /// The table name can be qualified with a database name (e.g., "database.table") or unqualified.
  ///
  /// ```swift
  /// // Load a table from the default database
  /// let users = try await spark.table("users")
  ///
  /// // Load a table from a specific database
  /// let sales = try await spark.table("analytics.sales_data")
  ///
  /// // Load a temporary view
  /// let tempView = try await spark.table("temp_user_stats")
  /// ```
  ///
  /// - Parameter tableName: The name of the table or view to load
  /// - Returns: A ``DataFrame`` representing the table data
  /// - Throws: ``SparkConnectError`` if the table doesn't exist or cannot be accessed
  public func table(_ tableName: String) async throws -> DataFrame {
    return await read.table(tableName)
  }

  public func executeCommand(_ runner: String, _ command: String, _ options: [String: String])
    async throws -> DataFrame
  {
    let plan = try await self.client.getExecuteExternalCommand(runner, command, options)
    return DataFrame(spark: self, plan: plan)
  }

  /// Executes a code block and prints the execution time.
  ///
  /// This utility method is useful for performance testing and optimization.
  /// It measures the time taken to execute the provided async closure and prints it to stdout.
  ///
  /// ```swift
  /// // Measure query execution time
  /// let result = try await spark.time {
  ///     try await spark.sql("SELECT COUNT(*) FROM large_table").collect()
  /// }
  /// // Prints: Time taken: 1234 ms
  /// ```
  ///
  /// - Parameter f: An async closure to execute and measure
  /// - Returns: The result of the executed closure
  /// - Throws: Any error thrown by the closure
  public func time<T: Sendable>(_ f: () async throws -> T) async throws -> T {
    let start = DispatchTime.now()
    let ret = try await f()
    let end = DispatchTime.now()
    let elapsed = (end.uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000
    print("Time taken: \(elapsed) ms")
    return ret
  }

  /// Adds a tag to all operations started by this thread in this session.
  ///
  /// Tags are useful for tracking and monitoring Spark operations. They can help identify
  /// specific workloads in the Spark UI and logs.
  ///
  /// ```swift
  /// // Add a tag for a specific operation
  /// try await spark.addTag("etl_job_2024")
  /// 
  /// // Perform operations that will be tagged
  /// let df = try await spark.sql("SELECT * FROM source_table")
  /// try await df.write.saveAsTable("processed_table")
  /// 
  /// // Remove the tag when done
  /// try await spark.removeTag("etl_job_2024")
  /// ```
  ///
  /// - Parameter tag: The tag to add. Cannot contain commas or be empty
  /// - Throws: ``SparkConnectError`` if the tag is invalid
  public func addTag(_ tag: String) async throws {
    try await client.addTag(tag: tag)
  }

  /// Removes a previously added tag from operations in this session.
  ///
  /// If the specified tag was not previously added, this method does nothing.
  ///
  /// ```swift
  /// // Remove a specific tag
  /// try await spark.removeTag("etl_job_2024")
  /// ```
  ///
  /// - Parameter tag: The tag to remove. Cannot contain commas or be empty
  /// - Throws: ``SparkConnectError`` if the tag is invalid
  public func removeTag(_ tag: String) async throws {
    try await client.removeTag(tag: tag)
  }

  /// Returns all operation tags currently set for this thread in this session.
  ///
  /// ```swift
  /// // Get all current tags
  /// let currentTags = await spark.getTags()
  /// print("Active tags: \(currentTags)")
  /// ```
  ///
  /// - Returns: A set of currently active tags
  public func getTags() async -> Set<String> {
    return await client.getTags()
  }

  /// Removes all operation tags for this thread in this session.
  ///
  /// ```swift
  /// // Clear all tags
  /// await spark.clearTags()
  /// ```
  public func clearTags() async {
    await client.clearTags()
  }

  /// Request to interrupt all currently running operations of this session.
  /// - Returns: Sequence of operation IDs requested to be interrupted.
  @discardableResult
  public func interruptAll() async throws -> [String] {
    return try await client.interruptAll()
  }

  /// Request to interrupt all currently running operations of this session with the given job tag.
  /// - Returns: Sequence of operation IDs requested to be interrupted.
  @discardableResult
  public func interruptTag(_ tag: String) async throws -> [String] {
    return try await client.interruptTag(tag)
  }

  /// Request to interrupt an operation of this session, given its operation ID.
  /// - Returns: Sequence of operation IDs requested to be interrupted.
  @discardableResult
  public func interruptOperation(_ operationId: String) async throws -> [String] {
    return try await client.interruptOperation(operationId)
  }

  func sameSemantics(_ plan: Plan, _ otherPlan: Plan) async throws -> Bool {
    return try await client.sameSemantics(plan, otherPlan)
  }

  func semanticHash(_ plan: Plan) async throws -> Int32 {
    return try await client.semanticHash(plan)
  }

  /// Returns a `StreamingQueryManager` that allows managing all the `StreamingQuery`s active on
  /// `this`.
  public var streams: StreamingQueryManager {
    get {
      StreamingQueryManager(self)
    }
  }

  /// This is defined as the return type of `SparkSession.sparkContext` method.
  /// This is an empty `Struct` type because `sparkContext` method is designed to throw
  /// `UNSUPPORTED_CONNECT_FEATURE.SESSION_SPARK_CONTEXT`.
  public struct SparkContext: Sendable {
  }

  /// A builder to create ``SparkSession`` instances.
  ///
  /// The Builder pattern provides a fluent interface for configuring and creating SparkSession instances.
  /// This is the recommended way to create a SparkSession.
  ///
  /// ## Creating a Session
  ///
  /// ```swift
  /// // Basic session creation
  /// let spark = try await SparkSession.builder
  ///     .remote("sc://localhost:15002")
  ///     .getOrCreate()
  ///
  /// // With additional configuration
  /// let configuredSpark = try await SparkSession.builder
  ///     .appName("MyAnalyticsApp")
  ///     .config("spark.sql.shuffle.partitions", "200")
  ///     .remote("sc://spark-cluster:15002")
  ///     .getOrCreate()
  /// ```
  ///
  /// ## Environment Variables
  ///
  /// The builder will use the `SPARK_REMOTE` environment variable if no remote URL is specified.
  /// If neither is provided, it defaults to `sc://localhost:15002`.
  ///
  /// - Important: This is a singleton builder. Multiple calls to `SparkSession.builder` return the same instance.
  public actor Builder {
    var sparkConf: [String: String] = [:]

    /// Sets a configuration option for the ``SparkSession``.
    ///
    /// Configuration options control various aspects of Spark behavior, from execution settings
    /// to SQL optimization parameters.
    ///
    /// ```swift
    /// let spark = try await SparkSession.builder
    ///     .config("spark.sql.shuffle.partitions", "200")
    ///     .config("spark.sql.adaptive.enabled", "true")
    ///     .getOrCreate()
    /// ```
    ///
    /// - Parameters:
    ///   - key: The configuration key (e.g., "spark.sql.shuffle.partitions")
    ///   - value: The configuration value as a string
    /// - Returns: The builder instance for method chaining
    public func config(_ key: String, _ value: String) -> Builder {
      sparkConf[key] = value
      return self
    }

    /// Sets a configuration option for the ``SparkSession``.
    /// - Parameters:
    ///   - key: The configuration key (e.g., "spark.sql.shuffle.partitions")
    ///   - value: The configuration bool value.
    /// - Returns: The builder instance for method chaining
    public func config(_ key: String, _ value: Bool) -> Builder {
      return config(key, String(value))
    }

    /// Sets a configuration option for the ``SparkSession``.
    /// - Parameters:
    ///   - key: The configuration key (e.g., "spark.sql.shuffle.partitions")
    ///   - value: The configuration `Int` value.
    /// - Returns: The builder instance for method chaining
    public func config(_ key: String, _ value: Int) -> Builder {
      return config(key, String(value))
    }

    /// Sets a configuration option for the ``SparkSession``.
    /// - Parameters:
    ///   - key: The configuration key (e.g., "spark.sql.shuffle.partitions")
    ///   - value: The configuration `Int64` value.
    /// - Returns: The builder instance for method chaining
    public func config(_ key: String, _ value: Int64) -> Builder {
      return config(key, String(value))
    }

    /// Sets a configuration option for the ``SparkSession``.
    /// - Parameters:
    ///   - key: The configuration key (e.g., "spark.sql.shuffle.partitions")
    ///   - value: The configuration `Double` value.
    /// - Returns: The builder instance for method chaining
    public func config(_ key: String, _ value: Double) -> Builder {
      return config(key, String(value))
    }

    /// Removes all configuration options from the builder.
    ///
    /// This method clears all previously set configurations, allowing you to start fresh.
    ///
    /// ```swift
    /// // Clear all configurations
    /// SparkSession.builder.clear()
    /// ```
    ///
    /// - Returns: The builder instance for method chaining
    @discardableResult
    func clear() -> Builder {
      sparkConf.removeAll()
      return self
    }

    /// Sets the remote URL for the Spark Connect server.
    ///
    /// The remote URL specifies which Spark Connect server to connect to.
    /// The URL format is `sc://{host}:{port}`.
    ///
    /// ```swift
    /// // Connect to a local Spark server
    /// let localSpark = try await SparkSession.builder
    ///     .remote("sc://localhost:15002")
    ///     .getOrCreate()
    ///
    /// // Connect to a remote cluster
    /// let remoteSpark = try await SparkSession.builder
    ///     .remote("sc://spark-cluster.example.com:15002")
    ///     .getOrCreate()
    /// ```
    ///
    /// - Parameter url: The connection URL in format `sc://{host}:{port}`
    /// - Returns: The builder instance for method chaining
    public func remote(_ url: String) -> Builder {
      return config("spark.remote", url)
    }

    /// Sets the application name for this ``SparkSession``.
    ///
    /// The application name is displayed in the Spark UI and helps identify your application
    /// among others running on the cluster.
    ///
    /// ```swift
    /// let spark = try await SparkSession.builder
    ///     .appName("ETL Pipeline - Q4 2024")
    ///     .remote("sc://localhost:15002")
    ///     .getOrCreate()
    /// ```
    ///
    /// - Parameter name: The name of your Spark application
    /// - Returns: The builder instance for method chaining
    public func appName(_ name: String) -> Builder {
      return config("spark.app.name", name)
    }

    /// Enables Apache Hive metastore support.
    ///
    /// When Hive support is enabled, Spark can read and write data from Hive tables
    /// and use the Hive metastore for table metadata.
    ///
    /// ```swift
    /// let spark = try await SparkSession.builder
    ///     .enableHiveSupport()
    ///     .getOrCreate()
    /// ```
    ///
    /// - Returns: The builder instance for method chaining
    func enableHiveSupport() -> Builder {
      return config("spark.sql.catalogImplementation", "hive")
    }

    /// Create a new ``SparkSession``. If `spark.remote` is not given, `sc://localhost:15002` is used.
    /// - Returns: A newly created ``SparkSession``.
    func create() async throws -> SparkSession {
      let remote = ProcessInfo.processInfo.environment["SPARK_REMOTE"] ?? "sc://localhost:15002"
      let session = SparkSession(sparkConf["spark.remote"] ?? remote)
      let response = try await session.client.connect(session.sessionID)
      await session.setVersion(response.sparkVersion.version)
      let isSuccess = try await session.client.setConf(map: sparkConf)
      assert(isSuccess)
      return session
    }

    /// Creates or retrieves an existing ``SparkSession``.
    ///
    /// This is the primary method for obtaining a ``SparkSession`` instance. If a session with
    /// the same configuration already exists, it will be returned. Otherwise, a new session
    /// is created with the specified configuration.
    ///
    /// The method will:
    /// 1. Check for the `SPARK_REMOTE` environment variable if no remote URL is set
    /// 2. Use `sc://localhost:15002` as the default if neither is specified
    /// 3. Connect to the Spark server and set up the session
    /// 4. Apply all configured settings
    ///
    /// ```swift
    /// // Basic usage
    /// let spark = try await SparkSession.builder.getOrCreate()
    ///
    /// // With configuration
    /// let configuredSpark = try await SparkSession.builder
    ///     .appName("DataAnalysis")
    ///     .config("spark.sql.shuffle.partitions", "100")
    ///     .getOrCreate()
    /// ```
    ///
    /// - Returns: A configured SparkSession instance
    /// - Throws: ``SparkConnectError`` if connection fails or configuration is invalid
    public func getOrCreate() async throws -> SparkSession {
      return try await create()
    }
  }
}
