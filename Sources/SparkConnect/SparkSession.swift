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

import Dispatch
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import GRPCProtobuf
import Synchronization

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

  /// Create a session that uses the specified connection string and userID.
  /// - Parameters:
  ///   - connection: a string in a patter, `sc://{host}:{port}`
  ///   - userID: an optional user ID. If absent, `SPARK_USER` environment or ``ProcessInfo.processInfo.userName`` is used.
  init(_ connection: String, _ userID: String? = nil) {
    let processInfo = ProcessInfo.processInfo
#if os(macOS) || os(Linux)
    let userName = processInfo.environment["SPARK_USER"] ?? processInfo.userName
#else
    let userName = processInfo.environment["SPARK_USER"] ?? ""
#endif
    self.client = SparkConnectClient(remote: connection, user: userID ?? userName)
    self.conf = RuntimeConf(self.client)
  }

  /// The Spark version of Spark Connect Servier. This is supposed to be overwritten during establishing connections.
  public var version: String = ""

  func setVersion(_ version: String) {
    self.version = version
  }

  /// A unique session ID for this session from client.
  nonisolated let sessionID: String = UUID().uuidString

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
      throw SparkConnectError.UnsupportedOperationException
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

  /// Create a ``DataFrame`` with a single ``Int64`` column name `id`, containing elements in a
  /// range from 0 to `end` (exclusive) with step value 1.
  ///
  /// - Parameter end: A value for the end of range.
  /// - Returns: A ``DataFrame`` instance.
  public func range(_ end: Int64) async throws -> DataFrame {
    return try await range(0, end)
  }

  /// Create a ``DataFrame`` with a single ``Int64`` column named `id`, containing elements in a
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

  /// Create a ``DataFrame`` for the given SQL statement.
  /// - Parameter sqlText: A SQL string.
  /// - Returns: A ``DataFrame`` instance.
  public func sql(_ sqlText: String) async throws -> DataFrame {
    return try await DataFrame(spark: self, sqlText: sqlText)
  }

  /// Returns a ``DataFrameReader`` that can be used to read non-streaming data in as a
  /// `DataFrame`
  public var read: DataFrameReader {
    get {
      return DataFrameReader(sparkSession: self)
    }
  }

  /// Returns the specified table/view as a ``DataFrame``. If it's a table, it must support batch
  /// reading and the returned ``DataFrame`` is the batch scan query plan of this table. If it's a
  /// view, the returned ``DataFrame`` is simply the query plan of the view, which can either be a
  /// batch or streaming query plan.
  ///
  /// - Parameter tableName: a qualified or unqualified name that designates a table or view. If a database is
  /// specified, it identifies the table/view from the database. Otherwise, it first attempts to
  /// find a temporary view with the given name and then match the table/view from the current
  /// database. Note that, the global temporary view database is also valid here.
  /// - Returns: A ``DataFrame`` instance.
  public func table(_ tableName: String) async throws -> DataFrame {
    return await read.table(tableName)
  }

  /// Executes some code block and prints to stdout the time taken to execute the block.
  /// - Parameter f: A function to execute.
  /// - Returns: The result of the executed code.
  public func time<T: Sendable>(_ f: () async throws -> T) async throws -> T {
    let start = DispatchTime.now()
    let ret = try await f()
    let end = DispatchTime.now()
    let elapsed = (end.uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000
    print("Time taken: \(elapsed) ms")
    return ret
  }

  /// Add a tag to be assigned to all the operations started by this thread in this session.
  /// - Parameter tag: The tag to be added. Cannot contain ',' (comma) character or be an empty string.
  public func addTag(_ tag: String) async throws {
    try await client.addTag(tag: tag)
  }

  /// Remove a tag previously added to be assigned to all the operations started by this thread in this session.
  /// Noop if such a tag was not added earlier.
  /// - Parameter tag: The tag to be removed. Cannot contain ',' (comma) character or be an empty string.
  public func removeTag(_ tag: String) async throws {
    try await client.removeTag(tag: tag)
  }

  /// Get the operation tags that are currently set to be assigned to all the operations started by
  /// this thread in this session.
  /// - Returns: A set of string.
  public func getTags() async -> Set<String> {
    return await client.getTags()
  }

  /// Clear the current thread's operation tags.
  public func clearTags() async {
    await client.clearTags()
  }

  /// This is defined as the return type of `SparkSession.sparkContext` method.
  /// This is an empty `Struct` type because `sparkContext` method is designed to throw
  /// `UNSUPPORTED_CONNECT_FEATURE.SESSION_SPARK_CONTEXT`.
  public struct SparkContext: Sendable {
  }

  /// A builder to create ``SparkSession``
  public actor Builder {
    var sparkConf: [String: String] = [:]

    /// Set a new configuration.
    /// - Parameters:
    ///   - key: A string for the configuration key.
    ///   - value: A string for the configuration value.
    /// - Returns: self
    public func config(_ key: String, _ value: String) -> Builder {
      sparkConf[key] = value
      return self
    }

    /// Remove all stored configurations.
    /// - Returns: self
    func clear() -> Builder {
      sparkConf.removeAll()
      return self
    }

    /// Set a url for remote connection.
    /// - Parameter url: A connection string in a pattern, `sc://{host}:{post}`.
    /// - Returns: self
    public func remote(_ url: String) -> Builder {
      return config("spark.remote", url)
    }

    /// Set `appName` of this session.
    /// - Parameter name: A string for application name
    /// - Returns: self
    public func appName(_ name: String) -> Builder {
      return config("spark.app.name", name)
    }

    /// Enable `Apache Hive` metastore support configuration.
    /// - Returns: self
    func enableHiveSupport() -> Builder {
      return config("spark.sql.catalogImplementation", "hive")
    }

    /// Create a new ``SparkSession``. If `spark.remote` is not given, `sc://localhost:15002` is used.
    /// - Returns: A newly created `SparkSession`.
    func create() async throws -> SparkSession {
      let session = SparkSession(sparkConf["spark.remote"] ?? "sc://localhost:15002")
      let response = try await session.client.connect(session.sessionID)
      await session.setVersion(response.sparkVersion.version)
      let isSuccess = try await session.client.setConf(map: sparkConf)
      assert(isSuccess)
      return session
    }

    /// Create a ``SparkSession`` from the given configurations.
    /// - Returns: A spark session.
    public func getOrCreate() async throws -> SparkSession {
      return try await create()
    }
  }
}
