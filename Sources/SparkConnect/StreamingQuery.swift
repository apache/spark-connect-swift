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

public struct StreamingQueryException: Sendable {
  let exceptionMessage: String
  let errorClass: String
  let stackTrace: String
}

public struct StreamingQueryStatus: Sendable {
  let statusMessage: String
  let isDataAvailable: Bool
  let isTriggerActive: Bool
  let isActive: Bool
}

/// A handle to a query that is executing continuously in the background as new data arrives.
public actor StreamingQuery: Sendable {
  /// Returns the unique id of this query that persists across restarts from checkpoint data. That
  /// is, this id is generated when a query is started for the first time, and will be the same
  /// every time it is restarted from checkpoint data. Also see ``runId``.
  public let id: UUID

  /// Returns the unique id of this run of the query. That is, every start/restart of a query will
  /// generate a unique runId. Therefore, every time a query is restarted from checkpoint, it will
  /// have the same ``id`` but different ``runId``s.
  public let runId: UUID

  /// Returns the user-specified name of the query, or null if not specified. This name can be
  /// specified in the `org.apache.spark.sql.streaming.DataStreamWriter` as
  /// `dataframe.writeStream.queryName("query").start()`. This name, if set, must be unique across
  /// all active queries.
  public let name: String

  /// Returns the `SparkSession` associated with `this`.
  public let sparkSession: SparkSession

  init(_ id: UUID, _ runId: UUID, _ name: String, _ sparkSession: SparkSession) {
    self.id = id
    self.runId = runId
    self.name = name
    self.sparkSession = sparkSession
  }

  @discardableResult
  private func executeCommand(
    _ command: StreamingQueryCommand.OneOf_Command
  ) async throws -> [ExecutePlanResponse] {
    return try await self.sparkSession.client.executeStreamingQueryCommand(
      self.id.uuidString,
      self.runId.uuidString,
      command
    )
  }

  /// Returns `true` if this query is actively running.
  public var isActive: Bool {
    get async throws {
      let response = try await executeCommand(StreamingQueryCommand.OneOf_Command.status(true))
      return response.first!.streamingQueryCommandResult.status.isActive
    }
  }

  /// Returns the ``StreamingQueryException`` if the query was terminated by an exception.
  /// - Returns: A ``StreamingQueryException``.
  public func exception() async throws -> StreamingQueryException? {
    let response = try await executeCommand(StreamingQueryCommand.OneOf_Command.exception(true))
    let result = response.first!.streamingQueryCommandResult.exception
    return StreamingQueryException(
      exceptionMessage: result.exceptionMessage,
      errorClass: result.errorClass,
      stackTrace: result.stackTrace,
    )
  }

  /// Returns the current status of the query.
  /// - Returns:
  public func status() async throws -> StreamingQueryStatus {
    let response = try await executeCommand(StreamingQueryCommand.OneOf_Command.status(true))
    let result = response.first!.streamingQueryCommandResult.status
    return StreamingQueryStatus(
      statusMessage: result.statusMessage,
      isDataAvailable: result.isDataAvailable,
      isTriggerActive: result.isTriggerActive,
      isActive: result.isActive,
    )
  }

  /// Returns an array of the most recent ``StreamingQueryProgress`` updates for this query.
  /// The number of progress updates retained for each stream is configured by Spark session
  /// configuration `spark.sql.streaming.numRecentProgressUpdates`.
  public var recentProgress: [String] {
    get async throws {
      let response = try await executeCommand(
        StreamingQueryCommand.OneOf_Command.recentProgress(true))
      let result = response.first!.streamingQueryCommandResult.recentProgress
      return result.recentProgressJson
    }
  }

  /// Returns the most recent ``StreamingQueryProgress`` update of this streaming query.
  public var lastProgress: String? {
    get async throws {
      let response = try await executeCommand(
        StreamingQueryCommand.OneOf_Command.lastProgress(true))
      let result = response.first!.streamingQueryCommandResult.recentProgress
      return result.recentProgressJson.first
    }
  }

  /// Waits for the termination of `this` query, either by `query.stop()` or by an exception.
  /// If the query has terminated with an exception, then the exception will be thrown.
  ///
  /// If the query has terminated, then all subsequent calls to this method will either return
  /// immediately (if the query was terminated by `stop()`), or throw the exception immediately
  /// (if the query has terminated with exception).
  /// - Parameter timeout: A timeout in milliseconds.
  /// - Returns: True on termination.
  public func awaitTermination(_ timeoutMs: Int64? = nil) async throws -> Bool? {
    var command = Spark_Connect_StreamingQueryCommand.AwaitTerminationCommand()
    if let timeoutMs {
      command.timeoutMs = timeoutMs
    }
    let response = try await executeCommand(
      StreamingQueryCommand.OneOf_Command.awaitTermination(command))
    return response.first!.streamingQueryCommandResult.awaitTermination.terminated
  }

  /// Blocks until all available data in the source has been processed and committed to the sink.
  ///
  /// This method is intended for testing. Note that in the case of continually arriving data, this
  /// method may block forever. Additionally, this method is only guaranteed to block until data
  /// that has been synchronously appended data to a
  /// `org.apache.spark.sql.execution.streaming.Source` prior to invocation.
  /// (i.e. `getOffset` must immediately reflect the addition).
  public func processAllAvailable() async throws {
    try await executeCommand(StreamingQueryCommand.OneOf_Command.processAllAvailable(true))
  }

  /// Stops the execution of this query if it is running. This waits until the termination of the
  /// query execution threads or until a timeout is hit.
  ///
  /// By default stop will block indefinitely. You can configure a timeout by the configuration
  /// `spark.sql.streaming.stopTimeout`. A timeout of 0 (or negative) milliseconds will block
  /// indefinitely. If a `TimeoutException` is thrown, users can retry stopping the stream. If the
  /// issue persists, it is advisable to kill the Spark application.
  public func stop() async throws {
    try await executeCommand(StreamingQueryCommand.OneOf_Command.stop(true))
  }

  /// Prints the physical plan to the console for debugging purposes.
  /// - Parameter extended: Whether to do extended explain or not.
  public func explain(_ extended: Bool = false) async throws {
    var command = Spark_Connect_StreamingQueryCommand.ExplainCommand()
    command.extended = extended
    let response = try await executeCommand(StreamingQueryCommand.OneOf_Command.explain(command))
    print(response.first!.streamingQueryCommandResult.explain.result)
  }
}
