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

/// Information about progress made for a source in the execution of a ``StreamingQuery``
/// during a trigger. See ``StreamingQueryProgress`` for more information.
public struct SourceProgress: Sendable {
  let description: String
  let startOffset: String
  let endOffset: String
  let latestOffset: String
  let numInputRows: Int64
  let inputRowsPerSecond: Double
  let processedRowsPerSecond: Double
  let metrics: [String: String] = [:]
}

/// Information about progress made for a sink in the execution of a ``StreamingQuery``
/// during a trigger. See ``StreamingQueryProgress`` for more information.
public struct SinkProgress: Sendable {
  let description: String
  let numOutputRows: Int64
  let metrics: [String: String] = [:]

  init(_ description: String) {
    self.description = description
    self.numOutputRows = -1
  }

  init(_ description: String, _ numOutputRows: Int64) {
    self.description = description
    self.numOutputRows = numOutputRows
  }
}

/// Information about updates made to stateful operators in a ``StreamingQuery``
/// during a trigger. See ``StreamingQueryProgress`` for more information.
public struct StateOperatorProgress: Sendable {
  let operatorName: String
  let numRowsTotal: Int64
  let numRowsUpdated: Int64
  let allUpdatesTimeMs: Int64
  let numRowsRemoved: Int64
  let allRemovalsTimeMs: Int64
  let commitTimeMs: Int64
  let memoryUsedBytes: Int64
  let numRowsDroppedByWatermark: Int64
  let numShufflePartitions: Int64
  let numStateStoreInstances: Int64
  let customMetrics: [String: Int64]
}

/// Information about progress made in the execution of a ``StreamingQuery``
/// during a trigger. Each event relates to processing done for a single trigger of
/// the streaming query. Events are emitted even when no new data is available to be processed.
public struct StreamingQueryProcess {
  let id: UUID
  let runId: UUID
  let name: String
  let timestamp: String
  let batchId: Int64
  let batchDuration: Int64
  let durationMs: [String: Int64]
  let eventTime: [String: String]
  let stateOperators: [StateOperatorProgress]
  let sources: [SourceProgress]
  let sink: SinkProgress
  let observedMetrics: [String: Row]

  func numInputRows() -> Int64 {
    return sources.map { $0.numInputRows }.reduce(0, +)
  }

  func inputRowsPerSecond() -> Double {
    return sources.map { $0.inputRowsPerSecond }.reduce(0, +)
  }

  func processedRowsPerSecond() -> Double {
    return sources.map { $0.processedRowsPerSecond }.reduce(0, +)
  }
}

/// A class to manage all the ``StreamingQuery``s active in a ``SparkSession``.
public actor StreamingQueryManager {
  let sparkSession: SparkSession

  init(_ sparkSession: SparkSession) {
    self.sparkSession = sparkSession
  }

  /// Returns a list of active queries associated with this SQLContext
  public var active: [StreamingQuery] {
    get async throws {
      let command = StreamingQueryManagerCommand.OneOf_Command.active(true)
      let response = try await self.sparkSession.client.executeStreamingQueryManagerCommand(command)
      return response.first!.streamingQueryManagerCommandResult.active.activeQueries.map {
        StreamingQuery(
          UUID(uuidString: $0.id.id)!,
          UUID(uuidString: $0.id.runID)!,
          $0.name,
          self.sparkSession
        )
      }
    }
  }

  /// Returns the query if there is an active query with the given id, or null.
  /// - Parameter id: an UUID.
  /// - Returns: A ``StreamingQuery``.
  public func get(_ id: UUID) async throws -> StreamingQuery {
    return try await get(id.uuidString)
  }

  /// Returns the query if there is an active query with the given id, or null.
  /// - Parameter id: an UUID String
  /// - Returns: A ``StreamingQuery``.
  public func get(_ id: String) async throws -> StreamingQuery {
    let command = StreamingQueryManagerCommand.OneOf_Command.getQuery(id)
    let response = try await self.sparkSession.client.executeStreamingQueryManagerCommand(command)
    let query = response.first!.streamingQueryManagerCommandResult.query
    guard query.hasID else {
      throw SparkConnectError.InvalidArgumentException
    }
    return StreamingQuery(
      UUID(uuidString: query.id.id)!,
      UUID(uuidString: query.id.runID)!,
      query.name,
      self.sparkSession
    )
  }

  /// Wait until any of the queries on the associated SQLContext has terminated since the creation
  /// of the context, or since `resetTerminated()` was called. If any query was terminated with an
  /// exception, then the exception will be thrown.
  /// - Parameter timeoutMs: A timeout in milliseconds.
  @discardableResult
  public func awaitAnyTermination(_ timeoutMs: Int64? = nil) async throws -> Bool {
    var awaitAnyTerminationCommand = StreamingQueryManagerCommand.AwaitAnyTerminationCommand()
    if let timeoutMs {
      guard timeoutMs > 0 else {
        throw SparkConnectError.InvalidArgumentException
      }
      awaitAnyTerminationCommand.timeoutMs = timeoutMs
    }
    let command = StreamingQueryManagerCommand.OneOf_Command.awaitAnyTermination(
      awaitAnyTerminationCommand)
    let response = try await self.sparkSession.client.executeStreamingQueryManagerCommand(command)
    return response.first!.streamingQueryManagerCommandResult.awaitAnyTermination.terminated
  }

  ///  Forget about past terminated queries so that `awaitAnyTermination()` can be used again to
  ///  wait for new terminations.
  public func resetTerminated() async throws {
    let command = StreamingQueryManagerCommand.OneOf_Command.resetTerminated(true)
    _ = try await self.sparkSession.client.executeStreamingQueryManagerCommand(command)
  }
}
