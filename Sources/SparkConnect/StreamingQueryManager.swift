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
      let result = try response.firstOrThrow().streamingQueryManagerCommandResult
      return result.active.activeQueries.map {
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
    let query = try response.firstOrThrow().streamingQueryManagerCommandResult.query
    guard query.hasID else {
      throw SparkConnectError.InvalidArgument
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
        throw SparkConnectError.InvalidArgument
      }
      awaitAnyTerminationCommand.timeoutMs = timeoutMs
    }
    let command = StreamingQueryManagerCommand.OneOf_Command.awaitAnyTermination(
      awaitAnyTerminationCommand)
    let response = try await self.sparkSession.client.executeStreamingQueryManagerCommand(command)
    let result = try response.firstOrThrow().streamingQueryManagerCommandResult
    return result.awaitAnyTermination.terminated
  }

  ///  Forget about past terminated queries so that `awaitAnyTermination()` can be used again to
  ///  wait for new terminations.
  public func resetTerminated() async throws {
    let command = StreamingQueryManagerCommand.OneOf_Command.resetTerminated(true)
    _ = try await self.sparkSession.client.executeStreamingQueryManagerCommand(command)
  }
}
