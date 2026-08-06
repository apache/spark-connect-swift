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
import GRPCCore
import Synchronization

/// The stream progress of a reattachable `ExecutePlan` execution, shared with the `@Sendable`
/// response stream handlers.
private final class ReattachableExecuteState: Sendable {
  private struct Progress {
    var lastResponseID: String? = nil
    var serverSideSessionID: String? = nil
    var responseCount = 0
    var resultComplete = false
  }

  private let progress = Mutex(Progress())

  /// Record a received response and assert that the server side session has not changed.
  func record(_ response: ExecutePlanResponse) throws {
    try progress.withLock { p in
      if let id = p.serverSideSessionID, id != response.serverSideSessionID {
        throw SparkConnectError.invalidState(
          SparkConnectError.Details(
            message:
              "Server side session ID changed. Create a new SparkSession to continue. "
              + "(Old: \(id), New: \(response.serverSideSessionID))"))
      }
      p.serverSideSessionID = response.serverSideSessionID
      p.lastResponseID = response.responseID
      p.responseCount += 1
      if case .resultComplete = response.responseType {
        p.resultComplete = true
      }
    }
  }

  var lastResponseID: String? { progress.withLock { $0.lastResponseID } }
  var responseCount: Int { progress.withLock { $0.responseCount } }
  var resultComplete: Bool { progress.withLock { $0.resultComplete } }
}

extension SparkConnectClient {
  /// Return a copy of `request` whose `request_options` asks the server to keep the execution
  /// reattachable with `ReattachExecute` after the response stream is broken.
  static func getReattachableExecutePlanRequest(
    _ request: ExecutePlanRequest
  ) -> ExecutePlanRequest {
    var reattachableRequest = request
    var option = ExecutePlanRequest.RequestOption()
    var reattachOptions = ReattachOptions()
    reattachOptions.reattachable = true
    option.reattachOptions = reattachOptions
    reattachableRequest.requestOptions.append(option)
    return reattachableRequest
  }

  /// Create a ``ReattachExecuteRequest`` resuming the response stream of `request` after
  /// `lastResponseID`, or from the start if nil.
  static func getReattachExecuteRequest(
    _ request: ExecutePlanRequest, _ lastResponseID: String?
  ) -> ReattachExecuteRequest {
    var reattach = ReattachExecuteRequest()
    reattach.sessionID = request.sessionID
    reattach.userContext = request.userContext
    reattach.operationID = request.operationID
    reattach.clientType = request.clientType
    if let lastResponseID {
      reattach.lastResponseID = lastResponseID
    }
    return reattach
  }

  /// Create a ``ReleaseExecuteRequest`` releasing the whole operation of `request` on the server.
  static func getReleaseAllRequest(_ request: ExecutePlanRequest) -> ReleaseExecuteRequest {
    var release = ReleaseExecuteRequest()
    release.sessionID = request.sessionID
    release.userContext = request.userContext
    release.operationID = request.operationID
    release.clientType = request.clientType
    release.releaseAll = ReleaseExecuteRequest.ReleaseAll()
    return release
  }

  /// Execute `request` as a reattachable execution
  /// like `org.apache.spark.sql.connect.client.ExecutePlanResponseReattachableIterator`.
  ///
  /// The plan is executed with `ReattachOptions.reattachable=true`. When the response stream is
  /// broken by a retriable RPC error, or ends without a `ResultComplete` message, the stream is
  /// resumed with `ReattachExecute` after the last received response. The retriable errors are
  /// governed by ``RetryPolicy/defaultPolicy`` whose backoff is reset whenever an attempt
  /// receives a new response. The server side buffer is released with a `release_all`
  /// `ReleaseExecute` at the end.
  /// - Parameters:
  ///   - request: An ``ExecutePlanRequest`` to execute.
  ///   - processResponse: A handler invoked for each ``ExecutePlanResponse`` in order.
  func executePlanWithReattach(
    _ request: ExecutePlanRequest,
    _ processResponse: @Sendable @escaping (ExecutePlanResponse) async throws -> Void
  ) async throws {
    let initialRequest = Self.getReattachableExecutePlanRequest(request)
    let state = ReattachableExecuteState()

    @Sendable func consume(
      _ response: GRPCCore.StreamingClientResponse<ExecutePlanResponse>
    ) async throws {
      for try await m in response.messages {
        try state.record(m)
        if !m.observedMetrics.isEmpty {
          await self.updateObservations(m.observedMetrics)
        }
        try await processResponse(m)
      }
    }

    var retryState = RetryPolicyState(.defaultPolicy)
    var responseCountAtLastError = 0
    var reattach = false
    while !state.resultComplete {
      do {
        try await withGPRC(retryable: false) { client in
          let service = SparkConnectService.Client(wrapping: client)
          if reattach {
            let reattachRequest =
              Self.getReattachExecuteRequest(initialRequest, state.lastResponseID)
            try await service.reattachExecute(reattachRequest, onResponse: consume)
          } else {
            try await service.executePlan(initialRequest, onResponse: consume)
          }
          if state.resultComplete {
            _ = try await service.releaseExecute(Self.getReleaseAllRequest(initialRequest))
          }
        }
        // When the stream ended gracefully without `ResultComplete`, the server has more
        // responses and the loop continues with `ReattachExecute`.
        reattach = true
      } catch {
        reattach = true
        let responseCount = state.responseCount
        if Self.isOperationNotFound(error) {
          guard responseCount == 0 else {
            await releaseAll(initialRequest)
            throw SparkConnectError.invalidState(
              SparkConnectError.Details(
                message:
                  "OPERATION_NOT_FOUND on the server but responses were already received from it."))
          }
          // The initial `ExecutePlan` didn't reach the server. Start over with `ExecutePlan`.
          reattach = false
        } else if !RetryPolicy.canRetry(error) {
          await releaseAll(initialRequest)
          throw error
        }
        // An attempt which received new responses resets the backoff.
        if responseCount > responseCountAtLastError {
          responseCountAtLastError = responseCount
          retryState = RetryPolicyState(.defaultPolicy)
        }
        guard !Task.isCancelled, let wait = retryState.nextAttempt(error) else {
          await releaseAll(initialRequest)
          throw error
        }
        try await Task.sleep(for: wait)
      }
    }
  }

  /// Inform the server to release the operation of `request` after giving up, ignoring failures
  /// because the server releases abandoned executions by itself.
  private func releaseAll(_ request: ExecutePlanRequest) async {
    try? await withGPRC(retryable: false) { client in
      let service = SparkConnectService.Client(wrapping: client)
      _ = try await service.releaseExecute(Self.getReleaseAllRequest(request))
    }
  }

  /// Return true if the error means that the operation of the initial `ExecutePlan` doesn't
  /// exist on the server.
  private static func isOperationNotFound(_ error: Error) -> Bool {
    guard let error = error as? RPCError else { return false }
    return error.message.contains("INVALID_HANDLE.OPERATION_NOT_FOUND")
      || error.message.contains("INVALID_HANDLE.SESSION_NOT_FOUND")
  }
}
