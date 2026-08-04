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
import GRPCProtobuf

/// A retry policy with exponential backoff for RPC calls
/// like `org.apache.spark.sql.connect.client.RetryPolicy`.
struct RetryPolicy: Sendable {
  let maxRetries: Int
  let initialBackoff: Duration
  let maxBackoff: Duration
  let backoffMultiplier: Double
  let jitter: Duration
  let minJitterThreshold: Duration
  let maxServerRetryDelay: Duration

  /// The default policy whose constants are synchronized with the Scala side
  /// `org.apache.spark.sql.connect.client.RetryPolicy` and the Python side
  /// `pyspark.sql.connect.client.retries.DefaultPolicy`.
  ///
  /// Note: these constants are selected so that the maximum tolerated wait is guaranteed
  /// to be at least 10 minutes.
  static let defaultPolicy = RetryPolicy(
    maxRetries: 15,
    initialBackoff: .milliseconds(50),
    maxBackoff: .seconds(60),
    backoffMultiplier: 4.0,
    jitter: .milliseconds(500),
    minJitterThreshold: .seconds(2),
    maxServerRetryDelay: .seconds(10 * 60))

  /// Return true if the error can be retried under the default policy.
  /// - Parameter error: The error thrown by an RPC call.
  /// - Returns: True if the error is a ``RPCError`` with code `UNAVAILABLE`, an `INTERNAL`
  /// error caused by another RPC preempting this RPC, or any error containing
  /// `google.rpc.RetryInfo` in its status details.
  static func canRetry(_ error: Error) -> Bool {
    guard let error = error as? RPCError else {
      return false
    }
    if error.code == .unavailable {
      return true
    }
    // This error happens if another RPC preempts this RPC.
    if error.code == .internalError && error.message.contains("INVALID_CURSOR.DISCONNECTED") {
      return true
    }
    // All errors containing `RetryInfo` should be retried.
    return serverRetryDelay(of: error) != nil
  }

  /// Extract `google.rpc.RetryInfo.retry_delay` from the gRPC status details.
  /// - Parameter error: The error thrown by an RPC call.
  /// - Returns: The server-provided retry delay, or nil if absent.
  static func serverRetryDelay(of error: Error) -> Duration? {
    guard let error = error as? RPCError,
      let status = try? error.unpackGoogleRPCStatus()
    else {
      return nil
    }
    return status.details.compactMap { $0.retryInfo }.first?.delay
  }
}

/// The stateful part of ``RetryPolicy`` tracking how many attempts have happened
/// and how long to wait until the next one.
struct RetryPolicyState {
  private let policy: RetryPolicy
  private var attempt: Int = 0
  private var nextWait: Duration

  init(_ policy: RetryPolicy) {
    self.policy = policy
    self.nextWait = policy.initialBackoff
  }

  /// Return the time to wait until the next attempt, or nil if the retries are exhausted.
  /// - Parameter error: The error that caused this attempt, used to recognize the
  /// server-provided `RetryInfo.retry_delay` which can override the client's `maxBackoff`.
  mutating func nextAttempt(_ error: Error) -> Duration? {
    guard attempt < policy.maxRetries else {
      return nil
    }
    attempt += 1

    var wait = nextWait
    nextWait = min(multiply(nextWait, by: policy.backoffMultiplier), policy.maxBackoff)

    if let retryDelay = RetryPolicy.serverRetryDelay(of: error) {
      wait = max(wait, min(retryDelay, policy.maxServerRetryDelay))
    }

    if wait >= policy.minJitterThreshold {
      wait += multiply(policy.jitter, by: Double.random(in: 0...1))
    }
    return wait
  }

  private func multiply(_ duration: Duration, by multiplier: Double) -> Duration {
    let seconds =
      Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    return .seconds(seconds * multiplier)
  }
}

/// Run `body` and retry it with exponential backoff when `shouldRetry` accepts the thrown error.
/// Task cancellation stops the retry loop immediately. When the retries are exhausted,
/// the last error is rethrown.
/// - Parameters:
///   - policy: A ``RetryPolicy`` instance.
///   - isolation: The actor isolation inherited from the caller.
///   - shouldRetry: A function that determines whether an error can be retried.
///   - body: A block to run.
/// - Returns: The result of `body`.
func withRetry<Result>(
  _ policy: RetryPolicy = .defaultPolicy,
  isolation: isolated (any Actor)? = #isolation,
  shouldRetry: (Error) -> Bool = RetryPolicy.canRetry,
  _ body: () async throws -> Result
) async throws -> Result {
  var state = RetryPolicyState(policy)
  while true {
    do {
      return try await body()
    } catch {
      guard !Task.isCancelled, shouldRetry(error), let wait = state.nextAttempt(error) else {
        throw error
      }
      try await Task.sleep(for: wait)
    }
  }
}
