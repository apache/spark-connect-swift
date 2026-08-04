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
import Testing

@testable import SparkConnect

/// A test suite for `RetryPolicy`
@Suite(.serialized)
struct RetryPolicyTests {
  // Disambiguate from `GRPCCore.RetryPolicy`.
  typealias RetryPolicy = SparkConnect.RetryPolicy

  static let unavailable = RPCError(code: .unavailable, message: "Connection refused")
  static let cursorDisconnected = RPCError(
    code: .internalError, message: "INVALID_CURSOR.DISCONNECTED: The cursor has been disconnected.")

  static func errorWithRetryInfo(
    _ code: RPCError.Code, delay: Duration
  ) -> RPCError {
    let status = GoogleRPCStatus(code: code, message: "m", details: .retryInfo(delay: delay))
    return RPCError(code: code, message: "m", metadata: status.rpcErrorMetadata)
  }

  static func testPolicy(
    maxRetries: Int = 15,
    initialBackoff: Duration = .milliseconds(50),
    maxBackoff: Duration = .seconds(60),
    backoffMultiplier: Double = 4.0,
    jitter: Duration = .zero,
    minJitterThreshold: Duration = .zero,
    maxServerRetryDelay: Duration = .seconds(10 * 60)
  ) -> RetryPolicy {
    RetryPolicy(
      maxRetries: maxRetries,
      initialBackoff: initialBackoff,
      maxBackoff: maxBackoff,
      backoffMultiplier: backoffMultiplier,
      jitter: jitter,
      minJitterThreshold: minJitterThreshold,
      maxServerRetryDelay: maxServerRetryDelay)
  }

  func expectApprox(_ wait: Duration?, _ expected: Duration) throws {
    let wait = try #require(wait)
    #expect(wait >= expected - .microseconds(1) && wait <= expected + .microseconds(1))
  }

  @Test
  func defaultPolicy() {
    let policy = RetryPolicy.defaultPolicy
    #expect(policy.maxRetries == 15)
    #expect(policy.initialBackoff == .milliseconds(50))
    #expect(policy.maxBackoff == .seconds(60))
    #expect(policy.backoffMultiplier == 4.0)
    #expect(policy.jitter == .milliseconds(500))
    #expect(policy.minJitterThreshold == .seconds(2))
    #expect(policy.maxServerRetryDelay == .seconds(10 * 60))
  }

  @Test
  func canRetry() {
    #expect(RetryPolicy.canRetry(Self.unavailable))
    #expect(RetryPolicy.canRetry(Self.cursorDisconnected))
    #expect(
      RetryPolicy.canRetry(Self.errorWithRetryInfo(.resourceExhausted, delay: .seconds(1))))
    #expect(!RetryPolicy.canRetry(RPCError(code: .internalError, message: "Other error")))
    #expect(!RetryPolicy.canRetry(RPCError(code: .deadlineExceeded, message: "Timeout")))
    #expect(!RetryPolicy.canRetry(RPCError(code: .invalidArgument, message: "Invalid")))
    #expect(!RetryPolicy.canRetry(SparkConnectError.InvalidArgument))
    #expect(!RetryPolicy.canRetry(CancellationError()))
  }

  @Test
  func serverRetryDelay() throws {
    let delay = RetryPolicy.serverRetryDelay(
      of: Self.errorWithRetryInfo(.unavailable, delay: .seconds(5)))
    #expect(delay == .seconds(5))
    #expect(RetryPolicy.serverRetryDelay(of: Self.unavailable) == nil)
    #expect(RetryPolicy.serverRetryDelay(of: SparkConnectError.InvalidArgument) == nil)
  }

  @Test
  func backoffSequence() throws {
    var state = RetryPolicyState(Self.testPolicy(maxRetries: 8))
    try expectApprox(state.nextAttempt(Self.unavailable), .milliseconds(50))
    try expectApprox(state.nextAttempt(Self.unavailable), .milliseconds(200))
    try expectApprox(state.nextAttempt(Self.unavailable), .milliseconds(800))
    try expectApprox(state.nextAttempt(Self.unavailable), .milliseconds(3200))
    try expectApprox(state.nextAttempt(Self.unavailable), .milliseconds(12800))
    try expectApprox(state.nextAttempt(Self.unavailable), .milliseconds(51200))
    try expectApprox(state.nextAttempt(Self.unavailable), .seconds(60))
    try expectApprox(state.nextAttempt(Self.unavailable), .seconds(60))
    #expect(state.nextAttempt(Self.unavailable) == nil)
  }

  @Test
  func serverRetryDelayOverridesBackoff() throws {
    var state = RetryPolicyState(Self.testPolicy())
    let error = Self.errorWithRetryInfo(.unavailable, delay: .seconds(5))
    try expectApprox(state.nextAttempt(error), .seconds(5))

    // The server-provided delay is limited by `maxServerRetryDelay`.
    var another = RetryPolicyState(Self.testPolicy())
    let longDelay = Self.errorWithRetryInfo(.unavailable, delay: .seconds(20 * 60))
    try expectApprox(another.nextAttempt(longDelay), .seconds(10 * 60))
  }

  @Test
  func withRetryExhaustsAndRethrowsLastError() async throws {
    let policy = Self.testPolicy(maxRetries: 3, initialBackoff: Duration.milliseconds(1), maxBackoff: Duration.milliseconds(2))
    var attempts = 0
    let error = await #expect(throws: RPCError.self) {
      try await withRetry(policy) {
        attempts += 1
        throw Self.unavailable
      }
    }
    #expect(attempts == 4)
    #expect(error?.code == .unavailable)
  }

  @Test
  func withRetrySucceedsAfterFailures() async throws {
    let policy = Self.testPolicy(maxRetries: 3, initialBackoff: Duration.milliseconds(1), maxBackoff: Duration.milliseconds(2))
    var attempts = 0
    let value = try await withRetry(policy) {
      attempts += 1
      if attempts < 3 {
        throw Self.unavailable
      }
      return 42
    }
    #expect(value == 42)
    #expect(attempts == 3)
  }

  @Test
  func withRetryDoesNotRetryNonRetryableError() async throws {
    let policy = Self.testPolicy(maxRetries: 3, initialBackoff: Duration.milliseconds(1), maxBackoff: Duration.milliseconds(2))
    var attempts = 0
    await #expect(throws: SparkConnectError.InvalidArgument) {
      try await withRetry(policy) {
        attempts += 1
        throw SparkConnectError.InvalidArgument
      }
    }
    #expect(attempts == 1)
  }

  @Test
  func withRetryStopsOnCancellation() async throws {
    let policy = Self.testPolicy(
      maxRetries: 5, initialBackoff: Duration.seconds(60), backoffMultiplier: 1.0)
    let clock = ContinuousClock()
    let start = clock.now
    let task = Task {
      try await withRetry(policy) {
        throw Self.unavailable
      }
    }
    task.cancel()
    let result = await task.result
    #expect(clock.now - start < .seconds(10))
    switch result {
    case .success:
      Issue.record("Expected an error")
    case .failure(let error):
      #expect(error is CancellationError || (error as? RPCError)?.code == .unavailable)
    }
  }
}
