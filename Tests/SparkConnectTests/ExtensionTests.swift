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

import Testing

@testable import SparkConnect

/// A test suite for `Extension`
@Suite(.serialized)
struct ExtensionTests {
  @Test
  func firstOrThrowOnNonEmptyArray() throws {
    #expect(try [1, 2, 3].firstOrThrow() == 1)
  }

  @Test
  func firstOrThrowOnEmptyArray() throws {
    let error = try #require(throws: SparkConnectError.self) {
      _ = try [Int]().firstOrThrow()
    }
    #expect(error == .InvalidState)
    #expect(
      error.message == "The server returned an empty response for firstOrThrowOnEmptyArray().")
  }

  @Test
  func firstOrThrowCommandResultAfterExecutionProgress() throws {
    // The server sends an empty `ExecutionProgress` periodically while a command is running.
    var progress = ExecutePlanResponse()
    progress.executionProgress = ExecutePlanResponse.ExecutionProgress()
    var result = ExecutePlanResponse()
    result.streamingQueryCommandResult.awaitTermination.terminated = true
    var complete = ExecutePlanResponse()
    complete.resultComplete = ExecutePlanResponse.ResultComplete()
    let responses = [progress, progress, result, complete]
    #expect(try responses.firstOrThrow(\.streamingQueryCommandResult).awaitTermination.terminated)
  }

  @Test
  func firstOrThrowEmptyCommandResult() throws {
    // An empty result is found by its case while the responses of no or other cases are skipped.
    var metrics = ExecutePlanResponse()
    metrics.metrics = ExecutePlanResponse.Metrics()
    var other = ExecutePlanResponse()
    other.sqlCommandResult = ExecutePlanResponse.SqlCommandResult()
    var result = ExecutePlanResponse()
    result.checkpointCommandResult = Spark_Connect_CheckpointCommandResult()
    let responses = [metrics, other, result]
    #expect(try responses.firstOrThrow(\.checkpointCommandResult) == result.checkpointCommandResult)
  }

  @Test
  func firstOrThrowWithoutCommandResult() throws {
    var progress = ExecutePlanResponse()
    progress.executionProgress = ExecutePlanResponse.ExecutionProgress()
    var complete = ExecutePlanResponse()
    complete.resultComplete = ExecutePlanResponse.ResultComplete()
    let error = try #require(throws: SparkConnectError.self) {
      _ = try [progress, complete].firstOrThrow(\.checkpointCommandResult)
    }
    #expect(error == .InvalidState)
    #expect(
      error.message
        == "The server returned no Spark_Connect_CheckpointCommandResult for "
        + "firstOrThrowWithoutCommandResult().")
  }
}
