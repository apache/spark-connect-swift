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

/// A test suite for the reattachable execution request builders
@Suite(.serialized)
struct ReattachableExecuteTests {
  static func executePlanRequest() -> ExecutePlanRequest {
    var request = ExecutePlanRequest()
    request.clientType = "swift"
    request.userContext = "user".toUserContext
    request.sessionID = "session-id"
    request.operationID = "operation-id"
    return request
  }

  @Test
  func reattachableExecutePlanRequest() {
    let request = SparkConnectClient.getReattachableExecutePlanRequest(Self.executePlanRequest())
    #expect(request.requestOptions.count == 1)
    #expect(request.requestOptions[0].reattachOptions.reattachable)
    #expect(request.sessionID == "session-id")
    #expect(request.operationID == "operation-id")
  }

  @Test
  func reattachExecuteRequest() {
    let request =
      SparkConnectClient.getReattachExecuteRequest(Self.executePlanRequest(), "response-id")
    #expect(request.sessionID == "session-id")
    #expect(request.userContext.userID == "user")
    #expect(request.operationID == "operation-id")
    #expect(request.clientType == "swift")
    #expect(request.hasLastResponseID)
    #expect(request.lastResponseID == "response-id")
  }

  @Test
  func reattachExecuteRequestFromStart() {
    let request = SparkConnectClient.getReattachExecuteRequest(Self.executePlanRequest(), nil)
    #expect(!request.hasLastResponseID)
  }

  @Test
  func releaseAllRequest() {
    let request = SparkConnectClient.getReleaseAllRequest(Self.executePlanRequest())
    #expect(request.sessionID == "session-id")
    #expect(request.userContext.userID == "user")
    #expect(request.operationID == "operation-id")
    #expect(request.clientType == "swift")
    #expect(request.release == .releaseAll(ReleaseExecuteRequest.ReleaseAll()))
  }
}
