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

  @Test
  func clientObservedServerSideSessionID() {
    var request = Self.executePlanRequest()
    let reattachWithoutID = SparkConnectClient.getReattachExecuteRequest(request, nil)
    #expect(!reattachWithoutID.hasClientObservedServerSideSessionID)
    #expect(!SparkConnectClient.getReleaseAllRequest(request).hasClientObservedServerSideSessionID)

    request.clientObservedServerSideSessionID = "server-side-session-id"
    let reattach = SparkConnectClient.getReattachExecuteRequest(request, nil)
    #expect(reattach.clientObservedServerSideSessionID == "server-side-session-id")
    let release = SparkConnectClient.getReleaseAllRequest(request)
    #expect(release.clientObservedServerSideSessionID == "server-side-session-id")
  }

  /// Create an ``ExecutePlanRequest`` with `client` without connecting to the server.
  static func executePlanRequest(_ client: isolated SparkConnectClient) async
    -> ExecutePlanRequest
  {
    client.sessionID = UUID().uuidString.lowercased()
    return await client.getExecutePlanRequest(Plan())
  }

  @Test
  func lowercaseOperationID() async throws {
    let client = try SparkConnectClient(remote: "sc://localhost")
    let request = SparkConnectClient.getReattachableExecutePlanRequest(
      await Self.executePlanRequest(client))
    let operationID = request.operationID
    // The same as `UUID.fromString(operationId).toString` of `ExecutePlan` in the server side.
    #expect(UUID(uuidString: operationID)?.uuidString.lowercased() == operationID)
    #expect(SparkConnectClient.getReattachExecuteRequest(request, nil).operationID == operationID)
    #expect(SparkConnectClient.getReleaseAllRequest(request).operationID == operationID)
  }
}
