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

import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import GRPCProtobuf
import Synchronization

/// Conceptually the remote spark session that communicates with the server
public actor SparkConnectClient {
  let clientType: String = "swift"
  let url: URL
  let host: String
  let port: Int
  let userContext: UserContext
  var sessionID: String? = nil

  /// Create a client to use GRPCClient.
  /// - Parameters:
  ///   - remote: A string to connect `Spark Connect` server.
  ///   - user: A string for the user ID of this connection.
  init(remote: String, user: String) {
    self.url = URL(string: remote)!
    self.host = url.host() ?? "localhost"
    self.port = self.url.port ?? 15002
    self.userContext = user.toUserContext
  }

  /// Stop the connection. Currently, this API is no-op because we don't reuse the connection yet.
  func stop() {
  }

  /// Connect to the `Spark Connect` server with the given session ID string.
  /// As a test connection, this sends the server `SparkVersion` request.
  /// - Parameter sessionID: A string for the session ID.
  /// - Returns: An `AnalyzePlanResponse` instance for `SparkVersion`
  func connect(_ sessionID: String) async throws -> AnalyzePlanResponse {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: self.host, port: self.port),
        transportSecurity: .plaintext
      )
    ) { client in
      // To prevent server-side `INVALID_HANDLE.FORMAT (SQLSTATE: HY000)` exception.
      if UUID(uuidString: sessionID) == nil {
        throw SparkConnectError.InvalidSessionIDException
      }

      self.sessionID = sessionID
      let service = SparkConnectService.Client(wrapping: client)
      let version = AnalyzePlanRequest.SparkVersion()
      var request = AnalyzePlanRequest()
      request.clientType = clientType
      request.userContext = userContext
      request.sessionID = sessionID
      request.analyze = .sparkVersion(version)
      let response = try await service.analyzePlan(request)
      return response
    }
  }

  /// Create a ``ConfigRequest`` instance for `Set` operation.
  /// - Parameter map: A map of key-value string pairs.
  /// - Returns: A ``ConfigRequest`` instance.
  func getConfigRequestSet(map: [String: String]) -> ConfigRequest {
    var request = ConfigRequest()
    request.operation = ConfigRequest.Operation()
    var set = ConfigRequest.Set()
    set.pairs = map.toSparkConnectKeyValue
    request.operation.opType = .set(set)
    return request
  }

  /// Request the server to set a map of configurations for this session.
  /// - Parameter map: A map of key-value pairs to set.
  /// - Returns: Always return true.
  func setConf(map: [String: String]) async throws -> Bool {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: self.host, port: self.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = SparkConnectService.Client(wrapping: client)
      var request = getConfigRequestSet(map: map)
      request.clientType = clientType
      request.userContext = userContext
      request.sessionID = self.sessionID!
      let _ = try await service.config(request)
      return true
    }
  }

  /// Create a ``ConfigRequest`` instance for `Get` operation.
  /// - Parameter keys: An array of keys to get.
  /// - Returns: A `ConfigRequest` instance.
  func getConfigRequestGet(keys: [String]) -> ConfigRequest {
    var request = ConfigRequest()
    request.operation = ConfigRequest.Operation()
    var get = ConfigRequest.Get()
    get.keys = keys
    request.operation.opType = .get(get)
    return request
  }

  /// Request the server to get a value of the given key.
  /// - Parameter key: A string for key to look up.
  /// - Returns: A string for the value of the key.
  func getConf(_ key: String) async throws -> String {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: self.host, port: self.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = SparkConnectService.Client(wrapping: client)
      var request = getConfigRequestGet(keys: [key])
      request.clientType = clientType
      request.userContext = userContext
      request.sessionID = self.sessionID!
      let response = try await service.config(request)
      return response.pairs[0].value
    }
  }

  /// Create a ``ConfigRequest`` for `GetAll` operation.
  /// - Returns: <#description#>
  func getConfigRequestGetAll() -> ConfigRequest {
    var request = ConfigRequest()
    request.operation = ConfigRequest.Operation()
    let getAll = ConfigRequest.GetAll()
    request.operation.opType = .getAll(getAll)
    return request
  }

  /// Request the server to get all configurations.
  /// - Returns: A map of key-value pairs.
  func getConfAll() async throws -> [String: String] {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: self.host, port: self.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = SparkConnectService.Client(wrapping: client)
      var request = getConfigRequestGetAll()
      request.clientType = clientType
      request.userContext = userContext
      request.sessionID = self.sessionID!
      let response = try await service.config(request)
      var map = [String: String]()
      for pair in response.pairs {
        map[pair.key] = pair.value
      }
      return map
    }
  }

  /// Create a `Plan` instance for `Range` relation.
  /// - Parameters:
  ///   - start: A start of the range.
  ///   - end: A end (exclusive) of the range.
  ///   - step: A step value for the range from `start` to `end`.
  /// - Returns: <#description#>
  func getPlanRange(_ start: Int64, _ end: Int64, _ step: Int64) -> Plan {
    var range = Range()
    range.start = start
    range.end = end
    range.step = step
    var relation = Relation()
    relation.range = range
    var plan = Plan()
    plan.opType = .root(relation)
    return plan
  }

  /// Create a ``ExecutePlanRequest`` instance with the given session ID and plan.
  /// The operation ID is created by UUID.
  /// - Parameters:
  ///   - sessionID: A string for the existing session ID.
  ///   - plan: A plan to execute.
  /// - Returns: An ``ExecutePlanRequest`` instance.
  func getExecutePlanRequest(_ sessionID: String, _ plan: Plan) async
    -> ExecutePlanRequest
  {
    var request = ExecutePlanRequest()
    request.clientType = clientType
    request.userContext = userContext
    request.sessionID = sessionID
    request.operationID = UUID().uuidString
    request.plan = plan
    return request
  }

  /// Create a ``AnalyzePlanRequest`` instance with the given session ID and plan.
  /// - Parameters:
  ///   - sessionID: A string for the existing session ID.
  ///   - plan: A plan to analyze.
  /// - Returns: An ``AnalyzePlanRequest`` instance
  func getAnalyzePlanRequest(_ sessionID: String, _ plan: Plan) async
    -> AnalyzePlanRequest
  {
    var request = AnalyzePlanRequest()
    request.clientType = clientType
    request.userContext = userContext
    request.sessionID = sessionID
    var schema = AnalyzePlanRequest.Schema()
    schema.plan = plan
    request.analyze = .schema(schema)
    return request
  }
}
