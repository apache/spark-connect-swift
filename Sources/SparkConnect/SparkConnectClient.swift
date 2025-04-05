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
      let request = analyze(self.sessionID!, {
        return OneOf_Analyze.sparkVersion(AnalyzePlanRequest.SparkVersion())
      })
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

  /// Create a ``ConfigRequest`` instance for `Unset` operation.
  /// - Parameter key: A string for key to unset.
  /// - Returns: A ``ConfigRequest`` instance.
  func getConfigRequestUnset(keys: [String]) -> ConfigRequest {
    var request = ConfigRequest()
    request.operation = ConfigRequest.Operation()
    var unset = ConfigRequest.Unset()
    unset.keys = keys
    request.operation.opType = .unset(unset)
    return request
  }

  func unsetConf(keys: [String]) async throws -> Bool {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: self.host, port: self.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = SparkConnectService.Client(wrapping: client)
      var request = getConfigRequestUnset(keys: keys)
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
  /// - Returns: A `ConfigRequest` instance.
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
  /// - Returns:  A `Plan` instance.
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

  /// Create a ``ExecutePlanRequest`` instance with the given plan.
  /// The operation ID is created by UUID.
  /// - Parameters:
  ///   - plan: A plan to execute.
  /// - Returns: An ``ExecutePlanRequest`` instance.
  func getExecutePlanRequest(_ sessionID: String, _ plan: Plan) async
    -> ExecutePlanRequest
  {
    var request = ExecutePlanRequest()
    request.clientType = clientType
    request.userContext = userContext
    request.sessionID = self.sessionID!
    request.operationID = UUID().uuidString
    request.plan = plan
    return request
  }

  /// Create a ``AnalyzePlanRequest`` instance with the given plan.
  /// - Parameters:
  ///   - plan: A plan to analyze.
  /// - Returns: An ``AnalyzePlanRequest`` instance
  func getAnalyzePlanRequest(_ sessionID: String, _ plan: Plan) async
    -> AnalyzePlanRequest
  {
    return analyze(sessionID, {
      var schema = AnalyzePlanRequest.Schema()
      schema.plan = plan
      return OneOf_Analyze.schema(schema)
    })
  }

  private func analyze(_ sessionID: String, _ f: () -> OneOf_Analyze) -> AnalyzePlanRequest {
    var request = AnalyzePlanRequest()
    request.clientType = clientType
    request.userContext = userContext
    request.sessionID = self.sessionID!
    request.analyze = f()
    return request
  }

  func getPersist(_ sessionID: String, _ plan: Plan, _ storageLevel: StorageLevel) async
    -> AnalyzePlanRequest
  {
    return analyze(
      sessionID,
      {
        var persist = AnalyzePlanRequest.Persist()
        persist.storageLevel = storageLevel.toSparkConnectStorageLevel
        persist.relation = plan.root
        return OneOf_Analyze.persist(persist)
      })
  }

  func getUnpersist(_ sessionID: String, _ plan: Plan, _ blocking: Bool = false) async
    -> AnalyzePlanRequest
  {
    return analyze(
      sessionID,
      {
        var unpersist = AnalyzePlanRequest.Unpersist()
        unpersist.relation = plan.root
        unpersist.blocking = blocking
        return OneOf_Analyze.unpersist(unpersist)
      })
  }

  func getStorageLevel(_ sessionID: String, _ plan: Plan) async -> AnalyzePlanRequest
  {
    return analyze(
      sessionID,
      {
        var level = AnalyzePlanRequest.GetStorageLevel()
        level.relation = plan.root
        return OneOf_Analyze.getStorageLevel(level)
      })
  }

  func getExplain(_ sessionID: String, _ plan: Plan, _ mode: String) async -> AnalyzePlanRequest
  {
    return analyze(
      sessionID,
      {
        var explain = AnalyzePlanRequest.Explain()
        explain.plan = plan
        explain.explainMode = mode.toExplainMode
        return OneOf_Analyze.explain(explain)
      })
  }

  func getTreeString(_ sessionID: String, _ plan: Plan, _ level: Int32) async -> AnalyzePlanRequest
  {
    return analyze(
      sessionID,
      {
        var treeString = AnalyzePlanRequest.TreeString()
        treeString.plan = plan
        treeString.level = level
        return OneOf_Analyze.treeString(treeString)
      })
  }

  static func getProject(_ child: Relation, _ cols: [String]) -> Plan {
    var project = Project()
    project.input = child
    let expressions: [Spark_Connect_Expression] = cols.map {
      var expression = Spark_Connect_Expression()
      expression.exprType = .unresolvedAttribute($0.toUnresolvedAttribute)
      return expression
    }
    project.expressions = expressions
    var relation = Relation()
    relation.project = project
    var plan = Plan()
    plan.opType = .root(relation)
    return plan
  }

  static func getFilter(_ child: Relation, _ conditionExpr: String) -> Plan {
    var filter = Filter()
    filter.input = child
    filter.condition.expressionString = conditionExpr.toExpressionString
    var relation = Relation()
    relation.filter = filter
    var plan = Plan()
    plan.opType = .root(relation)
    return plan
  }

  static func getSort(_ child: Relation, _ cols: [String]) -> Plan {
    var sort = Sort()
    sort.input = child
    let expressions: [Spark_Connect_Expression.SortOrder] = cols.map {
      var expression = Spark_Connect_Expression.SortOrder()
      expression.child.exprType = .unresolvedAttribute($0.toUnresolvedAttribute)
      expression.direction = .ascending
      return expression
    }
    sort.order = expressions
    sort.isGlobal = true
    var relation = Relation()
    relation.sort = sort
    var plan = Plan()
    plan.opType = .root(relation)
    return plan
  }

  static func getLimit(_ child: Relation, _ n: Int32) -> Plan {
    var limit = Limit()
    limit.input = child
    limit.limit = n
    var relation = Relation()
    relation.limit = limit
    var plan = Plan()
    plan.opType = .root(relation)
    return plan
  }

  static func getTail(_ child: Relation, _ n: Int32) -> Plan {
    var tail = Tail()
    tail.input = child
    tail.limit = n
    var relation = Relation()
    relation.tail = tail
    var plan = Plan()
    plan.opType = .root(relation)
    return plan
  }

  var result: [ExecutePlanResponse] = []
  private func addResponse(_ response: ExecutePlanResponse) {
    self.result.append(response)
  }

  func execute(_ sessionID: String, _ command: Command) async throws -> [ExecutePlanResponse] {
    self.result.removeAll()
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: self.host, port: self.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = SparkConnectService.Client(wrapping: client)
      var plan = Plan()
      plan.opType = .command(command)
      try await service.executePlan(getExecutePlanRequest(sessionID, plan)) {
        response in
        for try await m in response.messages {
          await self.addResponse(m)
        }
      }
    }
    return result
  }
}
