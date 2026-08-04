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
import GRPCCore
import GRPCProtobuf

/// A converter from gRPC ``RPCError``s to ``SparkConnectError``s
/// like `org.apache.spark.sql.connect.client.GrpcExceptionConverter`.
///
/// It parses the `google.rpc.ErrorInfo` from the gRPC status details to classify errors by
/// the Spark error class and to enrich ``SparkConnectError/Details-swift.struct`` with
/// the error class, SQLSTATE, and message parameters. When the status details are missing,
/// it falls back to classifying by the error message. When the `ErrorInfo` provides an
/// `errorId`, it additionally fetches the server-side error chain with the un-truncated
/// messages and optional stack traces by the `FetchErrorDetails` RPC.
enum GrpcErrorConverter {
  /// Convert an ``RPCError`` into a ``SparkConnectError`` after enriching it with the
  /// server-side error chain fetched by an additional `FetchErrorDetails` RPC
  /// like `org.apache.spark.sql.connect.client.GrpcExceptionConverter.fetchEnrichedError`.
  /// The RPC call is best-effort at-most-once without retries, and any failure silently
  /// falls back to the un-enriched conversion in order not to mask the original error.
  /// - Parameters:
  ///   - error: An ``RPCError`` from the server.
  ///   - client: A connected gRPC client to send the `FetchErrorDetails` request.
  ///   - sessionID: A session ID string. The enrichment is skipped if nil.
  ///   - userContext: A ``UserContext`` of the session.
  ///   - clientType: A client type string.
  /// - Returns: A ``SparkConnectError`` or nil if unclassifiable.
  static func convert(
    _ error: RPCError,
    fetchingDetailsWith client: GRPCClient<some ClientTransport>,
    sessionID: String?,
    userContext: UserContext,
    clientType: String
  ) async -> SparkConnectError? {
    var (details, errorID) = parse(error)
    if let errorID, let sessionID,
      let response = await fetchErrorDetails(
        errorID, with: client, sessionID: sessionID, userContext: userContext,
        clientType: clientType)
    {
      details = enrich(details, with: response)
    }
    return classify(details)
  }

  /// Build ``SparkConnectError/Details-swift.struct`` from the `google.rpc.ErrorInfo` of
  /// the gRPC status details, together with the server-side `errorId` if provided.
  private static func parse(_ error: RPCError) -> (SparkConnectError.Details, errorID: String?) {
    guard let status = try? error.unpackGoogleRPCStatus(),
      let info = status.details.compactMap({ $0.errorInfo }).first
    else {
      return (SparkConnectError.Details(message: error.message), nil)
    }
    let details = SparkConnectError.Details(
      message: status.message.isEmpty ? error.message : status.message,
      errorClass: info.metadata["errorClass"],
      sqlState: info.metadata["sqlState"],
      messageParameters: messageParameters(from: info.metadata["messageParameters"]))
    return (details, info.metadata["errorId"])
  }

  /// Classify ``SparkConnectError/Details-swift.struct`` into a ``SparkConnectError``
  /// by the Spark error class first and by the error message as a fallback.
  private static func classify(_ details: SparkConnectError.Details) -> SparkConnectError? {
    if let errorClass = details.errorClass, let converted = convert(errorClass, details) {
      return converted
    }
    return convert(byMessage: details.message, details)
  }

  /// Send a `FetchErrorDetails` request, returning nil on any failure.
  private static func fetchErrorDetails(
    _ errorID: String,
    with client: GRPCClient<some ClientTransport>,
    sessionID: String,
    userContext: UserContext,
    clientType: String
  ) async -> FetchErrorDetailsResponse? {
    let service = SparkConnectService.Client(wrapping: client)
    var request = FetchErrorDetailsRequest()
    request.sessionID = sessionID
    request.userContext = userContext
    request.clientType = clientType
    request.errorID = errorID
    return try? await service.fetchErrorDetails(request)
  }

  /// Merge a `FetchErrorDetailsResponse` into ``SparkConnectError/Details-swift.struct``.
  /// The `errorClass` and `sqlState` from `ErrorInfo` take precedence to keep the
  /// error classification stable, while the un-truncated message and the message
  /// parameters of the root error replace the truncated ones.
  private static func enrich(
    _ details: SparkConnectError.Details, with response: FetchErrorDetailsResponse
  ) -> SparkConnectError.Details {
    guard response.hasRootErrorIdx else { return details }
    let serverErrors = flatten(response)
    guard let root = serverErrors.first else { return details }
    return SparkConnectError.Details(
      message: root.message.isEmpty ? details.message : root.message,
      errorClass: details.errorClass ?? root.errorClass,
      sqlState: details.sqlState ?? root.sqlState,
      messageParameters: root.messageParameters.isEmpty
        ? details.messageParameters : root.messageParameters,
      serverErrors: serverErrors)
  }

  /// Flatten the `causeIdx`-linked errors of a `FetchErrorDetailsResponse` into an array
  /// starting from the root error, guarding against out-of-range indices and cycles.
  private static func flatten(
    _ response: FetchErrorDetailsResponse
  ) -> [SparkConnectError.ServerError] {
    var serverErrors: [SparkConnectError.ServerError] = []
    var visited = Set<Int>()
    var index = Int(response.rootErrorIdx)
    while response.errors.indices.contains(index), visited.insert(index).inserted {
      let error = response.errors[index]
      let throwable = error.hasSparkThrowable ? error.sparkThrowable : nil
      serverErrors.append(
        SparkConnectError.ServerError(
          message: error.message,
          errorTypeHierarchy: error.errorTypeHierarchy,
          stackTrace: error.stackTrace.map {
            "\($0.declaringClass).\($0.methodName)(\($0.fileName):\($0.lineNumber))"
          },
          errorClass: throwable?.hasErrorClass == true ? throwable?.errorClass : nil,
          sqlState: throwable?.hasSqlState == true ? throwable?.sqlState : nil,
          messageParameters: throwable?.messageParameters ?? [:]))
      guard error.hasCauseIdx else { break }
      index = Int(error.causeIdx)
    }
    return serverErrors
  }

  /// Classify by the Spark error class from `ErrorInfo`.
  private static func convert(
    _ errorClass: String, _ details: SparkConnectError.Details
  ) -> SparkConnectError? {
    switch errorClass {
    case "CATALOG_NOT_FOUND":
      return .catalogNotFound(details)
    case "DATA_SOURCE_NOT_FOUND":
      return .dataSourceNotFound(details)
    case "INVALID_HANDLE.SESSION_CLOSED":
      return .sessionClosed(details)
    case "INVALID_IDENTIFIER", "UNSUPPORTED_DATATYPE":
      return .invalidType(details)
    case "OUTPUT_TYPE_UNSPECIFIED":
      return .outputTypeUnspecified(details)
    case "PARSE_SYNTAX_ERROR":
      return .parseSyntaxError(details)
    case "SCHEMA_NOT_FOUND":
      return .schemaNotFound(details)
    case "SQL_CONF_NOT_FOUND":
      return .sqlConfNotFound(details)
    case "TABLE_OR_VIEW_ALREADY_EXISTS":
      return .tableOrViewAlreadyExists(details)
    case "TABLE_OR_VIEW_NOT_FOUND":
      return .tableOrViewNotFound(details)
    case "UNRESOLVED_COLUMN.WITH_SUGGESTION":
      return .columnNotFound(details)
    default:
      return nil
    }
  }

  /// Classify by the error message for legacy error classes and
  /// servers that don't provide `ErrorInfo`.
  private static func convert(
    byMessage message: String, _ details: SparkConnectError.Details
  ) -> SparkConnectError? {
    switch message {
    case let m where m.contains("CATALOG_NOT_FOUND"):
      return .catalogNotFound(details)
    case let m where m.contains("DATA_SOURCE_NOT_FOUND"):
      return .dataSourceNotFound(details)
    case let m where m.contains("INVALID_HANDLE.SESSION_CLOSED"):
      return .sessionClosed(details)
    case let m where m.contains("UNSUPPORTED_DATATYPE") || m.contains("INVALID_IDENTIFIER"):
      return .invalidType(details)
    case let m where m.contains("Invalid view name:"):
      return .invalidViewName(details)
    case let m where m.contains("OUTPUT_TYPE_UNSPECIFIED"):
      return .outputTypeUnspecified(details)
    case let m where m.contains("PARSE_SYNTAX_ERROR"):
      return .parseSyntaxError(details)
    case let m where m.contains("SCHEMA_NOT_FOUND"):
      return .schemaNotFound(details)
    case let m where m.contains("SQL_CONF_NOT_FOUND"):
      return .sqlConfNotFound(details)
    case let m where m.contains("TABLE_OR_VIEW_ALREADY_EXISTS"):
      return .tableOrViewAlreadyExists(details)
    case let m where m.contains("TABLE_OR_VIEW_NOT_FOUND"):
      return .tableOrViewNotFound(details)
    case let m where m.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"):
      return .columnNotFound(details)
    default:
      return nil
    }
  }

  /// Decode the JSON-encoded `messageParameters` of `ErrorInfo` metadata.
  private static func messageParameters(from json: String?) -> [String: String] {
    guard let json, let data = json.data(using: .utf8) else { return [:] }
    return (try? JSONDecoder().decode([String: String].self, from: data)) ?? [:]
  }
}
