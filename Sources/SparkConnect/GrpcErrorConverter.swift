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
/// it falls back to classifying by the error message.
enum GrpcErrorConverter {
  /// Convert an ``RPCError`` into a ``SparkConnectError``.
  /// - Parameter error: An ``RPCError`` from the server.
  /// - Returns: A ``SparkConnectError`` or nil if unclassifiable.
  static func convert(_ error: RPCError) -> SparkConnectError? {
    var details = SparkConnectError.Details(message: error.message)
    if let status = try? error.unpackGoogleRPCStatus(),
      let info = status.details.compactMap({ $0.errorInfo }).first
    {
      details = SparkConnectError.Details(
        message: status.message.isEmpty ? error.message : status.message,
        errorClass: info.metadata["errorClass"],
        sqlState: info.metadata["sqlState"],
        messageParameters: messageParameters(from: info.metadata["messageParameters"]))
    }
    if let errorClass = details.errorClass, let converted = convert(errorClass, details) {
      return converted
    }
    return convert(byMessage: details.message, details)
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
