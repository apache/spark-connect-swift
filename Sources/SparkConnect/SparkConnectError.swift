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

/// A enum for ``SparkConnect`` package errors
///
/// Each case carries a ``SparkConnectError/Details-swift.struct`` payload with the error message
/// and, for server-side errors, the Spark error class and SQLSTATE.
///
/// Note that the equality of ``SparkConnectError`` compares only the error kind and
/// ignores the attached ``SparkConnectError/Details-swift.struct``.
public enum SparkConnectError: Error, Sendable, Equatable {
  /// A server-side error fetched by the `FetchErrorDetails` RPC.
  public struct ServerError: Sendable, Equatable {
    /// The un-truncated error message.
    public let message: String
    /// The fully qualified names of the exception class and its parent classes.
    public let errorTypeHierarchy: [String]
    /// The server-side stack trace frames formatted as `declaringClass.methodName(fileName:line)`.
    /// This is non-empty only if the SQL configuration
    /// `spark.sql.connect.serverStacktrace.enabled` is true.
    public let stackTrace: [String]
    /// A Spark error class of this error if it is a `SparkThrowable`.
    public let errorClass: String?
    /// A SQLSTATE of this error if it is a `SparkThrowable`.
    public let sqlState: String?
    /// The message parameters of the error class.
    public let messageParameters: [String: String]

    public init(
      message: String = "",
      errorTypeHierarchy: [String] = [],
      stackTrace: [String] = [],
      errorClass: String? = nil,
      sqlState: String? = nil,
      messageParameters: [String: String] = [:]
    ) {
      self.message = message
      self.errorTypeHierarchy = errorTypeHierarchy
      self.stackTrace = stackTrace
      self.errorClass = errorClass
      self.sqlState = sqlState
      self.messageParameters = messageParameters
    }
  }

  /// Detailed error information.
  public struct Details: Sendable, Equatable {
    /// An error message.
    public let message: String
    /// A Spark error class like `TABLE_OR_VIEW_NOT_FOUND` if the server provides it.
    public let errorClass: String?
    /// A SQLSTATE like `42P01` if the server provides it.
    public let sqlState: String?
    /// The message parameters of the error class.
    public let messageParameters: [String: String]
    /// The server-side error chain fetched by the `FetchErrorDetails` RPC.
    /// The first element is the thrown error and the rest follow the cause chain in order.
    /// This is empty if the server does not support the RPC or the fetch fails.
    public let serverErrors: [ServerError]

    public init(
      message: String = "",
      errorClass: String? = nil,
      sqlState: String? = nil,
      messageParameters: [String: String] = [:],
      serverErrors: [ServerError] = []
    ) {
      self.message = message
      self.errorClass = errorClass
      self.sqlState = sqlState
      self.messageParameters = messageParameters
      self.serverErrors = serverErrors
    }

    /// A multi-line string of the server-side error chain in the JVM stack trace style,
    /// or nil if ``serverErrors`` is empty.
    public var serverStackTrace: String? {
      guard !serverErrors.isEmpty else { return nil }
      var lines: [String] = []
      for (index, error) in serverErrors.enumerated() {
        let className = error.errorTypeHierarchy.first ?? ""
        if index == 0 {
          lines.append(className)
        } else {
          lines.append("Caused by: \(className): \(error.message)")
        }
        lines.append(contentsOf: error.stackTrace.map { "\tat \($0)" })
      }
      return lines.joined(separator: "\n")
    }
  }

  case catalogNotFound(Details)
  case columnNotFound(Details)
  case dataSourceNotFound(Details)
  case invalidArgument(Details)
  case invalidArrowData(Details)
  case invalidSessionID(Details)
  case invalidState(Details)
  case invalidType(Details)
  case invalidViewName(Details)
  case localRelationTooLarge(Details)
  case outputTypeUnspecified(Details)
  case parseSyntaxError(Details)
  case schemaNotFound(Details)
  case sessionClosed(Details)
  case sqlConfNotFound(Details)
  case tableOrViewAlreadyExists(Details)
  case tableOrViewNotFound(Details)
  case unsupportedOperation(Details)

  // Compatibility members to keep the original case spellings working
  // for `throw`/`catch`/`#expect(throws:)` call sites.
  public static var CatalogNotFound: SparkConnectError { .catalogNotFound(Details()) }
  public static var ColumnNotFound: SparkConnectError { .columnNotFound(Details()) }
  public static var DataSourceNotFound: SparkConnectError { .dataSourceNotFound(Details()) }
  public static var InvalidArgument: SparkConnectError { .invalidArgument(Details()) }
  public static var InvalidArrowData: SparkConnectError { .invalidArrowData(Details()) }
  public static var InvalidSessionID: SparkConnectError { .invalidSessionID(Details()) }
  public static var InvalidState: SparkConnectError { .invalidState(Details()) }
  public static var InvalidType: SparkConnectError { .invalidType(Details()) }
  public static var InvalidViewName: SparkConnectError { .invalidViewName(Details()) }
  public static var LocalRelationTooLarge: SparkConnectError { .localRelationTooLarge(Details()) }
  public static var OutputTypeUnspecified: SparkConnectError { .outputTypeUnspecified(Details()) }
  public static var ParseSyntaxError: SparkConnectError { .parseSyntaxError(Details()) }
  public static var SchemaNotFound: SparkConnectError { .schemaNotFound(Details()) }
  public static var SessionClosed: SparkConnectError { .sessionClosed(Details()) }
  public static var SqlConfNotFound: SparkConnectError { .sqlConfNotFound(Details()) }
  public static var TableOrViewAlreadyExists: SparkConnectError {
    .tableOrViewAlreadyExists(Details())
  }
  public static var TableOrViewNotFound: SparkConnectError { .tableOrViewNotFound(Details()) }
  public static var UnsupportedOperation: SparkConnectError { .unsupportedOperation(Details()) }

  /// The detailed error information.
  public var details: Details {
    switch self {
    case .catalogNotFound(let details), .columnNotFound(let details),
      .dataSourceNotFound(let details), .invalidArgument(let details),
      .invalidArrowData(let details), .invalidSessionID(let details),
      .invalidState(let details), .invalidType(let details), .invalidViewName(let details),
      .localRelationTooLarge(let details), .outputTypeUnspecified(let details),
      .parseSyntaxError(let details), .schemaNotFound(let details),
      .sessionClosed(let details), .sqlConfNotFound(let details),
      .tableOrViewAlreadyExists(let details), .tableOrViewNotFound(let details),
      .unsupportedOperation(let details):
      return details
    }
  }

  /// An error message.
  public var message: String { details.message }

  /// A Spark error class like `TABLE_OR_VIEW_NOT_FOUND` if the server provides it.
  public var errorClass: String? { details.errorClass }

  /// A SQLSTATE like `42P01` if the server provides it.
  public var sqlState: String? { details.sqlState }

  /// The message parameters of the error class.
  public var messageParameters: [String: String] { details.messageParameters }

  /// The server-side error chain fetched by the `FetchErrorDetails` RPC.
  public var serverErrors: [ServerError] { details.serverErrors }

  /// A multi-line string of the server-side error chain in the JVM stack trace style.
  public var serverStackTrace: String? { details.serverStackTrace }

  private var caseName: String {
    switch self {
    case .catalogNotFound: return "catalogNotFound"
    case .columnNotFound: return "columnNotFound"
    case .dataSourceNotFound: return "dataSourceNotFound"
    case .invalidArgument: return "invalidArgument"
    case .invalidArrowData: return "invalidArrowData"
    case .invalidSessionID: return "invalidSessionID"
    case .invalidState: return "invalidState"
    case .invalidType: return "invalidType"
    case .invalidViewName: return "invalidViewName"
    case .localRelationTooLarge: return "localRelationTooLarge"
    case .outputTypeUnspecified: return "outputTypeUnspecified"
    case .parseSyntaxError: return "parseSyntaxError"
    case .schemaNotFound: return "schemaNotFound"
    case .sessionClosed: return "sessionClosed"
    case .sqlConfNotFound: return "sqlConfNotFound"
    case .tableOrViewAlreadyExists: return "tableOrViewAlreadyExists"
    case .tableOrViewNotFound: return "tableOrViewNotFound"
    case .unsupportedOperation: return "unsupportedOperation"
    }
  }

  public static func == (lhs: SparkConnectError, rhs: SparkConnectError) -> Bool {
    return lhs.caseName == rhs.caseName
  }
}

extension SparkConnectError: CustomStringConvertible {
  public var description: String {
    var result = self.caseName
    if !self.message.isEmpty {
      result += ": \(self.message)"
    } else if let errorClass = self.errorClass {
      result += ": [\(errorClass)]"
    }
    return result
  }
}

/// Supports the compatibility members in `catch` expression patterns
/// like `catch SparkConnectError.TableOrViewNotFound`.
public func ~= (pattern: SparkConnectError, value: any Error) -> Bool {
  guard let error = value as? SparkConnectError else { return false }
  return error == pattern
}
