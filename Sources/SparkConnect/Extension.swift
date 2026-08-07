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

extension String {
  /// Get a `Plan` instance from a string.
  var toSparkConnectPlan: Plan {
    var sql = Spark_Connect_SQL()
    sql.query = self
    var relation = Relation()
    relation.sql = sql
    var plan = Plan()
    plan.opType = Plan.OneOf_OpType.root(relation)
    return plan
  }

  private func toExpression(_ value: Sendable) throws -> Spark_Connect_Expression {
    var expr = Spark_Connect_Expression()
    expr.literal = try ExpressionLiteral(value)
    return expr
  }

  func toSparkConnectPlan(_ posArguments: [Sendable]) throws -> Plan {
    var sql = Spark_Connect_SQL()
    sql.query = self
    sql.posArguments = try posArguments.map { try toExpression($0) }
    var relation = Relation()
    relation.sql = sql
    var plan = Plan()
    plan.opType = Plan.OneOf_OpType.root(relation)
    return plan
  }

  func toSparkConnectPlan(_ namedArguments: [String: Sendable]) throws -> Plan {
    var sql = Spark_Connect_SQL()
    sql.query = self
    sql.namedArguments = try namedArguments.mapValues { try toExpression($0) }
    var relation = Relation()
    relation.sql = sql
    var plan = Plan()
    plan.opType = Plan.OneOf_OpType.root(relation)
    return plan
  }

  /// Get a `UserContext` instance from a string.
  var toUserContext: UserContext {
    var context = UserContext()
    context.userID = self
    context.userName = self
    return context
  }

  /// Get a `KeyValue` instance by using a string as the key.
  var toKeyValue: KeyValue {
    var keyValue = KeyValue()
    keyValue.key = self
    return keyValue
  }

  var toUnresolvedAttribute: UnresolvedAttribute {
    var attribute = UnresolvedAttribute()
    attribute.unparsedIdentifier = self
    return attribute
  }

  var toExpressionString: ExpressionString {
    var expression = ExpressionString()
    expression.expression = self
    return expression
  }

  var toExpression: Spark_Connect_Expression {
    var expressionString = ExpressionString()
    expressionString.expression = self
    var expression = Spark_Connect_Expression()
    expression.expressionString = expressionString
    return expression
  }

  var toExplainMode: ExplainMode {
    let mode =
      switch self {
      case "codegen": ExplainMode.codegen
      case "cost": ExplainMode.cost
      case "extended": ExplainMode.extended
      case "formatted": ExplainMode.formatted
      case "simple": ExplainMode.simple
      default: ExplainMode.simple
      }
    return mode
  }

  var toSaveMode: SaveMode {
    return switch self.lowercased() {
    case "append": SaveMode.append
    case "overwrite": SaveMode.overwrite
    case "error": SaveMode.errorIfExists
    case "errorIfExists": SaveMode.errorIfExists
    case "ignore": SaveMode.ignore
    default: SaveMode.errorIfExists
    }
  }

  var toJoinType: JoinType {
    return switch self.lowercased() {
    case "inner": JoinType.inner
    case "cross": JoinType.cross
    case "outer", "full", "fullouter", "full_outer": JoinType.fullOuter
    case "left", "leftouter", "left_outer": JoinType.leftOuter
    case "right", "rightouter", "right_outer": JoinType.rightOuter
    case "semi", "leftsemi", "left_semi": JoinType.leftSemi
    case "anti", "leftanti", "left_anti": JoinType.leftAnti
    default: JoinType.inner
    }
  }

  var toGroupType: GroupType {
    return switch self.lowercased() {
    case "groupby": .groupby
    case "rollup": .rollup
    case "cube": .cube
    case "pivot": .pivot
    case "groupingsets": .groupingSets
    default: .UNRECOGNIZED(-1)
    }
  }

  var toOutputType: OutputType {
    let mode =
      switch self {
      case "unspecified": OutputType.unspecified
      case "materializedView": OutputType.materializedView
      case "table": OutputType.table
      case "temporaryView": OutputType.temporaryView
      case "sink": OutputType.sink
      default: OutputType.UNRECOGNIZED(-1)
      }
    return mode
  }
}

/// Inside `extension ExpressionLiteral`, the unqualified name `Decimal` refers to
/// the nested `Literal.Decimal` proto type, so resolve Swift's `Decimal` at file scope.
private typealias SwiftDecimal = Decimal

extension ExpressionLiteral {
  /// Create an `ExpressionLiteral` from a Swift value of a supported type.
  init(_ value: Sendable) throws {
    self.init()
    switch value {
    case let value as Bool:
      self.boolean = value
    case let value as Int8:
      self.byte = Int32(value)
    case let value as Int16:
      self.short = Int32(value)
    case let value as Int32:
      self.integer = value
    case let value as Int64:
      self.long = value
    case let value as Int:
      self.long = Int64(value)
    case let value as Float:
      self.float = value
    case let value as Double:
      self.double = value
    case let value as SwiftDecimal:
      var decimal = ExpressionLiteral.Decimal()
      decimal.value = "\(value)"
      self.decimal = decimal
    case let value as String:
      self.string = value
    case let value as Data:
      self.binary = value
    case let value as Date:
      // `Date` is an absolute point in time, so it maps to Spark's `TIMESTAMP`
      // (microseconds since the UNIX epoch), not `DATE`.
      self.timestamp = Int64(value.timeIntervalSince1970 * 1_000_000)
    case let value as LocalTime:
      self.time.nano = value.nanoOfDay
      self.time.precision = 6
    default:
      if case Optional<Any>.none = value as Any {
        var dataType = ProtoDataType()
        dataType.null = ProtoDataType.NULL()
        self.null = dataType
      } else {
        throw SparkConnectError.InvalidType
      }
    }
  }

  /// A Swift value converted from this `ExpressionLiteral`, or nil for null and unsupported types.
  var toSwiftValue: Sendable? {
    switch self.literalType {
    case .boolean(let value): value
    case .byte(let value): Int8(value)
    case .short(let value): Int16(value)
    case .integer(let value): value
    case .long(let value): value
    case .float(let value): value
    case .double(let value): value
    case .decimal(let value): SwiftDecimal(string: value.value)
    case .string(let value): value
    case .binary(let value): value
    // Date in units of days since the UNIX epoch.
    case .date(let value): Date(timeIntervalSince1970: TimeInterval(value) * 86_400)
    // Timestamp in units of microseconds since the UNIX epoch.
    case .timestamp(let value): Date(timeIntervalSince1970: TimeInterval(value) / 1_000_000)
    case .timestampNtz(let value): Date(timeIntervalSince1970: TimeInterval(value) / 1_000_000)
    case .time(let value): LocalTime(nanoOfDay: value.nano)
    default: nil
    }
  }
}

extension [String: String] {
  /// Get an array of `KeyValue` from `[String: String]`.
  var toSparkConnectKeyValue: [KeyValue] {
    var array = [KeyValue]()
    for keyValue in self {
      var kv = KeyValue()
      kv.key = keyValue.key
      kv.value = keyValue.value
      array.append(kv)
    }
    return array
  }
}

extension Data {
  /// Get an `Int32` value from unsafe 4 bytes.
  var int32: Int32 { withUnsafeBytes({ $0.load(as: Int32.self) }) }
}

extension SparkSession: Equatable {
  public static func == (lhs: SparkSession, rhs: SparkSession) -> Bool {
    return lhs.sessionID == rhs.sessionID
  }
}
