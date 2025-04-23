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

import Foundation

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

  var toExplainMode: ExplainMode {
    let mode = switch self {
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

extension YearMonthInterval {
  func fieldToString(_ field: Int32) throws -> String {
    return switch field {
    case 0: "year"
    case 1: "month"
    default:
      throw SparkConnectError.InvalidTypeException
    }
  }

  func toString() throws -> String {
    let startFieldName = try fieldToString(self.startField)
    let endFieldName = try fieldToString(self.endField)
    let interval = if startFieldName == endFieldName {
      "interval \(startFieldName)"
    } else if startFieldName < endFieldName {
      "interval \(startFieldName) to \(endFieldName)"
    } else {
      throw SparkConnectError.InvalidTypeException
    }
    return interval
  }
}

extension DayTimeInterval {
  func fieldToString(_ field: Int32) throws -> String {
    return switch field {
    case 0: "day"
    case 1: "hour"
    case 2: "minute"
    case 3: "second"
    default:
      throw SparkConnectError.InvalidTypeException
    }
  }

  func toString() throws -> String {
    let startFieldName = try fieldToString(self.startField)
    let endFieldName = try fieldToString(self.endField)
    let interval = if startFieldName == endFieldName {
      "interval \(startFieldName)"
    } else if startFieldName < endFieldName {
      "interval \(startFieldName) to \(endFieldName)"
    } else {
      throw SparkConnectError.InvalidTypeException
    }
    return interval
  }
}

extension MapType {
  func toString() throws -> String {
    return "map<\(try self.keyType.simpleString),\(try self.valueType.simpleString)>"
  }
}

extension StructType {
  func toString() throws -> String {
    let fieldTypes = try fields.map { "\($0.name):\(try $0.dataType.simpleString)" }
    return "struct<\(fieldTypes.joined(separator: ","))>"
  }
}

extension DataType {
  var simpleString: String {
    get throws {
      return switch self.kind {
      case .null:
        "void"
      case .binary:
        "binary"
      case .boolean:
        "boolean"
      case .byte:
        "tinyint"
      case .short:
        "smallint"
      case .integer:
        "int"
      case .long:
        "bigint"
      case .float:
        "float"
      case .double:
        "double"
      case .decimal:
        "decimal(\(self.decimal.precision),\(self.decimal.scale))"
      case .string:
        "string"
      case .char:
        "char"
      case .varChar:
        "varchar"
      case .date:
        "date"
      case .timestamp:
        "timestamp"
      case .timestampNtz:
        "timestamp_ntz"
      case .calendarInterval:
        "interval"
      case .yearMonthInterval:
        try self.yearMonthInterval.toString()
      case .dayTimeInterval:
        try self.dayTimeInterval.toString()
      case .array:
        "array<\(try self.array.elementType.simpleString)>"
      case .struct:
        try self.struct.toString()
      case .map:
        try self.map.toString()
      case .variant:
        "variant"
      case .udt:
        self.udt.type
      case .unparsed:
        self.unparsed.dataTypeString
      default:
        throw SparkConnectError.InvalidTypeException
      }
    }
  }
}
