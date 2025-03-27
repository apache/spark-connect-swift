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
