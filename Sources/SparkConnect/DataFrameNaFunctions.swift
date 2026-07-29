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

/// Functionality for working with missing data in ``DataFrame``s.
///
/// Use ``DataFrame/na`` to access this. It mirrors PySpark's `DataFrameNaFunctions`
/// (`df.na.fill`, `df.na.drop`, `df.na.replace`).
public actor DataFrameNaFunctions: Sendable {
  let df: DataFrame

  init(df: DataFrame) {
    self.df = df
  }

  // MARK: - Fill

  /// Returns a new ``DataFrame`` that replaces null values in boolean columns with `value`.
  /// - Parameters:
  ///   - value: The replacement value.
  ///   - cols: Column names to consider. When empty, all type-compatible columns are considered.
  /// - Returns: A ``DataFrame``.
  public func fill(_ value: Bool, _ cols: [String] = []) async -> DataFrame {
    var literal = ExpressionLiteral()
    literal.boolean = value
    return await transform { SparkConnectClient.getNAFill($0, cols, [literal]) }
  }

  /// Returns a new ``DataFrame`` that replaces null values in numeric columns with `value`.
  /// - Parameters:
  ///   - value: The replacement value.
  ///   - cols: Column names to consider. When empty, all type-compatible columns are considered.
  /// - Returns: A ``DataFrame``.
  public func fill(_ value: Int, _ cols: [String] = []) async -> DataFrame {
    var literal = ExpressionLiteral()
    literal.long = Int64(value)
    return await transform { SparkConnectClient.getNAFill($0, cols, [literal]) }
  }

  /// Returns a new ``DataFrame`` that replaces null values in numeric columns with `value`.
  /// - Parameters:
  ///   - value: The replacement value.
  ///   - cols: Column names to consider. When empty, all type-compatible columns are considered.
  /// - Returns: A ``DataFrame``.
  public func fill(_ value: Double, _ cols: [String] = []) async -> DataFrame {
    var literal = ExpressionLiteral()
    literal.double = value
    return await transform { SparkConnectClient.getNAFill($0, cols, [literal]) }
  }

  /// Returns a new ``DataFrame`` that replaces null values in string columns with `value`.
  /// - Parameters:
  ///   - value: The replacement value.
  ///   - cols: Column names to consider. When empty, all type-compatible columns are considered.
  /// - Returns: A ``DataFrame``.
  public func fill(_ value: String, _ cols: [String] = []) async -> DataFrame {
    var literal = ExpressionLiteral()
    literal.string = value
    return await transform { SparkConnectClient.getNAFill($0, cols, [literal]) }
  }

  /// Returns a new ``DataFrame`` that replaces null values, using the column name to replacement
  /// value mapping. The replacement values must be of type `Bool`, `Int64`, `Double`, or `String`.
  /// - Parameter valueMap: A dictionary of column name and replacement value.
  /// - Returns: A ``DataFrame``.
  public func fill(_ valueMap: [String: Sendable]) async -> DataFrame {
    let cols = Array(valueMap.keys)
    let values = cols.map { fillLiteral(valueMap[$0]!) }
    return await transform { SparkConnectClient.getNAFill($0, cols, values) }
  }

  // MARK: - Drop

  /// Returns a new ``DataFrame`` that drops rows containing null values.
  /// - Parameters:
  ///   - how: `any` (default) drops a row if it contains any null value, `all` drops a row only
  ///   if every considered value is null.
  ///   - cols: Column names to consider. When empty, all columns are considered.
  /// - Returns: A ``DataFrame``.
  public func drop(how: String = "any", _ cols: [String] = []) async -> DataFrame {
    let minNonNulls: Int32? = how.lowercased() == "all" ? 1 : nil
    return await transform { SparkConnectClient.getNADrop($0, cols, minNonNulls) }
  }

  /// Returns a new ``DataFrame`` that drops rows containing less than `minNonNulls` non-null values.
  /// - Parameters:
  ///   - minNonNulls: The minimum number of non-null values a row must contain to be kept.
  ///   - cols: Column names to consider. When empty, all columns are considered.
  /// - Returns: A ``DataFrame``.
  public func drop(minNonNulls: Int32, _ cols: [String] = []) async -> DataFrame {
    return await transform { SparkConnectClient.getNADrop($0, cols, minNonNulls) }
  }

  // MARK: - Replace

  /// Returns a new ``DataFrame`` that replaces values in the given column using the replacement
  /// mapping. The values must be of type `Bool`, `Double`, or `String`.
  /// - Parameters:
  ///   - col: A column name, or `*` to consider all type-compatible columns.
  ///   - replacement: A mapping of old value to new value.
  /// - Returns: A ``DataFrame``.
  public func replace<T: Sendable & Hashable>(
    _ col: String, _ replacement: [T: T]
  ) async -> DataFrame {
    return await replace(col == "*" ? [] : [col], replacement)
  }

  /// Returns a new ``DataFrame`` that replaces values in the given columns using the replacement
  /// mapping. The values must be of type `Bool`, `Double`, or `String`.
  /// - Parameters:
  ///   - cols: Column names to consider. When empty, all type-compatible columns are considered.
  ///   - replacement: A mapping of old value to new value.
  /// - Returns: A ``DataFrame``.
  public func replace<T: Sendable & Hashable>(
    _ cols: [String], _ replacement: [T: T]
  ) async -> DataFrame {
    let replacements = replacement.map { (replaceLiteral($0.key), replaceLiteral($0.value)) }
    return await transform { SparkConnectClient.getNAReplace($0, cols, replacements) }
  }

  // MARK: - Helpers

  /// Builds a new ``DataFrame`` from this ``DataFrame``'s plan using the given plan builder.
  private func transform(_ f: (Relation) -> Plan) async -> DataFrame {
    let plan = await df.getPlan() as! Plan
    return DataFrame(spark: await df.spark, plan: f(plan.root))
  }

  /// Converts a `fill` value to an ``ExpressionLiteral`` (`bool`, `long`, `double`, `string`).
  private func fillLiteral(_ value: Sendable) -> ExpressionLiteral {
    var literal = ExpressionLiteral()
    switch value {
    case let value as Bool:
      literal.boolean = value
    case let value as Int:
      literal.long = Int64(value)
    case let value as Int32:
      literal.long = Int64(value)
    case let value as Int64:
      literal.long = value
    case let value as Float:
      literal.double = Double(value)
    case let value as Double:
      literal.double = value
    case let value as String:
      literal.string = value
    default:
      literal.string = value as! String
    }
    return literal
  }

  /// Converts a `replace` value to an ``ExpressionLiteral`` (`bool`, `double`, `string`).
  /// Integers are represented as `double` because `replace` does not support `long`.
  private func replaceLiteral(_ value: Sendable) -> ExpressionLiteral {
    var literal = ExpressionLiteral()
    switch value {
    case let value as Bool:
      literal.boolean = value
    case let value as Int:
      literal.double = Double(value)
    case let value as Int32:
      literal.double = Double(value)
    case let value as Int64:
      literal.double = Double(value)
    case let value as Float:
      literal.double = Double(value)
    case let value as Double:
      literal.double = value
    case let value as String:
      literal.string = value
    default:
      literal.string = value as! String
    }
    return literal
  }
}

extension DataFrame {
  /// Returns a ``DataFrameNaFunctions`` for working with missing data.
  public var na: DataFrameNaFunctions {
    DataFrameNaFunctions(df: self)
  }
}
