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

/// Returns a ``Column`` based on the given column name.
/// - Parameter name: A column name.
/// - Returns: A ``Column``.
public func col(_ name: String) -> Column {
  return Column(name)
}

/// Returns a ``Column`` based on the given column name. This is an alias of ``col(_:)``.
/// - Parameter name: A column name.
/// - Returns: A ``Column``.
public func column(_ name: String) -> Column {
  return Column(name)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: Bool) -> Column {
  var literal = ExpressionLiteral()
  literal.boolean = value
  return litColumn(literal)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: Int8) -> Column {
  var literal = ExpressionLiteral()
  literal.byte = Int32(value)
  return litColumn(literal)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: Int16) -> Column {
  var literal = ExpressionLiteral()
  literal.short = Int32(value)
  return litColumn(literal)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: Int32) -> Column {
  var literal = ExpressionLiteral()
  literal.integer = value
  return litColumn(literal)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: Int64) -> Column {
  var literal = ExpressionLiteral()
  literal.long = value
  return litColumn(literal)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: Int) -> Column {
  var literal = ExpressionLiteral()
  literal.long = Int64(value)
  return litColumn(literal)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: Float) -> Column {
  var literal = ExpressionLiteral()
  literal.float = value
  return litColumn(literal)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: Double) -> Column {
  var literal = ExpressionLiteral()
  literal.double = value
  return litColumn(literal)
}

/// Creates a ``Column`` of literal value.
/// - Parameter value: A literal value.
/// - Returns: A ``Column``.
public func lit(_ value: String) -> Column {
  var literal = ExpressionLiteral()
  literal.string = value
  return litColumn(literal)
}

/// A type that can be used as a literal operand of ``Column`` operators.
///
/// This allows Swift literals to be mixed with ``Column`` expressions directly
/// without wrapping them in `lit`:
///
/// ```swift
/// df.filter(col("age") > 21 && col("name") == "Alice")
/// ```
public protocol SparkLiteral {
  /// A literal ``Column`` representation of this value.
  var toLiteralColumn: Column { get }
}

extension Bool: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

extension Int8: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

extension Int16: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

extension Int32: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

extension Int64: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

extension Int: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

extension Float: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

extension Double: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

extension String: SparkLiteral {
  public var toLiteralColumn: Column { lit(self) }
}

/// Returns a sort expression based on the ascending order of the given column name.
/// - Parameter name: A column name.
/// - Returns: A ``Column`` with ascending sort order.
public func asc(_ name: String) -> Column {
  return Column(name).asc()
}

/// Returns a sort expression based on the descending order of the given column name.
/// - Parameter name: A column name.
/// - Returns: A ``Column`` with descending sort order.
public func desc(_ name: String) -> Column {
  return Column(name).desc()
}

/// Parses the expression string into the column that it represents.
///
/// ```swift
/// df.select(expr("id + 1"))
/// ```
///
/// - Parameter sqlExpr: A SQL expression string.
/// - Returns: A ``Column``.
public func expr(_ sqlExpr: String) -> Column {
  return Column(sqlExpr.toExpression)
}

/// Returns the number of items in a group.
/// - Parameter col: A ``Column`` to count.
/// - Returns: A ``Column``.
public func count(_ col: Column) -> Column {
  return fn("count", col)
}

/// Returns the sum of all values in the expression.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func sum(_ col: Column) -> Column {
  return fn("sum", col)
}

/// Returns the average of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func avg(_ col: Column) -> Column {
  return fn("avg", col)
}

/// Returns the average of the values in a group. This is an alias of ``avg(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func mean(_ col: Column) -> Column {
  return fn("avg", col)
}

/// Returns the minimum value of the expression in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func min(_ col: Column) -> Column {
  return fn("min", col)
}

/// Returns the maximum value of the expression in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func max(_ col: Column) -> Column {
  return fn("max", col)
}

// MARK: - Condition functions

/// Evaluates a list of conditions and returns one of multiple possible result expressions.
/// If `otherwise` is not defined at the end, null is returned for unmatched conditions.
///
/// ```swift
/// df.select(when(col("age") > 21, "adult").otherwise("minor").alias("group"))
/// ```
/// - Parameters:
///   - condition: A condition ``Column``.
///   - value: A value ``Column`` to return when the condition is true.
/// - Returns: A ``Column``.
public func when(_ condition: Column, _ value: Column) -> Column {
  return fn("when", condition, value)
}

/// Evaluates a list of conditions and returns one of multiple possible result expressions.
/// If `otherwise` is not defined at the end, null is returned for unmatched conditions.
/// - Parameters:
///   - condition: A condition ``Column``.
///   - value: A literal value to return when the condition is true.
/// - Returns: A ``Column``.
public func when(_ condition: Column, _ value: some SparkLiteral) -> Column {
  return when(condition, value.toLiteralColumn)
}

func fn(_ name: String, _ args: Column...) -> Column {
  var function = Spark_Connect_Expression.UnresolvedFunction()
  function.functionName = name
  function.arguments = args.map { $0.expr }
  var expr = Spark_Connect_Expression()
  expr.unresolvedFunction = function
  return Column(expr)
}

func fn(_ name: String, _ args: [Column], isDistinct: Bool = false) -> Column {
  var function = Spark_Connect_Expression.UnresolvedFunction()
  function.functionName = name
  function.arguments = args.map { $0.expr }
  function.isDistinct = isDistinct
  var expr = Spark_Connect_Expression()
  expr.unresolvedFunction = function
  return Column(expr)
}

private func litColumn(_ literal: ExpressionLiteral) -> Column {
  var expr = Spark_Connect_Expression()
  expr.literal = literal
  return Column(expr)
}
