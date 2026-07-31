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

/// A column expression that can be used in ``DataFrame`` operations.
///
/// A ``Column`` wraps a Spark Connect `Expression` and is created via
/// ``col(_:)``, `lit`, or other functions.
///
/// ```swift
/// let df2 = df.select(col("name"), col("age").cast("long").alias("age_long"))
/// ```
public struct Column: Sendable {
  let expr: Spark_Connect_Expression

  init(_ expr: Spark_Connect_Expression) {
    self.expr = expr
  }

  /// Creates a ``Column`` referencing the given column name.
  ///
  /// `"*"` and names ending with `".*"` are expanded as stars.
  /// - Parameter name: A column name.
  public init(_ name: String) {
    var expr = Spark_Connect_Expression()
    if name == "*" {
      expr.unresolvedStar = Spark_Connect_Expression.UnresolvedStar()
    } else if name.hasSuffix(".*") {
      var star = Spark_Connect_Expression.UnresolvedStar()
      star.unparsedTarget = name
      expr.unresolvedStar = star
    } else {
      expr.unresolvedAttribute = name.toUnresolvedAttribute
    }
    self.expr = expr
  }

  /// Returns this column aliased with the given name.
  /// - Parameter name: An alias name.
  /// - Returns: A ``Column`` with the alias applied.
  public func alias(_ name: String) -> Column {
    var alias = Spark_Connect_Expression.Alias()
    alias.expr = self.expr
    alias.name = [name]
    var expr = Spark_Connect_Expression()
    expr.alias = alias
    return Column(expr)
  }

  /// Returns a sort expression based on the ascending order of this column.
  /// - Returns: A ``Column`` with ascending sort order.
  public func asc() -> Column {
    return sortOrder(.ascending, .sortNullsFirst)
  }

  /// Returns a sort expression based on the descending order of this column.
  /// - Returns: A ``Column`` with descending sort order.
  public func desc() -> Column {
    return sortOrder(.descending, .sortNullsLast)
  }

  /// Casts this column to a different data type.
  /// - Parameter to: A data type name like `"int"`, `"long"`, or `"string"`.
  /// - Returns: A ``Column`` casted to the given type.
  public func cast(_ to: String) -> Column {
    var cast = Spark_Connect_Expression.Cast()
    cast.expr = self.expr
    cast.typeStr = to
    var expr = Spark_Connect_Expression()
    expr.cast = cast
    return Column(expr)
  }

  private func sortOrder(
    _ direction: Spark_Connect_Expression.SortOrder.SortDirection,
    _ nullOrdering: Spark_Connect_Expression.SortOrder.NullOrdering
  ) -> Column {
    var sortOrder = Spark_Connect_Expression.SortOrder()
    sortOrder.child = self.expr
    sortOrder.direction = direction
    sortOrder.nullOrdering = nullOrdering
    var expr = Spark_Connect_Expression()
    expr.sortOrder = sortOrder
    return Column(expr)
  }

  private static func fn(_ name: String, _ args: Column...) -> Column {
    var function = Spark_Connect_Expression.UnresolvedFunction()
    function.functionName = name
    function.arguments = args.map { $0.expr }
    var expr = Spark_Connect_Expression()
    expr.unresolvedFunction = function
    return Column(expr)
  }

  // MARK: - Comparison operators

  /// Returns an equality test expression. Note that this returns a ``Column``
  /// expression evaluated by Spark, not a `Bool`.
  ///
  /// ```swift
  /// df.filter(col("name") == lit("Alice"))
  /// ```
  public static func == (lhs: Column, rhs: Column) -> Column {
    return fn("=", lhs, rhs)
  }

  /// Returns an inequality test expression.
  public static func != (lhs: Column, rhs: Column) -> Column {
    return fn("!", fn("=", lhs, rhs))
  }

  /// Returns a less-than test expression.
  public static func < (lhs: Column, rhs: Column) -> Column {
    return fn("<", lhs, rhs)
  }

  /// Returns a less-than-or-equal test expression.
  public static func <= (lhs: Column, rhs: Column) -> Column {
    return fn("<=", lhs, rhs)
  }

  /// Returns a greater-than test expression.
  public static func > (lhs: Column, rhs: Column) -> Column {
    return fn(">", lhs, rhs)
  }

  /// Returns a greater-than-or-equal test expression.
  public static func >= (lhs: Column, rhs: Column) -> Column {
    return fn(">=", lhs, rhs)
  }

  // MARK: - Logical operators

  /// Returns a logical AND expression.
  ///
  /// ```swift
  /// df.filter(col("age") > lit(21) && col("name") == lit("Alice"))
  /// ```
  public static func && (lhs: Column, rhs: Column) -> Column {
    return fn("and", lhs, rhs)
  }

  /// Returns a logical OR expression.
  public static func || (lhs: Column, rhs: Column) -> Column {
    return fn("or", lhs, rhs)
  }

  /// Returns a logical NOT expression.
  public static prefix func ! (col: Column) -> Column {
    return fn("!", col)
  }

  // MARK: - Arithmetic operators

  /// Returns an addition expression.
  public static func + (lhs: Column, rhs: Column) -> Column {
    return fn("+", lhs, rhs)
  }

  /// Returns a subtraction expression.
  public static func - (lhs: Column, rhs: Column) -> Column {
    return fn("-", lhs, rhs)
  }

  /// Returns a multiplication expression.
  public static func * (lhs: Column, rhs: Column) -> Column {
    return fn("*", lhs, rhs)
  }

  /// Returns a division expression.
  public static func / (lhs: Column, rhs: Column) -> Column {
    return fn("/", lhs, rhs)
  }

  /// Returns a modulo expression.
  public static func % (lhs: Column, rhs: Column) -> Column {
    return fn("%", lhs, rhs)
  }

  /// Returns a negation expression.
  public static prefix func - (col: Column) -> Column {
    return fn("negative", col)
  }
}
