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

  // MARK: - Predicates

  /// Returns an expression that is true if this column is null.
  ///
  /// ```swift
  /// df.filter(col("name").isNull())
  /// ```
  /// - Returns: A ``Column`` testing for null.
  public func isNull() -> Column {
    return Column.fn("isnull", self)
  }

  /// Returns an expression that is true if this column is not null.
  ///
  /// ```swift
  /// df.filter(col("name").isNotNull())
  /// ```
  /// - Returns: A ``Column`` testing for non-null.
  public func isNotNull() -> Column {
    return Column.fn("isnotnull", self)
  }

  /// Returns an expression that is true if this column's value is contained in
  /// the given values.
  ///
  /// ```swift
  /// df.filter(col("age").isin(20, 30))
  /// ```
  /// - Parameter values: Literal values to test against.
  /// - Returns: A ``Column`` testing for membership.
  public func isin(_ values: any SparkLiteral...) -> Column {
    return Column.fn("in", [self] + values.map { $0.toLiteralColumn })
  }

  /// Returns an expression that is true if this column is between the given
  /// lower and upper bounds, inclusive.
  ///
  /// ```swift
  /// df.filter(col("age").between(20, 30))
  /// ```
  /// - Parameters:
  ///   - lower: A lower bound.
  ///   - upper: An upper bound.
  /// - Returns: A ``Column`` testing for the range.
  public func between(_ lower: Column, _ upper: Column) -> Column {
    return (self >= lower) && (self <= upper)
  }

  /// Returns an expression that is true if this column is between the given
  /// lower and upper bounds, inclusive.
  public func between(_ lower: Column, _ upper: some SparkLiteral) -> Column {
    return (self >= lower) && (self <= upper)
  }

  /// Returns an expression that is true if this column is between the given
  /// lower and upper bounds, inclusive.
  public func between(_ lower: some SparkLiteral, _ upper: Column) -> Column {
    return (self >= lower) && (self <= upper)
  }

  /// Returns an expression that is true if this column is between the given
  /// lower and upper bounds, inclusive.
  public func between(_ lower: some SparkLiteral, _ upper: some SparkLiteral) -> Column {
    return (self >= lower) && (self <= upper)
  }

  /// Returns a null-safe equality test expression. Unlike `==`, this returns
  /// `true` when both sides are null.
  ///
  /// ```swift
  /// df.filter(col("name").eqNullSafe(col("nickname")))
  /// ```
  /// - Parameter other: A ``Column`` to compare with.
  /// - Returns: A ``Column`` testing for null-safe equality.
  public func eqNullSafe(_ other: Column) -> Column {
    return Column.fn("<=>", self, other)
  }

  /// Returns a null-safe equality test expression against a literal value.
  public func eqNullSafe(_ other: some SparkLiteral) -> Column {
    return eqNullSafe(other.toLiteralColumn)
  }

  // MARK: - String methods

  /// Returns a SQL `LIKE` expression.
  ///
  /// ```swift
  /// df.filter(col("name").like("Al%"))
  /// ```
  /// - Parameter pattern: A SQL LIKE pattern.
  /// - Returns: A ``Column`` testing for the pattern match.
  public func like(_ pattern: String) -> Column {
    return Column.fn("like", self, pattern.toLiteralColumn)
  }

  /// Returns a SQL `RLIKE` expression (LIKE with regex).
  ///
  /// ```swift
  /// df.filter(col("name").rlike("^Al.*"))
  /// ```
  /// - Parameter pattern: A regular expression.
  /// - Returns: A ``Column`` testing for the regex match.
  public func rlike(_ pattern: String) -> Column {
    return Column.fn("rlike", self, pattern.toLiteralColumn)
  }

  /// Returns a SQL `ILIKE` expression (case insensitive LIKE).
  ///
  /// ```swift
  /// df.filter(col("name").ilike("al%"))
  /// ```
  /// - Parameter pattern: A SQL LIKE pattern.
  /// - Returns: A ``Column`` testing for the case-insensitive pattern match.
  public func ilike(_ pattern: String) -> Column {
    return Column.fn("ilike", self, pattern.toLiteralColumn)
  }

  /// Returns an expression that is true if this column contains the other value.
  ///
  /// ```swift
  /// df.filter(col("name").contains("li"))
  /// ```
  /// - Parameter other: A ``Column`` to search for.
  /// - Returns: A ``Column`` testing for containment.
  public func contains(_ other: Column) -> Column {
    return Column.fn("contains", self, other)
  }

  /// Returns an expression that is true if this column contains the literal value.
  public func contains(_ other: some SparkLiteral) -> Column {
    return contains(other.toLiteralColumn)
  }

  /// Returns an expression that is true if this column starts with the other value.
  ///
  /// ```swift
  /// df.filter(col("name").startsWith("Al"))
  /// ```
  /// - Parameter other: A ``Column`` prefix.
  /// - Returns: A ``Column`` testing for the prefix match.
  public func startsWith(_ other: Column) -> Column {
    return Column.fn("startswith", self, other)
  }

  /// Returns an expression that is true if this column starts with the literal string.
  public func startsWith(_ literal: String) -> Column {
    return startsWith(literal.toLiteralColumn)
  }

  /// Returns an expression that is true if this column ends with the other value.
  ///
  /// ```swift
  /// df.filter(col("name").endsWith("ce"))
  /// ```
  /// - Parameter other: A ``Column`` suffix.
  /// - Returns: A ``Column`` testing for the suffix match.
  public func endsWith(_ other: Column) -> Column {
    return Column.fn("endswith", self, other)
  }

  /// Returns an expression that is true if this column ends with the literal string.
  public func endsWith(_ literal: String) -> Column {
    return endsWith(literal.toLiteralColumn)
  }

  /// Returns a substring expression.
  ///
  /// ```swift
  /// df.select(col("name").substr(lit(1), lit(3)))
  /// ```
  /// - Parameters:
  ///   - startPos: A starting position (1-based).
  ///   - len: A substring length.
  /// - Returns: A ``Column`` of the substring.
  public func substr(_ startPos: Column, _ len: Column) -> Column {
    return Column.fn("substr", self, startPos, len)
  }

  /// Returns a substring expression.
  ///
  /// ```swift
  /// df.select(col("name").substr(1, 3))
  /// ```
  public func substr(_ startPos: Int, _ len: Int) -> Column {
    return substr(startPos.toLiteralColumn, len.toLiteralColumn)
  }

  // MARK: - Extraction

  /// Returns an expression that gets an item at the given position out of an
  /// array, or gets a value by the given key out of a map.
  ///
  /// ```swift
  /// df.select(col("arr").getItem(0), col("map").getItem("key"))
  /// ```
  /// - Parameter key: An array position (0-based) or a map key.
  /// - Returns: A ``Column`` of the extracted value.
  public func getItem(_ key: some SparkLiteral) -> Column {
    return extractValue(key.toLiteralColumn)
  }

  /// Returns an expression that gets a field by the given name in a struct.
  ///
  /// ```swift
  /// df.select(col("struct").getField("a"))
  /// ```
  /// - Parameter name: A field name.
  /// - Returns: A ``Column`` of the extracted field.
  public func getField(_ name: String) -> Column {
    return extractValue(name.toLiteralColumn)
  }

  private func extractValue(_ extraction: Column) -> Column {
    var extractValue = Spark_Connect_Expression.UnresolvedExtractValue()
    extractValue.child = self.expr
    extractValue.extraction = extraction.expr
    var expr = Spark_Connect_Expression()
    expr.unresolvedExtractValue = extractValue
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
    return fn(name, args)
  }

  private static func fn(_ name: String, _ args: [Column]) -> Column {
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

  // MARK: - Literal operand overloads

  /// Returns an equality test expression against a literal value.
  ///
  /// ```swift
  /// df.filter(col("name") == "Alice")
  /// ```
  public static func == (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs == rhs.toLiteralColumn
  }

  /// Returns an equality test expression against a literal value.
  public static func == (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn == rhs
  }

  /// Returns an inequality test expression against a literal value.
  public static func != (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs != rhs.toLiteralColumn
  }

  /// Returns an inequality test expression against a literal value.
  public static func != (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn != rhs
  }

  /// Returns a less-than test expression against a literal value.
  public static func < (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs < rhs.toLiteralColumn
  }

  /// Returns a less-than test expression against a literal value.
  public static func < (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn < rhs
  }

  /// Returns a less-than-or-equal test expression against a literal value.
  public static func <= (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs <= rhs.toLiteralColumn
  }

  /// Returns a less-than-or-equal test expression against a literal value.
  public static func <= (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn <= rhs
  }

  /// Returns a greater-than test expression against a literal value.
  public static func > (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs > rhs.toLiteralColumn
  }

  /// Returns a greater-than test expression against a literal value.
  public static func > (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn > rhs
  }

  /// Returns a greater-than-or-equal test expression against a literal value.
  public static func >= (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs >= rhs.toLiteralColumn
  }

  /// Returns a greater-than-or-equal test expression against a literal value.
  public static func >= (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn >= rhs
  }

  /// Returns a logical AND expression with a literal value.
  public static func && (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs && rhs.toLiteralColumn
  }

  /// Returns a logical AND expression with a literal value.
  public static func && (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn && rhs
  }

  /// Returns a logical OR expression with a literal value.
  public static func || (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs || rhs.toLiteralColumn
  }

  /// Returns a logical OR expression with a literal value.
  public static func || (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn || rhs
  }

  /// Returns an addition expression with a literal value.
  public static func + (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs + rhs.toLiteralColumn
  }

  /// Returns an addition expression with a literal value.
  public static func + (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn + rhs
  }

  /// Returns a subtraction expression with a literal value.
  public static func - (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs - rhs.toLiteralColumn
  }

  /// Returns a subtraction expression with a literal value.
  public static func - (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn - rhs
  }

  /// Returns a multiplication expression with a literal value.
  public static func * (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs * rhs.toLiteralColumn
  }

  /// Returns a multiplication expression with a literal value.
  public static func * (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn * rhs
  }

  /// Returns a division expression with a literal value.
  public static func / (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs / rhs.toLiteralColumn
  }

  /// Returns a division expression with a literal value.
  public static func / (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn / rhs
  }

  /// Returns a modulo expression with a literal value.
  public static func % (lhs: Column, rhs: some SparkLiteral) -> Column {
    return lhs % rhs.toLiteralColumn
  }

  /// Returns a modulo expression with a literal value.
  public static func % (lhs: some SparkLiteral, rhs: Column) -> Column {
    return lhs.toLiteralColumn % rhs
  }
}
