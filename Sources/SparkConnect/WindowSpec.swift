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

/// A window specification that defines the partitioning, ordering, and frame boundaries.
///
/// Use the static methods in ``Window`` to create a ``WindowSpec``.
///
/// ```swift
/// df.withColumn("rank", rank().over(Window.partitionBy("dept").orderBy("salary")))
/// ```
public struct WindowSpec: Sendable {
  let partitionSpec: [Spark_Connect_Expression]
  let orderSpec: [Spark_Connect_Expression.SortOrder]
  let frame: Spark_Connect_Expression.Window.WindowFrame?

  init(
    _ partitionSpec: [Spark_Connect_Expression] = [],
    _ orderSpec: [Spark_Connect_Expression.SortOrder] = [],
    _ frame: Spark_Connect_Expression.Window.WindowFrame? = nil
  ) {
    self.partitionSpec = partitionSpec
    self.orderSpec = orderSpec
    self.frame = frame
  }

  /// Defines the partitioning columns in a ``WindowSpec``.
  /// - Parameter colNames: Column names to partition by.
  /// - Returns: A ``WindowSpec``.
  public func partitionBy(_ colNames: String...) -> WindowSpec {
    return partitionBy(colNames.map { Column($0) })
  }

  /// Defines the partitioning columns in a ``WindowSpec``.
  /// - Parameter cols: ``Column``s to partition by.
  /// - Returns: A ``WindowSpec``.
  public func partitionBy(_ cols: Column...) -> WindowSpec {
    return partitionBy(cols)
  }

  func partitionBy(_ cols: [Column]) -> WindowSpec {
    return WindowSpec(cols.map { $0.expr }, orderSpec, frame)
  }

  /// Defines the ordering columns in a ``WindowSpec``.
  /// - Parameter colNames: Column names to order by.
  /// - Returns: A ``WindowSpec``.
  public func orderBy(_ colNames: String...) -> WindowSpec {
    return orderBy(colNames.map { Column($0) })
  }

  /// Defines the ordering columns in a ``WindowSpec``.
  /// - Parameter cols: ``Column``s or sort expressions like `col("a").desc()` to order by.
  /// - Returns: A ``WindowSpec``.
  public func orderBy(_ cols: Column...) -> WindowSpec {
    return orderBy(cols)
  }

  func orderBy(_ cols: [Column]) -> WindowSpec {
    return WindowSpec(partitionSpec, cols.map { toSortOrder($0) }, frame)
  }

  /// Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
  ///
  /// Both `start` and `end` are positions relative to the current row. For example, "0" means
  /// "current row", while "-1" means the row before the current row, and "5" means the fifth row
  /// after the current row.
  ///
  /// We recommend users use ``Window/unboundedPreceding``, ``Window/unboundedFollowing``, and
  /// ``Window/currentRow`` to specify special boundary values, rather than using integral values
  /// directly.
  /// - Parameters:
  ///   - start: A boundary start, inclusive. The frame is unbounded if this is
  ///     ``Window/unboundedPreceding``.
  ///   - end: A boundary end, inclusive. The frame is unbounded if this is
  ///     ``Window/unboundedFollowing``.
  /// - Returns: A ``WindowSpec``.
  public func rowsBetween(_ start: Int64, _ end: Int64) throws -> WindowSpec {
    return withFrame(
      .row, try rowBoundary(start, Window.unboundedPreceding),
      try rowBoundary(end, Window.unboundedFollowing))
  }

  /// Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
  ///
  /// Both `start` and `end` are relative to the current row based on the actual value of the
  /// ORDER BY expression(s). For example, "0" means "current row", while "-1" means one off
  /// before the current row, and "5" means the five off after the current row.
  ///
  /// We recommend users use ``Window/unboundedPreceding``, ``Window/unboundedFollowing``, and
  /// ``Window/currentRow`` to specify special boundary values, rather than using integral values
  /// directly.
  /// - Parameters:
  ///   - start: A boundary start, inclusive. The frame is unbounded if this is
  ///     ``Window/unboundedPreceding``.
  ///   - end: A boundary end, inclusive. The frame is unbounded if this is
  ///     ``Window/unboundedFollowing``.
  /// - Returns: A ``WindowSpec``.
  public func rangeBetween(_ start: Int64, _ end: Int64) -> WindowSpec {
    return withFrame(
      .range, rangeBoundary(start, Window.unboundedPreceding),
      rangeBoundary(end, Window.unboundedFollowing))
  }

  private func withFrame(
    _ frameType: Spark_Connect_Expression.Window.WindowFrame.FrameType,
    _ lower: Spark_Connect_Expression.Window.WindowFrame.FrameBoundary,
    _ upper: Spark_Connect_Expression.Window.WindowFrame.FrameBoundary
  ) -> WindowSpec {
    var frame = Spark_Connect_Expression.Window.WindowFrame()
    frame.frameType = frameType
    frame.lower = lower
    frame.upper = upper
    return WindowSpec(partitionSpec, orderSpec, frame)
  }

  /// A row-based boundary is an offset of the row position, so it must fit in `Int32` like Scala.
  private func rowBoundary(
    _ value: Int64, _ unbounded: Int64
  ) throws -> Spark_Connect_Expression.Window.WindowFrame.FrameBoundary {
    var boundary = Spark_Connect_Expression.Window.WindowFrame.FrameBoundary()
    switch value {
    case Window.currentRow:
      boundary.currentRow = true
    case unbounded:
      boundary.unbounded = true
    case Int64(Int32.min)...Int64(Int32.max):
      boundary.value = lit(Int32(value)).expr
    default:
      throw SparkConnectError.InvalidArgument
    }
    return boundary
  }

  private func rangeBoundary(
    _ value: Int64, _ unbounded: Int64
  ) -> Spark_Connect_Expression.Window.WindowFrame.FrameBoundary {
    var boundary = Spark_Connect_Expression.Window.WindowFrame.FrameBoundary()
    switch value {
    case Window.currentRow:
      boundary.currentRow = true
    case unbounded:
      boundary.unbounded = true
    default:
      boundary.value = lit(value).expr
    }
    return boundary
  }

  private func toSortOrder(_ col: Column) -> Spark_Connect_Expression.SortOrder {
    if case .sortOrder(let sortOrder) = col.expr.exprType {
      return sortOrder
    }
    var sortOrder = Spark_Connect_Expression.SortOrder()
    sortOrder.child = col.expr
    sortOrder.direction = .ascending
    sortOrder.nullOrdering = .sortNullsFirst
    return sortOrder
  }
}
