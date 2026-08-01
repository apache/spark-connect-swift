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

/// Utility functions for defining window in ``DataFrame``s.
///
/// ```swift
/// // PARTITION BY country ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
/// Window.partitionBy("country").orderBy("date")
///   .rowsBetween(Window.unboundedPreceding, Window.currentRow)
///
/// // PARTITION BY country ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
/// Window.partitionBy("country").orderBy("date").rowsBetween(-3, 3)
/// ```
///
/// When ordering is not defined, an unbounded window frame (rowFrame, unboundedPreceding,
/// unboundedFollowing) is used by default. When ordering is defined, a growing window frame
/// (rangeFrame, unboundedPreceding, currentRow) is used by default.
public struct Window: Sendable {
  private init() {}

  /// Value representing the first row in the partition, equivalent to "UNBOUNDED PRECEDING" in
  /// SQL. This can be used to specify the frame boundaries.
  public static let unboundedPreceding: Int64 = Int64.min

  /// Value representing the last row in the partition, equivalent to "UNBOUNDED FOLLOWING" in
  /// SQL. This can be used to specify the frame boundaries.
  public static let unboundedFollowing: Int64 = Int64.max

  /// Value representing the current row. This can be used to specify the frame boundaries.
  public static let currentRow: Int64 = 0

  /// Creates a ``WindowSpec`` with the partitioning defined.
  /// - Parameter colNames: Column names to partition by.
  /// - Returns: A ``WindowSpec``.
  public static func partitionBy(_ colNames: String...) -> WindowSpec {
    return WindowSpec().partitionBy(colNames.map { Column($0) })
  }

  /// Creates a ``WindowSpec`` with the partitioning defined.
  /// - Parameter cols: ``Column``s to partition by.
  /// - Returns: A ``WindowSpec``.
  public static func partitionBy(_ cols: Column...) -> WindowSpec {
    return WindowSpec().partitionBy(cols)
  }

  /// Creates a ``WindowSpec`` with the ordering defined.
  /// - Parameter colNames: Column names to order by.
  /// - Returns: A ``WindowSpec``.
  public static func orderBy(_ colNames: String...) -> WindowSpec {
    return WindowSpec().orderBy(colNames.map { Column($0) })
  }

  /// Creates a ``WindowSpec`` with the ordering defined.
  /// - Parameter cols: ``Column``s or sort expressions like `col("a").desc()` to order by.
  /// - Returns: A ``WindowSpec``.
  public static func orderBy(_ cols: Column...) -> WindowSpec {
    return WindowSpec().orderBy(cols)
  }

  /// Creates a ``WindowSpec`` with the frame boundaries defined, from `start` (inclusive) to
  /// `end` (inclusive). See ``WindowSpec/rowsBetween(_:_:)`` for details.
  /// - Parameters:
  ///   - start: A boundary start, inclusive. The frame is unbounded if this is
  ///     ``unboundedPreceding``.
  ///   - end: A boundary end, inclusive. The frame is unbounded if this is
  ///     ``unboundedFollowing``.
  /// - Returns: A ``WindowSpec``.
  public static func rowsBetween(_ start: Int64, _ end: Int64) throws -> WindowSpec {
    return try WindowSpec().rowsBetween(start, end)
  }

  /// Creates a ``WindowSpec`` with the frame boundaries defined, from `start` (inclusive) to
  /// `end` (inclusive). See ``WindowSpec/rangeBetween(_:_:)`` for details.
  /// - Parameters:
  ///   - start: A boundary start, inclusive. The frame is unbounded if this is
  ///     ``unboundedPreceding``.
  ///   - end: A boundary end, inclusive. The frame is unbounded if this is
  ///     ``unboundedFollowing``.
  /// - Returns: A ``WindowSpec``.
  public static func rangeBetween(_ start: Int64, _ end: Int64) -> WindowSpec {
    return WindowSpec().rangeBetween(start, end)
  }
}
