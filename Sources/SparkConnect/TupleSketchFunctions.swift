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

// MARK: - Tuple sketch functions
//
// These functions operate on the compact binary representation of a Datasketches `TupleSketch`,
// a probabilistic data structure that estimates the number of distinct keys in a data set while
// carrying a summary value per key, and supports set operations. The estimates are approximate.
// There is one sketch per summary type, `_double` and `_integer`, and the `_theta` variants
// combine a `TupleSketch` with a `ThetaSketch` produced by the functions in
// `ThetaSketchFunctions.swift`. Sketches are produced by the `tuple_sketch_agg_*`,
// `tuple_union_agg_*` and `tuple_intersection_agg_*` aggregate functions in
// `AggregateFunctions.swift`.

/// Returns the set difference of two Datasketches `TupleSketch` with double summaries objects,
/// that is the entries whose keys are in `col1` but not in `col2`.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch` to
///     subtract from `col1`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_difference_double(_ col1: Column, _ col2: Column) -> Column {
  return fn("tuple_difference_double", col1, col2)
}

/// Returns the set difference of two Datasketches `TupleSketch` with integer summaries objects,
/// that is the entries whose keys are in `col1` but not in `col2`.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch` to
///     subtract from `col1`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_difference_integer(_ col1: Column, _ col2: Column) -> Column {
  return fn("tuple_difference_integer", col1, col2)
}

/// Returns the set difference of a Datasketches `TupleSketch` with double summaries and a
/// Datasketches `ThetaSketch`, that is the entries of `col1` whose keys are not in `col2`.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch` to
///     subtract from `col1`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_difference_theta_double(_ col1: Column, _ col2: Column) -> Column {
  return fn("tuple_difference_theta_double", col1, col2)
}

/// Returns the set difference of a Datasketches `TupleSketch` with integer summaries and a
/// Datasketches `ThetaSketch`, that is the entries of `col1` whose keys are not in `col2`.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch` to
///     subtract from `col1`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_difference_theta_integer(_ col1: Column, _ col2: Column) -> Column {
  return fn("tuple_difference_theta_integer", col1, col2)
}

/// Returns the intersection of two Datasketches `TupleSketch` with double summaries objects.
/// The server-side default summary mode `sum` is used.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_double(_ col1: Column, _ col2: Column) -> Column {
  return fn("tuple_intersection_double", col1, col2)
}

/// Returns the intersection of two Datasketches `TupleSketch` with double summaries objects.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_double(_ col1: Column, _ col2: Column, mode: Column) -> Column {
  return fn("tuple_intersection_double", col1, col2, mode)
}

/// Returns the intersection of two Datasketches `TupleSketch` with integer summaries objects.
/// The server-side default summary mode `sum` is used.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_integer(_ col1: Column, _ col2: Column) -> Column {
  return fn("tuple_intersection_integer", col1, col2)
}

/// Returns the intersection of two Datasketches `TupleSketch` with integer summaries objects.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_integer(_ col1: Column, _ col2: Column, mode: Column) -> Column {
  return fn("tuple_intersection_integer", col1, col2, mode)
}

/// Returns the intersection of a Datasketches `TupleSketch` with double summaries and a
/// Datasketches `ThetaSketch`. The server-side default summary mode `sum` is used.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_theta_double(_ col1: Column, _ col2: Column) -> Column {
  return fn("tuple_intersection_theta_double", col1, col2)
}

/// Returns the intersection of a Datasketches `TupleSketch` with double summaries and a
/// Datasketches `ThetaSketch`.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_theta_double(
  _ col1: Column, _ col2: Column, mode: Column
) -> Column {
  return fn("tuple_intersection_theta_double", col1, col2, mode)
}

/// Returns the intersection of a Datasketches `TupleSketch` with integer summaries and a
/// Datasketches `ThetaSketch`. The server-side default summary mode `sum` is used.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_theta_integer(_ col1: Column, _ col2: Column) -> Column {
  return fn("tuple_intersection_theta_integer", col1, col2)
}

/// Returns the intersection of a Datasketches `TupleSketch` with integer summaries and a
/// Datasketches `ThetaSketch`.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_theta_integer(
  _ col1: Column, _ col2: Column, mode: Column
) -> Column {
  return fn("tuple_intersection_theta_integer", col1, col2, mode)
}

/// Returns the estimated number of unique keys in a Datasketches `TupleSketch` with double
/// summaries.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `TupleSketch`.
/// - Returns: A ``Column`` that evaluates to a double.
public func tuple_sketch_estimate_double(_ col: Column) -> Column {
  return fn("tuple_sketch_estimate_double", col)
}

/// Returns the estimated number of unique keys in a Datasketches `TupleSketch` with integer
/// summaries.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `TupleSketch`.
/// - Returns: A ``Column`` that evaluates to a double.
public func tuple_sketch_estimate_integer(_ col: Column) -> Column {
  return fn("tuple_sketch_estimate_integer", col)
}

/// Returns the summary value of a Datasketches `TupleSketch` with double summaries, combining
/// the retained summaries with the given mode. The server-side default summary mode `sum` is
/// used.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `TupleSketch`.
/// - Returns: A ``Column`` that evaluates to a double.
public func tuple_sketch_summary_double(_ col: Column) -> Column {
  return fn("tuple_sketch_summary_double", col)
}

/// Returns the summary value of a Datasketches `TupleSketch` with double summaries, combining
/// the retained summaries with the given mode.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant.
/// - Returns: A ``Column`` that evaluates to a double.
public func tuple_sketch_summary_double(_ col: Column, mode: Column) -> Column {
  return fn("tuple_sketch_summary_double", col, mode)
}

/// Returns the summary value of a Datasketches `TupleSketch` with integer summaries, combining
/// the retained summaries with the given mode. The server-side default summary mode `sum` is
/// used.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `TupleSketch`.
/// - Returns: A ``Column`` that evaluates to a long.
public func tuple_sketch_summary_integer(_ col: Column) -> Column {
  return fn("tuple_sketch_summary_integer", col)
}

/// Returns the summary value of a Datasketches `TupleSketch` with integer summaries, combining
/// the retained summaries with the given mode.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant.
/// - Returns: A ``Column`` that evaluates to a long.
public func tuple_sketch_summary_integer(_ col: Column, mode: Column) -> Column {
  return fn("tuple_sketch_summary_integer", col, mode)
}

/// Returns the theta value of a Datasketches `TupleSketch` with double summaries, that is the
/// fraction of the key space that the sketch retains.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `TupleSketch`.
/// - Returns: A ``Column`` that evaluates to a double.
public func tuple_sketch_theta_double(_ col: Column) -> Column {
  return fn("tuple_sketch_theta_double", col)
}

/// Returns the theta value of a Datasketches `TupleSketch` with integer summaries, that is the
/// fraction of the key space that the sketch retains.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `TupleSketch`.
/// - Returns: A ``Column`` that evaluates to a double.
public func tuple_sketch_theta_integer(_ col: Column) -> Column {
  return fn("tuple_sketch_theta_integer", col)
}

/// Merges two Datasketches `TupleSketch` with double summaries objects.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. It must be between 4 and 26, and defaults to
///     12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_double(
  _ col1: Column, _ col2: Column,
  lgNomEntries: Column = lit(Int32(12)), mode: Column = lit("sum")
) -> Column {
  return fn("tuple_union_double", col1, col2, lgNomEntries, mode)
}

/// Merges two Datasketches `TupleSketch` with double summaries objects.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. It must be between 4 and 26, and defaults to 12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_double(
  _ col1: Column, _ col2: Column,
  lgNomEntries: Int32, mode: Column = lit("sum")
) -> Column {
  return tuple_union_double(col1, col2, lgNomEntries: lit(lgNomEntries), mode: mode)
}

/// Merges two Datasketches `TupleSketch` with integer summaries objects.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. It must be between 4 and 26, and defaults to
///     12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_integer(
  _ col1: Column, _ col2: Column,
  lgNomEntries: Column = lit(Int32(12)), mode: Column = lit("sum")
) -> Column {
  return fn("tuple_union_integer", col1, col2, lgNomEntries, mode)
}

/// Merges two Datasketches `TupleSketch` with integer summaries objects.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. It must be between 4 and 26, and defaults to 12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_integer(
  _ col1: Column, _ col2: Column,
  lgNomEntries: Int32, mode: Column = lit("sum")
) -> Column {
  return tuple_union_integer(col1, col2, lgNomEntries: lit(lgNomEntries), mode: mode)
}

/// Merges a Datasketches `TupleSketch` with double summaries and a Datasketches `ThetaSketch`
/// into a `TupleSketch` with double summaries.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. It must be between 4 and 26, and defaults to
///     12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_theta_double(
  _ col1: Column, _ col2: Column,
  lgNomEntries: Column = lit(Int32(12)), mode: Column = lit("sum")
) -> Column {
  return fn("tuple_union_theta_double", col1, col2, lgNomEntries, mode)
}

/// Merges a Datasketches `TupleSketch` with double summaries and a Datasketches `ThetaSketch`
/// into a `TupleSketch` with double summaries.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. It must be between 4 and 26, and defaults to 12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_theta_double(
  _ col1: Column, _ col2: Column,
  lgNomEntries: Int32, mode: Column = lit("sum")
) -> Column {
  return tuple_union_theta_double(col1, col2, lgNomEntries: lit(lgNomEntries), mode: mode)
}

/// Merges a Datasketches `TupleSketch` with integer summaries and a Datasketches `ThetaSketch`
/// into a `TupleSketch` with integer summaries.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. It must be between 4 and 26, and defaults to
///     12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_theta_integer(
  _ col1: Column, _ col2: Column,
  lgNomEntries: Column = lit(Int32(12)), mode: Column = lit("sum")
) -> Column {
  return fn("tuple_union_theta_integer", col1, col2, lgNomEntries, mode)
}

/// Merges a Datasketches `TupleSketch` with integer summaries and a Datasketches `ThetaSketch`
/// into a `TupleSketch` with integer summaries.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. It must be between 4 and 26, and defaults to 12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_theta_integer(
  _ col1: Column, _ col2: Column,
  lgNomEntries: Int32, mode: Column = lit("sum")
) -> Column {
  return tuple_union_theta_integer(col1, col2, lgNomEntries: lit(lgNomEntries), mode: mode)
}
