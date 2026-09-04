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

// MARK: - KLL sketch functions
//
// These functions operate on the compact binary representation of a Datasketches KLL sketch, a
// probabilistic data structure that estimates quantiles and ranks of a data set. The estimates
// are approximate. There is one sketch per element type: `KllLongsSketch` (`_bigint`),
// `KllDoublesSketch` (`_double`) and `KllFloatsSketch` (`_float`). Sketches are produced by the
// `kll_sketch_agg_*` and `kll_merge_agg_*` aggregate functions in `AggregateFunctions.swift`.

/// Returns the number of items collected in a Datasketches `KllLongsSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `KllLongsSketch`.
/// - Returns: A ``Column`` that evaluates to a long.
public func kll_sketch_get_n_bigint(_ col: Column) -> Column {
  return fn("kll_sketch_get_n_bigint", col)
}

/// Returns the number of items collected in a Datasketches `KllDoublesSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `KllDoublesSketch`.
/// - Returns: A ``Column`` that evaluates to a long.
public func kll_sketch_get_n_double(_ col: Column) -> Column {
  return fn("kll_sketch_get_n_double", col)
}

/// Returns the number of items collected in a Datasketches `KllFloatsSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `KllFloatsSketch`.
/// - Returns: A ``Column`` that evaluates to a long.
public func kll_sketch_get_n_float(_ col: Column) -> Column {
  return fn("kll_sketch_get_n_float", col)
}

/// Returns the quantile value of a Datasketches `KllLongsSketch` at the given `rank`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - sketch: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
///   - rank: A ``Column`` that evaluates to a rank between 0.0 and 1.0, or to an array of such
///     ranks. It must be a constant.
/// - Returns: A ``Column`` that evaluates to a long, or to an array of longs when `rank` is an
///   array.
public func kll_sketch_get_quantile_bigint(_ sketch: Column, _ rank: Column) -> Column {
  return fn("kll_sketch_get_quantile_bigint", sketch, rank)
}

/// Returns the quantile value of a Datasketches `KllDoublesSketch` at the given `rank`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - sketch: A ``Column`` that evaluates to the binary representation of a
///     `KllDoublesSketch`.
///   - rank: A ``Column`` that evaluates to a rank between 0.0 and 1.0, or to an array of such
///     ranks. It must be a constant.
/// - Returns: A ``Column`` that evaluates to a double, or to an array of doubles when `rank` is
///   an array.
public func kll_sketch_get_quantile_double(_ sketch: Column, _ rank: Column) -> Column {
  return fn("kll_sketch_get_quantile_double", sketch, rank)
}

/// Returns the quantile value of a Datasketches `KllFloatsSketch` at the given `rank`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - sketch: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
///   - rank: A ``Column`` that evaluates to a rank between 0.0 and 1.0, or to an array of such
///     ranks. It must be a constant.
/// - Returns: A ``Column`` that evaluates to a float, or to an array of floats when `rank` is
///   an array.
public func kll_sketch_get_quantile_float(_ sketch: Column, _ rank: Column) -> Column {
  return fn("kll_sketch_get_quantile_float", sketch, rank)
}

/// Returns the rank of the given `quantile` value in a Datasketches `KllLongsSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - sketch: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
///   - quantile: A ``Column`` that evaluates to a long, or to an array of longs, to look up. It
///     must be a constant.
/// - Returns: A ``Column`` that evaluates to a rank between 0.0 and 1.0, or to an array of such
///   ranks when `quantile` is an array.
public func kll_sketch_get_rank_bigint(_ sketch: Column, _ quantile: Column) -> Column {
  return fn("kll_sketch_get_rank_bigint", sketch, quantile)
}

/// Returns the rank of the given `quantile` value in a Datasketches `KllDoublesSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - sketch: A ``Column`` that evaluates to the binary representation of a
///     `KllDoublesSketch`.
///   - quantile: A ``Column`` that evaluates to a double, or to an array of doubles, to look
///     up. It must be a constant.
/// - Returns: A ``Column`` that evaluates to a rank between 0.0 and 1.0, or to an array of such
///   ranks when `quantile` is an array.
public func kll_sketch_get_rank_double(_ sketch: Column, _ quantile: Column) -> Column {
  return fn("kll_sketch_get_rank_double", sketch, quantile)
}

/// Returns the rank of the given `quantile` value in a Datasketches `KllFloatsSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - sketch: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
///   - quantile: A ``Column`` that evaluates to a float, or to an array of floats, to look up.
///     It must be a constant.
/// - Returns: A ``Column`` that evaluates to a rank between 0.0 and 1.0, or to an array of such
///   ranks when `quantile` is an array.
public func kll_sketch_get_rank_float(_ sketch: Column, _ quantile: Column) -> Column {
  return fn("kll_sketch_get_rank_float", sketch, quantile)
}

/// Merges two Datasketches `KllLongsSketch` buffers into one.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - left: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
///   - right: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
public func kll_sketch_merge_bigint(_ left: Column, _ right: Column) -> Column {
  return fn("kll_sketch_merge_bigint", left, right)
}

/// Merges two Datasketches `KllDoublesSketch` buffers into one.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - left: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
///   - right: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
public func kll_sketch_merge_double(_ left: Column, _ right: Column) -> Column {
  return fn("kll_sketch_merge_double", left, right)
}

/// Merges two Datasketches `KllFloatsSketch` buffers into one.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - left: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
///   - right: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
public func kll_sketch_merge_float(_ left: Column, _ right: Column) -> Column {
  return fn("kll_sketch_merge_float", left, right)
}

/// Returns a human readable summary of a Datasketches `KllLongsSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `KllLongsSketch`.
/// - Returns: A ``Column`` that evaluates to a string.
public func kll_sketch_to_string_bigint(_ col: Column) -> Column {
  return fn("kll_sketch_to_string_bigint", col)
}

/// Returns a human readable summary of a Datasketches `KllDoublesSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `KllDoublesSketch`.
/// - Returns: A ``Column`` that evaluates to a string.
public func kll_sketch_to_string_double(_ col: Column) -> Column {
  return fn("kll_sketch_to_string_double", col)
}

/// Returns a human readable summary of a Datasketches `KllFloatsSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a
///   `KllFloatsSketch`.
/// - Returns: A ``Column`` that evaluates to a string.
public func kll_sketch_to_string_float(_ col: Column) -> Column {
  return fn("kll_sketch_to_string_float", col)
}
