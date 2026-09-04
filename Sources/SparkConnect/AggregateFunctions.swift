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

/// Returns some value of the column for a group of rows.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func any_value(_ col: Column) -> Column {
  return fn("any_value", col)
}

/// Returns some value of the column for a group of rows.
/// If `ignoreNulls` is true, returns only non-null values.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - ignoreNulls: A ``Column`` that evaluates to a boolean. Must be a constant.
/// - Returns: A ``Column``.
public func any_value(_ col: Column, _ ignoreNulls: Column) -> Column {
  return fn("any_value", col, ignoreNulls)
}

/// Returns the approximate number of distinct items in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func approx_count_distinct(_ col: Column) -> Column {
  return fn("approx_count_distinct", col)
}

/// Returns the approximate number of distinct items in a group.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - rsd: The maximum relative standard deviation allowed (default = 0.05).
/// - Returns: A ``Column``.
public func approx_count_distinct(_ col: Column, _ rsd: Double) -> Column {
  return fn("approx_count_distinct", col, lit(rsd))
}

/// Returns the approximate `percentile` of the numeric column `col` which is the smallest value
/// in the ordered `col` values (sorted from least to greatest) such that no more than `percentage`
/// of `col` values is less than the value or equal to that value.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - percentage: A percentage. Must be between 0.0 and 1.0.
///   - accuracy: A positive numeric literal that controls approximation accuracy
///     at the cost of memory (default = 10000).
/// - Returns: A ``Column``.
public func approx_percentile(_ col: Column, _ percentage: Double, _ accuracy: Int32 = 10000)
  -> Column
{
  return fn("approx_percentile", col, lit(percentage), lit(accuracy))
}

/// Returns the approximate `percentile`s of the numeric column `col` which are the smallest values
/// in the ordered `col` values (sorted from least to greatest) such that no more than each
/// `percentage` of `col` values is less than the value or equal to that value.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - percentage: An array of percentages. Each value must be between 0.0 and 1.0.
///   - accuracy: A positive numeric literal that controls approximation accuracy
///     at the cost of memory (default = 10000).
/// - Returns: A ``Column``.
public func approx_percentile(_ col: Column, _ percentage: [Double], _ accuracy: Int32 = 10000)
  -> Column
{
  return fn("approx_percentile", col, fn("array", percentage.map { lit($0) }), lit(accuracy))
}

/// Returns a list of objects with duplicates.
/// This is an alias of ``collect_list(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func array_agg(_ col: Column) -> Column {
  return fn("array_agg", col)
}

/// Returns the bitwise `AND` of all non-null input values, or null if none.
/// - Parameter col: A ``Column`` that evaluates to an integral.
/// - Returns: A ``Column``.
public func bit_and(_ col: Column) -> Column {
  return fn("bit_and", col)
}

/// Returns the bitwise `OR` of all non-null input values, or null if none.
/// - Parameter col: A ``Column`` that evaluates to an integral.
/// - Returns: A ``Column``.
public func bit_or(_ col: Column) -> Column {
  return fn("bit_or", col)
}

/// Returns the bitwise `XOR` of all non-null input values, or null if none.
/// - Parameter col: A ``Column`` that evaluates to an integral.
/// - Returns: A ``Column``.
public func bit_xor(_ col: Column) -> Column {
  return fn("bit_xor", col)
}

/// Returns a bitmap that is the bitwise `AND` of all of the bitmaps in a group. The input column
/// should be bitmaps created by ``bitmap_construct_agg(_:)``.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to a binary bitmap.
/// - Returns: A ``Column`` that evaluates to a binary bitmap.
public func bitmap_and_agg(_ col: Column) -> Column {
  return fn("bitmap_and_agg", col)
}

/// Returns a bitmap with the positions of the bits set from all the values in a group. The input
/// column will most likely be ``bitmap_bit_position(_:)``.
/// - Parameter col: A ``Column`` that evaluates to an integral.
/// - Returns: A ``Column`` that evaluates to a binary bitmap.
public func bitmap_construct_agg(_ col: Column) -> Column {
  return fn("bitmap_construct_agg", col)
}

/// Returns a bitmap that is the bitwise `OR` of all of the bitmaps in a group. The input column
/// should be bitmaps created by ``bitmap_construct_agg(_:)``.
/// - Parameter col: A ``Column`` that evaluates to a binary bitmap.
/// - Returns: A ``Column`` that evaluates to a binary bitmap.
public func bitmap_or_agg(_ col: Column) -> Column {
  return fn("bitmap_or_agg", col)
}

/// Returns true if all values of the column are true.
/// - Parameter col: A ``Column`` that evaluates to a boolean.
/// - Returns: A ``Column``.
public func bool_and(_ col: Column) -> Column {
  return fn("bool_and", col)
}

/// Returns true if at least one value of the column is true.
/// - Parameter col: A ``Column`` that evaluates to a boolean.
/// - Returns: A ``Column``.
public func bool_or(_ col: Column) -> Column {
  return fn("bool_or", col)
}

/// Returns a list of objects with duplicates.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func collect_list(_ col: Column) -> Column {
  return fn("collect_list", col)
}

/// Returns a set of objects with duplicate elements eliminated.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func collect_set(_ col: Column) -> Column {
  return fn("collect_set", col)
}

/// Given an array-typed column, collects the distinct union of the elements of the arrays
/// across rows and returns it as an array.
///
/// Null elements are dropped by default, matching ``collect_set(_:)``.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameter col: An array ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func collect_union(_ col: Column) -> Column {
  return fn("collect_union", col)
}

/// Returns the Pearson Correlation Coefficient for two columns.
/// - Parameters:
///   - column1: A ``Column``.
///   - column2: A ``Column``.
/// - Returns: A ``Column``.
public func corr(_ column1: Column, _ column2: Column) -> Column {
  return fn("corr", column1, column2)
}

/// Returns the number of distinct items in a group.
/// This is an alias of ``count_distinct(_:_:)``.
/// - Parameters:
///   - expr: A ``Column`` to count.
///   - exprs: Additional ``Column``s to count.
/// - Returns: A ``Column``.
public func countDistinct(_ expr: Column, _ exprs: Column...) -> Column {
  return fn("count", [expr] + exprs, isDistinct: true)
}

/// Returns the number of distinct items in a group.
/// - Parameters:
///   - expr: A ``Column`` to count.
///   - exprs: Additional ``Column``s to count.
/// - Returns: A ``Column``.
public func count_distinct(_ expr: Column, _ exprs: Column...) -> Column {
  return fn("count", [expr] + exprs, isDistinct: true)
}

/// Returns the number of `TRUE` values for the expression.
/// - Parameter col: A ``Column`` that evaluates to a boolean.
/// - Returns: A ``Column``.
public func count_if(_ col: Column) -> Column {
  return fn("count_if", col)
}

/// Returns a count-min sketch of the column with the given `eps` and `confidence`,
/// using a randomly generated seed.
/// The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - eps: The relative error. Must be positive.
///   - confidence: The confidence. Must be positive and less than 1.0.
/// - Returns: A ``Column``.
public func count_min_sketch(_ col: Column, _ eps: Double, _ confidence: Double) -> Column {
  return count_min_sketch(col, eps, confidence, Int64.random(in: Int64.min...Int64.max))
}

/// Returns a count-min sketch of the column with the given `eps`, `confidence` and `seed`.
/// The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - eps: The relative error. Must be positive.
///   - confidence: The confidence. Must be positive and less than 1.0.
///   - seed: The random seed.
/// - Returns: A ``Column``.
public func count_min_sketch(_ col: Column, _ eps: Double, _ confidence: Double, _ seed: Int64)
  -> Column
{
  return fn("count_min_sketch", col, lit(eps), lit(confidence), lit(seed))
}

/// Returns the population covariance for two columns.
/// - Parameters:
///   - column1: A ``Column``.
///   - column2: A ``Column``.
/// - Returns: A ``Column``.
public func covar_pop(_ column1: Column, _ column2: Column) -> Column {
  return fn("covar_pop", column1, column2)
}

/// Returns the sample covariance for two columns.
/// - Parameters:
///   - column1: A ``Column``.
///   - column2: A ``Column``.
/// - Returns: A ``Column``.
public func covar_samp(_ column1: Column, _ column2: Column) -> Column {
  return fn("covar_samp", column1, column2)
}

/// Returns true if all values of the column are true.
/// This is an alias of ``bool_and(_:)``.
/// - Parameter col: A ``Column`` that evaluates to a boolean.
/// - Returns: A ``Column``.
public func every(_ col: Column) -> Column {
  return fn("every", col)
}

/// Returns the first value in a group.
///
/// The function by default returns the first values it sees. It will return the first non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func first(_ col: Column) -> Column {
  return first(col, false)
}

/// Returns the first value in a group.
///
/// The function by default returns the first values it sees. It will return the first non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - ignoreNulls: Whether to skip null values.
/// - Returns: A ``Column``.
public func first(_ col: Column, _ ignoreNulls: Bool) -> Column {
  return fn("first", col, lit(ignoreNulls))
}

/// Returns the first value in a group.
/// This is similar to ``first(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func first_value(_ col: Column) -> Column {
  return fn("first_value", col)
}

/// Returns the first value in a group.
///
/// The function by default returns the first values it sees. It will return the first non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// This is similar to ``first(_:_:)``.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - ignoreNulls: Whether to skip null values.
/// - Returns: A ``Column``.
public func first_value(_ col: Column, _ ignoreNulls: Bool) -> Column {
  return fn("first_value", col, lit(ignoreNulls))
}

/// Indicates whether a specified column in a GROUP BY list is aggregated or not,
/// returns 1 for aggregated or 0 for not aggregated in the result set.
/// - Parameter col: A ``Column`` to check.
/// - Returns: A ``Column``.
public func grouping(_ col: Column) -> Column {
  return fn("grouping", col)
}

/// Returns the level of grouping.
/// - Parameter cols: ``Column``s to check.
/// - Returns: A ``Column``.
public func grouping_id(_ cols: Column...) -> Column {
  return fn("grouping_id", cols)
}

/// Computes a histogram on numeric `col` using `nBins` bins.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - nBins: The number of bins. Must be greater than 1.
/// - Returns: A ``Column``.
public func histogram_numeric(_ col: Column, _ nBins: Int32) -> Column {
  return fn("histogram_numeric", col, lit(nBins))
}

/// Returns the updatable binary representation of the Datasketches `HllSketch` built from the
/// values in a group. The sketch uses the server-side default `lgConfigK` of 12.
/// - Parameter col: A ``Column`` that evaluates to an integer, long, string, or binary.
/// - Returns: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
public func hll_sketch_agg(_ col: Column) -> Column {
  return fn("hll_sketch_agg", col)
}

/// Returns the updatable binary representation of the Datasketches `HllSketch` built from the
/// values in a group, configured with `lgConfigK`.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an integer, long, string, or binary.
///   - lgConfigK: The log-base-2 of `K`, where `K` is the number of buckets or slots for the
///     `HllSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
public func hll_sketch_agg(_ col: Column, _ lgConfigK: Int32) -> Column {
  return hll_sketch_agg(col, lit(lgConfigK))
}

/// Returns the updatable binary representation of the Datasketches `HllSketch` built from the
/// values in a group, configured with `lgConfigK`.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an integer, long, string, or binary.
///   - lgConfigK: A ``Column`` that evaluates to the log-base-2 of `K`, where `K` is the number
///     of buckets or slots for the `HllSketch`. Must be a constant.
/// - Returns: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
public func hll_sketch_agg(_ col: Column, _ lgConfigK: Column) -> Column {
  return fn("hll_sketch_agg", col, lgConfigK)
}

/// Returns the updatable binary representation of the Datasketches `HllSketch` generated by
/// merging the `HllSketch` values in a group via a Datasketches `Union` instance. Throws an
/// exception if the sketches have different `lgConfigK` values.
/// - Parameter col: A ``Column`` of binary `HllSketch` representations to aggregate.
/// - Returns: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
public func hll_union_agg(_ col: Column) -> Column {
  return fn("hll_union_agg", col)
}

/// Returns the updatable binary representation of the Datasketches `HllSketch` generated by
/// merging the `HllSketch` values in a group via a Datasketches `Union` instance. Throws an
/// exception if the sketches have different `lgConfigK` values and `allowDifferentLgConfigK` is
/// `false`.
/// - Parameters:
///   - col: A ``Column`` of binary `HllSketch` representations to aggregate.
///   - allowDifferentLgConfigK: Whether sketches with different `lgConfigK` values are allowed
///     to be merged.
/// - Returns: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
public func hll_union_agg(_ col: Column, _ allowDifferentLgConfigK: Bool) -> Column {
  return hll_union_agg(col, lit(allowDifferentLgConfigK))
}

/// Returns the updatable binary representation of the Datasketches `HllSketch` generated by
/// merging the `HllSketch` values in a group via a Datasketches `Union` instance. Throws an
/// exception if the sketches have different `lgConfigK` values and `allowDifferentLgConfigK` is
/// `false`.
/// - Parameters:
///   - col: A ``Column`` of binary `HllSketch` representations to aggregate.
///   - allowDifferentLgConfigK: A ``Column`` that evaluates to whether sketches with different
///     `lgConfigK` values are allowed to be merged. Must be a constant.
/// - Returns: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
public func hll_union_agg(_ col: Column, _ allowDifferentLgConfigK: Column) -> Column {
  return fn("hll_union_agg", col, allowDifferentLgConfigK)
}

/// Returns the compact binary representation of the Datasketches `KllLongsSketch` that merges
/// the `KllLongsSketch` values in a group. The merged sketch adopts the `k` of the first input
/// sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameter col: A ``Column`` of binary `KllLongsSketch` representations to aggregate.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
public func kll_merge_agg_bigint(_ col: Column) -> Column {
  return fn("kll_merge_agg_bigint", col)
}

/// Returns the compact binary representation of the Datasketches `KllLongsSketch` that merges
/// the `KllLongsSketch` values in a group. The `k` parameter controls the size and accuracy of
/// the sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameters:
///   - col: A ``Column`` of binary `KllLongsSketch` representations to aggregate.
///   - k: The parameter that controls the size and accuracy of the sketch. It must be between 8
///     and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
public func kll_merge_agg_bigint(_ col: Column, _ k: Int32) -> Column {
  return kll_merge_agg_bigint(col, lit(k))
}

/// Returns the compact binary representation of the Datasketches `KllLongsSketch` that merges
/// the `KllLongsSketch` values in a group. The `k` parameter controls the size and accuracy of
/// the sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameters:
///   - col: A ``Column`` of binary `KllLongsSketch` representations to aggregate.
///   - k: A ``Column`` that evaluates to the parameter that controls the size and accuracy of
///     the sketch. It must be a constant between 8 and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
public func kll_merge_agg_bigint(_ col: Column, _ k: Column) -> Column {
  return fn("kll_merge_agg_bigint", col, k)
}

/// Returns the compact binary representation of the Datasketches `KllDoublesSketch` that merges
/// the `KllDoublesSketch` values in a group. The merged sketch adopts the `k` of the first
/// input sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameter col: A ``Column`` of binary `KllDoublesSketch` representations to aggregate.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
public func kll_merge_agg_double(_ col: Column) -> Column {
  return fn("kll_merge_agg_double", col)
}

/// Returns the compact binary representation of the Datasketches `KllDoublesSketch` that merges
/// the `KllDoublesSketch` values in a group. The `k` parameter controls the size and accuracy
/// of the sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameters:
///   - col: A ``Column`` of binary `KllDoublesSketch` representations to aggregate.
///   - k: The parameter that controls the size and accuracy of the sketch. It must be between 8
///     and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
public func kll_merge_agg_double(_ col: Column, _ k: Int32) -> Column {
  return kll_merge_agg_double(col, lit(k))
}

/// Returns the compact binary representation of the Datasketches `KllDoublesSketch` that merges
/// the `KllDoublesSketch` values in a group. The `k` parameter controls the size and accuracy
/// of the sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameters:
///   - col: A ``Column`` of binary `KllDoublesSketch` representations to aggregate.
///   - k: A ``Column`` that evaluates to the parameter that controls the size and accuracy of
///     the sketch. It must be a constant between 8 and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
public func kll_merge_agg_double(_ col: Column, _ k: Column) -> Column {
  return fn("kll_merge_agg_double", col, k)
}

/// Returns the compact binary representation of the Datasketches `KllFloatsSketch` that merges
/// the `KllFloatsSketch` values in a group. The merged sketch adopts the `k` of the first input
/// sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameter col: A ``Column`` of binary `KllFloatsSketch` representations to aggregate.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
public func kll_merge_agg_float(_ col: Column) -> Column {
  return fn("kll_merge_agg_float", col)
}

/// Returns the compact binary representation of the Datasketches `KllFloatsSketch` that merges
/// the `KllFloatsSketch` values in a group. The `k` parameter controls the size and accuracy of
/// the sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameters:
///   - col: A ``Column`` of binary `KllFloatsSketch` representations to aggregate.
///   - k: The parameter that controls the size and accuracy of the sketch. It must be between 8
///     and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
public func kll_merge_agg_float(_ col: Column, _ k: Int32) -> Column {
  return kll_merge_agg_float(col, lit(k))
}

/// Returns the compact binary representation of the Datasketches `KllFloatsSketch` that merges
/// the `KllFloatsSketch` values in a group. The `k` parameter controls the size and accuracy of
/// the sketch.
/// This requires Apache Spark 4.1.2 or later.
/// - Parameters:
///   - col: A ``Column`` of binary `KllFloatsSketch` representations to aggregate.
///   - k: A ``Column`` that evaluates to the parameter that controls the size and accuracy of
///     the sketch. It must be a constant between 8 and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
public func kll_merge_agg_float(_ col: Column, _ k: Column) -> Column {
  return fn("kll_merge_agg_float", col, k)
}

/// Returns the compact binary representation of the Datasketches `KllLongsSketch` built from
/// the long values in a group. The sketch uses the server-side default `k` of 200.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to a long.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
public func kll_sketch_agg_bigint(_ col: Column) -> Column {
  return fn("kll_sketch_agg_bigint", col)
}

/// Returns the compact binary representation of the Datasketches `KllLongsSketch` built from
/// the long values in a group. The `k` parameter controls the size and accuracy of the sketch.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a long.
///   - k: The parameter that controls the size and accuracy of the sketch. It must be between 8
///     and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
public func kll_sketch_agg_bigint(_ col: Column, _ k: Int32) -> Column {
  return kll_sketch_agg_bigint(col, lit(k))
}

/// Returns the compact binary representation of the Datasketches `KllLongsSketch` built from
/// the long values in a group. The `k` parameter controls the size and accuracy of the sketch.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a long.
///   - k: A ``Column`` that evaluates to the parameter that controls the size and accuracy of
///     the sketch. It must be a constant between 8 and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllLongsSketch`.
public func kll_sketch_agg_bigint(_ col: Column, _ k: Column) -> Column {
  return fn("kll_sketch_agg_bigint", col, k)
}

/// Returns the compact binary representation of the Datasketches `KllDoublesSketch` built from
/// the double values in a group. The sketch uses the server-side default `k` of 200.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to a double.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
public func kll_sketch_agg_double(_ col: Column) -> Column {
  return fn("kll_sketch_agg_double", col)
}

/// Returns the compact binary representation of the Datasketches `KllDoublesSketch` built from
/// the double values in a group. The `k` parameter controls the size and accuracy of the
/// sketch.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a double.
///   - k: The parameter that controls the size and accuracy of the sketch. It must be between 8
///     and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
public func kll_sketch_agg_double(_ col: Column, _ k: Int32) -> Column {
  return kll_sketch_agg_double(col, lit(k))
}

/// Returns the compact binary representation of the Datasketches `KllDoublesSketch` built from
/// the double values in a group. The `k` parameter controls the size and accuracy of the
/// sketch.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a double.
///   - k: A ``Column`` that evaluates to the parameter that controls the size and accuracy of
///     the sketch. It must be a constant between 8 and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllDoublesSketch`.
public func kll_sketch_agg_double(_ col: Column, _ k: Column) -> Column {
  return fn("kll_sketch_agg_double", col, k)
}

/// Returns the compact binary representation of the Datasketches `KllFloatsSketch` built from
/// the float values in a group. The sketch uses the server-side default `k` of 200.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to a float.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
public func kll_sketch_agg_float(_ col: Column) -> Column {
  return fn("kll_sketch_agg_float", col)
}

/// Returns the compact binary representation of the Datasketches `KllFloatsSketch` built from
/// the float values in a group. The `k` parameter controls the size and accuracy of the sketch.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a float.
///   - k: The parameter that controls the size and accuracy of the sketch. It must be between 8
///     and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
public func kll_sketch_agg_float(_ col: Column, _ k: Int32) -> Column {
  return kll_sketch_agg_float(col, lit(k))
}

/// Returns the compact binary representation of the Datasketches `KllFloatsSketch` built from
/// the float values in a group. The `k` parameter controls the size and accuracy of the sketch.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a float.
///   - k: A ``Column`` that evaluates to the parameter that controls the size and accuracy of
///     the sketch. It must be a constant between 8 and 65535.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `KllFloatsSketch`.
public func kll_sketch_agg_float(_ col: Column, _ k: Column) -> Column {
  return fn("kll_sketch_agg_float", col, k)
}

/// Returns the kurtosis of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func kurtosis(_ col: Column) -> Column {
  return fn("kurtosis", col)
}

/// Returns the last value in a group.
///
/// The function by default returns the last values it sees. It will return the last non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func last(_ col: Column) -> Column {
  return last(col, false)
}

/// Returns the last value in a group.
///
/// The function by default returns the last values it sees. It will return the last non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - ignoreNulls: Whether to skip null values.
/// - Returns: A ``Column``.
public func last(_ col: Column, _ ignoreNulls: Bool) -> Column {
  return fn("last", col, lit(ignoreNulls))
}

/// Returns the last value in a group.
/// This is similar to ``last(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func last_value(_ col: Column) -> Column {
  return fn("last_value", col)
}

/// Returns the last value in a group.
///
/// The function by default returns the last values it sees. It will return the last non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// This is similar to ``last(_:_:)``.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - ignoreNulls: Whether to skip null values.
/// - Returns: A ``Column``.
public func last_value(_ col: Column, _ ignoreNulls: Bool) -> Column {
  return fn("last_value", col, lit(ignoreNulls))
}

/// Returns the concatenation of the non-null input values.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func listagg(_ col: Column) -> Column {
  return fn("listagg", col)
}

/// Returns the concatenation of the non-null input values, separated by `delimiter`.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - delimiter: A delimiter to separate the values.
/// - Returns: A ``Column``.
public func listagg(_ col: Column, _ delimiter: String) -> Column {
  return fn("listagg", col, lit(delimiter))
}

/// Returns the concatenation of the distinct non-null input values.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func listagg_distinct(_ col: Column) -> Column {
  return fn("listagg", [col], isDistinct: true)
}

/// Returns the concatenation of the distinct non-null input values, separated by `delimiter`.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - delimiter: A delimiter to separate the values.
/// - Returns: A ``Column``.
public func listagg_distinct(_ col: Column, _ delimiter: String) -> Column {
  return fn("listagg", [col, lit(delimiter)], isDistinct: true)
}

/// Returns the value associated with the maximum value of `ord`.
/// - Parameters:
///   - col: A ``Column`` to return the value from.
///   - ord: A ``Column`` to be maximized.
/// - Returns: A ``Column``.
public func max_by(_ col: Column, _ ord: Column) -> Column {
  return fn("max_by", col, ord)
}

/// Returns the median of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func median(_ col: Column) -> Column {
  return fn("median", col)
}

/// Returns the value associated with the minimum value of `ord`.
/// - Parameters:
///   - col: A ``Column`` to return the value from.
///   - ord: A ``Column`` to be minimized.
/// - Returns: A ``Column``.
public func min_by(_ col: Column, _ ord: Column) -> Column {
  return fn("min_by", col, ord)
}

/// Returns the most frequent value in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func mode(_ col: Column) -> Column {
  return fn("mode", col)
}

/// Returns the most frequent value in a group.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - deterministic: If there are multiple equally-frequent results,
///     whether to return the lowest (defined by min-hash) one.
/// - Returns: A ``Column``.
public func mode(_ col: Column, _ deterministic: Bool) -> Column {
  return fn("mode", col, lit(deterministic))
}

/// Returns the exact `percentile` of the numeric column `col` at the given `percentage`.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - percentage: A percentage. Must be between 0.0 and 1.0.
///   - frequency: A positive numeric literal for the number of times `col` should be counted
///     (default = 1).
/// - Returns: A ``Column``.
public func percentile(_ col: Column, _ percentage: Double, _ frequency: Int32 = 1) -> Column {
  return fn("percentile", col, lit(percentage), lit(frequency))
}

/// Returns the exact `percentile`s of the numeric column `col` at the given `percentage`s.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - percentage: An array of percentages. Each value must be between 0.0 and 1.0.
///   - frequency: A positive numeric literal for the number of times `col` should be counted
///     (default = 1).
/// - Returns: A ``Column``.
public func percentile(_ col: Column, _ percentage: [Double], _ frequency: Int32 = 1) -> Column {
  return fn("percentile", col, fn("array", percentage.map { lit($0) }), lit(frequency))
}

/// Returns the approximate `percentile` of the numeric column `col` which is the smallest value
/// in the ordered `col` values (sorted from least to greatest) such that no more than `percentage`
/// of `col` values is less than the value or equal to that value.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - percentage: A percentage ``Column``. Each value must be between 0.0 and 1.0.
///   - accuracy: A positive numeric literal ``Column`` that controls approximation accuracy
///     at the cost of memory.
/// - Returns: A ``Column``.
public func percentile_approx(_ col: Column, _ percentage: Column, _ accuracy: Column) -> Column {
  return fn("percentile_approx", col, percentage, accuracy)
}

/// Returns the product of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func product(_ col: Column) -> Column {
  return fn("product", col)
}

/// Returns the average of the independent variable for non-null pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_avgx(_ y: Column, _ x: Column) -> Column {
  return fn("regr_avgx", y, x)
}

/// Returns the average of the dependent variable for non-null pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_avgy(_ y: Column, _ x: Column) -> Column {
  return fn("regr_avgy", y, x)
}

/// Returns the number of non-null number pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_count(_ y: Column, _ x: Column) -> Column {
  return fn("regr_count", y, x)
}

/// Returns the intercept of the univariate linear regression line for non-null pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_intercept(_ y: Column, _ x: Column) -> Column {
  return fn("regr_intercept", y, x)
}

/// Returns the coefficient of determination for non-null pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_r2(_ y: Column, _ x: Column) -> Column {
  return fn("regr_r2", y, x)
}

/// Returns the slope of the linear regression line for non-null pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_slope(_ y: Column, _ x: Column) -> Column {
  return fn("regr_slope", y, x)
}

/// Returns `REGR_COUNT(y, x) * VAR_POP(x)` for non-null pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_sxx(_ y: Column, _ x: Column) -> Column {
  return fn("regr_sxx", y, x)
}

/// Returns `REGR_COUNT(y, x) * COVAR_POP(y, x)` for non-null pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_sxy(_ y: Column, _ x: Column) -> Column {
  return fn("regr_sxy", y, x)
}

/// Returns `REGR_COUNT(y, x) * VAR_POP(y)` for non-null pairs in a group.
/// - Parameters:
///   - y: The dependent variable. A ``Column`` that evaluates to a numeric.
///   - x: The independent variable. A ``Column`` that evaluates to a numeric.
/// - Returns: A ``Column``.
public func regr_syy(_ y: Column, _ x: Column) -> Column {
  return fn("regr_syy", y, x)
}

/// Returns the merged schema in DDL format of all `VARIANT` values in a group.
/// - Parameter v: A ``Column`` of the `VARIANT` type to aggregate.
/// - Returns: A ``Column`` that evaluates to a schema string in DDL format.
public func schema_of_variant_agg(_ v: Column) -> Column {
  return fn("schema_of_variant_agg", v)
}

/// Returns the skewness of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func skewness(_ col: Column) -> Column {
  return fn("skewness", col)
}

/// Returns true if at least one value of the column is true.
/// This is an alias of ``bool_or(_:)``.
/// - Parameter col: A ``Column`` that evaluates to a boolean.
/// - Returns: A ``Column``.
public func some(_ col: Column) -> Column {
  return fn("some", col)
}

/// Returns the sample standard deviation of the expression in a group.
/// This is an alias of ``stddev_samp(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func std(_ col: Column) -> Column {
  return fn("std", col)
}

/// Returns the sample standard deviation of the expression in a group.
/// This is an alias of ``stddev_samp(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func stddev(_ col: Column) -> Column {
  return fn("stddev", col)
}

/// Returns the population standard deviation of the expression in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func stddev_pop(_ col: Column) -> Column {
  return fn("stddev_pop", col)
}

/// Returns the sample standard deviation of the expression in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func stddev_samp(_ col: Column) -> Column {
  return fn("stddev_samp", col)
}

/// Returns the concatenation of the non-null input values.
/// This is an alias of ``listagg(_:)`` and requires Apache Spark 4.0.0 or later.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func string_agg(_ col: Column) -> Column {
  return fn("string_agg", col)
}

/// Returns the concatenation of the non-null input values, separated by `delimiter`.
/// This is an alias of ``listagg(_:_:)`` and requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - delimiter: A delimiter to separate the values.
/// - Returns: A ``Column``.
public func string_agg(_ col: Column, _ delimiter: String) -> Column {
  return fn("string_agg", col, lit(delimiter))
}

/// Returns the concatenation of the distinct non-null input values.
/// This is an alias of ``listagg_distinct(_:)`` and requires Apache Spark 4.0.0 or later.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func string_agg_distinct(_ col: Column) -> Column {
  return fn("string_agg", [col], isDistinct: true)
}

/// Returns the concatenation of the distinct non-null input values, separated by `delimiter`.
/// This is an alias of ``listagg_distinct(_:_:)`` and requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - delimiter: A delimiter to separate the values.
/// - Returns: A ``Column``.
public func string_agg_distinct(_ col: Column, _ delimiter: String) -> Column {
  return fn("string_agg", [col, lit(delimiter)], isDistinct: true)
}

/// Returns the sum of distinct values in the expression.
/// This is an alias of ``sum_distinct(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func sumDistinct(_ col: Column) -> Column {
  return sum_distinct(col)
}

/// Returns the sum of distinct values in the expression.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func sum_distinct(_ col: Column) -> Column {
  return fn("sum", [col], isDistinct: true)
}

/// Returns the compact binary representation of the Datasketches `ThetaSketch` that is the
/// intersection of the Theta sketches in a group.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` of Theta sketches to aggregate.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_intersection_agg(_ col: Column) -> Column {
  return fn("theta_intersection_agg", col)
}

/// Returns the compact binary representation of the Datasketches `ThetaSketch` built from the
/// values in a group, using the server-side default of 12 nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to an array, binary, double, float, integer,
///   long, or string.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_sketch_agg(_ col: Column) -> Column {
  return fn("theta_sketch_agg", col)
}

/// Returns the compact binary representation of the Datasketches `ThetaSketch` built from the
/// values in a group, configured with `lgNomEntries` nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an array, binary, double, float, integer, long, or
///     string.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. Must be between 4 and 26.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_sketch_agg(_ col: Column, _ lgNomEntries: Int32) -> Column {
  return theta_sketch_agg(col, lit(lgNomEntries))
}

/// Returns the compact binary representation of the Datasketches `ThetaSketch` built from the
/// values in a group, configured with `lgNomEntries` nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an array, binary, double, float, integer, long, or
///     string.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. Must be a constant between 4 and 26.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_sketch_agg(_ col: Column, _ lgNomEntries: Column) -> Column {
  return fn("theta_sketch_agg", col, lgNomEntries)
}

/// Returns the compact binary representation of the Datasketches `ThetaSketch` that is the union
/// of the Theta sketches in a group, using the server-side default of 12 nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` of Theta sketches to aggregate.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_union_agg(_ col: Column) -> Column {
  return fn("theta_union_agg", col)
}

/// Returns the compact binary representation of the Datasketches `ThetaSketch` that is the union
/// of the Theta sketches in a group, configured with `lgNomEntries` nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` of Theta sketches to aggregate.
///   - lgNomEntries: The log-base-2 of the number of nominal entries used by the union
///     operation. Must be between 4 and 26.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_union_agg(_ col: Column, _ lgNomEntries: Int32) -> Column {
  return theta_union_agg(col, lit(lgNomEntries))
}

/// Returns the compact binary representation of the Datasketches `ThetaSketch` that is the union
/// of the Theta sketches in a group, configured with `lgNomEntries` nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col: A ``Column`` of Theta sketches to aggregate.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries used by the union operation. Must be a constant between 4 and 26.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_union_agg(_ col: Column, _ lgNomEntries: Column) -> Column {
  return fn("theta_union_agg", col, lgNomEntries)
}

/// Returns the mean calculated from values of a group and `null` on overflow.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func try_avg(_ col: Column) -> Column {
  return fn("try_avg", col)
}

/// Returns the sum calculated from values of a group and `null` on overflow.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func try_sum(_ col: Column) -> Column {
  return fn("try_sum", col)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with double
/// summaries that is the intersection of the Tuple sketches in a group. The server-side default
/// summary mode `sum` is used.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameter col: A ``Column`` of `TupleSketch` objects with double summaries to aggregate.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_agg_double(_ col: Column) -> Column {
  return fn("tuple_intersection_agg_double", col)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with double
/// summaries that is the intersection of the Tuple sketches in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col: A ``Column`` of `TupleSketch` objects with double summaries to aggregate.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_agg_double(_ col: Column, mode: Column) -> Column {
  return fn("tuple_intersection_agg_double", col, mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with integer
/// summaries that is the intersection of the Tuple sketches in a group. The server-side default
/// summary mode `sum` is used.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameter col: A ``Column`` of `TupleSketch` objects with integer summaries to aggregate.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_agg_integer(_ col: Column) -> Column {
  return fn("tuple_intersection_agg_integer", col)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with integer
/// summaries that is the intersection of the Tuple sketches in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col: A ``Column`` of `TupleSketch` objects with integer summaries to aggregate.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_intersection_agg_integer(_ col: Column, mode: Column) -> Column {
  return fn("tuple_intersection_agg_integer", col, mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with double
/// summaries built from the key and summary values in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - key: A ``Column`` that evaluates to an array, binary, double, float, integer, long, or
///     string.
///   - summary: A ``Column`` that evaluates to a double.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. It must be between 4 and 26, and defaults to
///     12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_sketch_agg_double(
  _ key: Column, _ summary: Column,
  lgNomEntries: Column = lit(Int32(12)), mode: Column = lit("sum")
) -> Column {
  return fn("tuple_sketch_agg_double", key, summary, lgNomEntries, mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with double
/// summaries built from the key and summary values in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - key: A ``Column`` that evaluates to an array, binary, double, float, integer, long, or
///     string.
///   - summary: A ``Column`` that evaluates to a double.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. It must be between 4 and 26, and defaults to 12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_sketch_agg_double(
  _ key: Column, _ summary: Column,
  lgNomEntries: Int32, mode: Column = lit("sum")
) -> Column {
  return tuple_sketch_agg_double(key, summary, lgNomEntries: lit(lgNomEntries), mode: mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with integer
/// summaries built from the key and summary values in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - key: A ``Column`` that evaluates to an array, binary, double, float, integer, long, or
///     string.
///   - summary: A ``Column`` that evaluates to an integer.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. It must be between 4 and 26, and defaults to
///     12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_sketch_agg_integer(
  _ key: Column, _ summary: Column,
  lgNomEntries: Column = lit(Int32(12)), mode: Column = lit("sum")
) -> Column {
  return fn("tuple_sketch_agg_integer", key, summary, lgNomEntries, mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with integer
/// summaries built from the key and summary values in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - key: A ``Column`` that evaluates to an array, binary, double, float, integer, long, or
///     string.
///   - summary: A ``Column`` that evaluates to an integer.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. It must be between 4 and 26, and defaults to 12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_sketch_agg_integer(
  _ key: Column, _ summary: Column,
  lgNomEntries: Int32, mode: Column = lit("sum")
) -> Column {
  return tuple_sketch_agg_integer(key, summary, lgNomEntries: lit(lgNomEntries), mode: mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with double
/// summaries that is the union of the Tuple sketches in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col: A ``Column`` of `TupleSketch` objects with double summaries to aggregate.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. It must be between 4 and 26, and defaults to
///     12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_agg_double(
  _ col: Column,
  lgNomEntries: Column = lit(Int32(12)), mode: Column = lit("sum")
) -> Column {
  return fn("tuple_union_agg_double", col, lgNomEntries, mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with double
/// summaries that is the union of the Tuple sketches in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col: A ``Column`` of `TupleSketch` objects with double summaries to aggregate.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. It must be between 4 and 26, and defaults to 12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_agg_double(
  _ col: Column,
  lgNomEntries: Int32, mode: Column = lit("sum")
) -> Column {
  return tuple_union_agg_double(col, lgNomEntries: lit(lgNomEntries), mode: mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with integer
/// summaries that is the union of the Tuple sketches in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col: A ``Column`` of `TupleSketch` objects with integer summaries to aggregate.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries, which is the size of the sketch. It must be between 4 and 26, and defaults to
///     12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_agg_integer(
  _ col: Column,
  lgNomEntries: Column = lit(Int32(12)), mode: Column = lit("sum")
) -> Column {
  return fn("tuple_union_agg_integer", col, lgNomEntries, mode)
}

/// Returns the compact binary representation of the Datasketches `TupleSketch` with integer
/// summaries that is the union of the Tuple sketches in a group.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - col: A ``Column`` of `TupleSketch` objects with integer summaries to aggregate.
///   - lgNomEntries: The log-base-2 of the number of nominal entries, which is the size of the
///     sketch. It must be between 4 and 26, and defaults to 12.
///   - mode: A ``Column`` that evaluates to the summary mode, one of `sum`, `min`, `max` or
///     `alwaysone`. It must be a constant. It defaults to `sum`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `TupleSketch`.
public func tuple_union_agg_integer(
  _ col: Column,
  lgNomEntries: Int32, mode: Column = lit("sum")
) -> Column {
  return tuple_union_agg_integer(col, lgNomEntries: lit(lgNomEntries), mode: mode)
}

/// Returns the population variance of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func var_pop(_ col: Column) -> Column {
  return fn("var_pop", col)
}

/// Returns the unbiased variance of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func var_samp(_ col: Column) -> Column {
  return fn("var_samp", col)
}

/// Returns the unbiased variance of the values in a group.
/// This is an alias of ``var_samp(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func variance(_ col: Column) -> Column {
  return fn("variance", col)
}

/// Returns the element-wise mean of the float vectors in a group.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameter col: A ``Column`` that evaluates to an `ARRAY<FLOAT>` to aggregate. All the
///   vectors in a group must have the same dimension.
/// - Returns: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
public func vector_avg(_ col: Column) -> Column {
  return fn("vector_avg", col)
}

/// Returns the element-wise sum of the float vectors in a group.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameter col: A ``Column`` that evaluates to an `ARRAY<FLOAT>` to aggregate. All the
///   vectors in a group must have the same dimension.
/// - Returns: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
public func vector_sum(_ col: Column) -> Column {
  return fn("vector_sum", col)
}
