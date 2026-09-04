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

// MARK: - HLL sketch functions
//
// These functions operate on the updatable binary representation of a Datasketches `HllSketch`,
// a probabilistic data structure that estimates the number of distinct values in a data set.
// The estimates are approximate. Sketches are produced by the ``hll_sketch_agg(_:)`` and
// ``hll_union_agg(_:)`` aggregate functions in `AggregateFunctions.swift`.

/// Returns the estimated number of unique values given the binary representation of a
/// Datasketches `HllSketch`.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
/// - Returns: A ``Column`` that evaluates to the estimated number of unique values.
public func hll_sketch_estimate(_ col: Column) -> Column {
  return fn("hll_sketch_estimate", col)
}

/// Merges two binary representations of Datasketches `HllSketch` objects, using a Datasketches
/// `Union` object. Throws an exception if the sketches have different `lgConfigK` values.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
public func hll_union(_ col1: Column, _ col2: Column) -> Column {
  return fn("hll_union", col1, col2)
}

/// Merges two binary representations of Datasketches `HllSketch` objects, using a Datasketches
/// `Union` object. Throws an exception if the sketches have different `lgConfigK` values and
/// `allowDifferentLgConfigK` is `false`.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
///   - allowDifferentLgConfigK: Whether sketches with different `lgConfigK` values are allowed
///     to be merged.
/// - Returns: A ``Column`` that evaluates to the binary representation of an `HllSketch`.
public func hll_union(_ col1: Column, _ col2: Column, _ allowDifferentLgConfigK: Bool) -> Column {
  return fn("hll_union", col1, col2, lit(allowDifferentLgConfigK))
}
