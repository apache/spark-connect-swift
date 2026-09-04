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

// MARK: - Theta sketch functions
//
// These functions operate on the compact binary representation of a Datasketches `ThetaSketch`,
// a probabilistic data structure that estimates the number of distinct values in a data set and
// supports set operations. The estimates are approximate. Sketches are produced by the
// ``theta_sketch_agg(_:)``, ``theta_union_agg(_:)`` and ``theta_intersection_agg(_:)`` aggregate
// functions in `AggregateFunctions.swift`.

/// Returns the set difference of two Datasketches `ThetaSketch` objects, that is the elements
/// that are in `col1` but not in `col2`, using a Datasketches `AnotB` object.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch` to
///     subtract from `col1`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_difference(_ col1: Column, _ col2: Column) -> Column {
  return fn("theta_difference", col1, col2)
}

/// Returns the intersection of two Datasketches `ThetaSketch` objects, using a Datasketches
/// `Intersection` object.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_intersection(_ col1: Column, _ col2: Column) -> Column {
  return fn("theta_intersection", col1, col2)
}

/// Returns the estimated number of unique values in a Datasketches `ThetaSketch`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter col: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
/// - Returns: A ``Column`` that evaluates to the estimated number of unique values.
public func theta_sketch_estimate(_ col: Column) -> Column {
  return fn("theta_sketch_estimate", col)
}

/// Merges two Datasketches `ThetaSketch` objects using a Datasketches `Union` object, using the
/// server-side default of 12 nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_union(_ col1: Column, _ col2: Column) -> Column {
  return fn("theta_union", col1, col2)
}

/// Merges two Datasketches `ThetaSketch` objects using a Datasketches `Union` object configured
/// with `lgNomEntries` nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - lgNomEntries: The log-base-2 of the number of nominal entries used by the union
///     operation. Must be between 4 and 26.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_union(_ col1: Column, _ col2: Column, _ lgNomEntries: Int32) -> Column {
  return theta_union(col1, col2, lit(lgNomEntries))
}

/// Merges two Datasketches `ThetaSketch` objects using a Datasketches `Union` object configured
/// with `lgNomEntries` nominal entries.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - col1: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - col2: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
///   - lgNomEntries: A ``Column`` that evaluates to the log-base-2 of the number of nominal
///     entries used by the union operation. Must be a constant between 4 and 26.
/// - Returns: A ``Column`` that evaluates to the binary representation of a `ThetaSketch`.
public func theta_union(_ col1: Column, _ col2: Column, _ lgNomEntries: Column) -> Column {
  return fn("theta_union", col1, col2, lgNomEntries)
}
