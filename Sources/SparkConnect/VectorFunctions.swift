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

// MARK: - Vector functions

/// Returns the cosine similarity between two float vectors.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameters:
///   - left: The first vector. A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
///   - right: The second vector. A ``Column`` that evaluates to an `ARRAY<FLOAT>` of the same
///     dimension as `left`.
/// - Returns: A ``Column`` that evaluates to a float.
public func vector_cosine_similarity(_ left: Column, _ right: Column) -> Column {
  return fn("vector_cosine_similarity", left, right)
}

/// Returns the inner product (dot product) between two float vectors.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameters:
///   - left: The first vector. A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
///   - right: The second vector. A ``Column`` that evaluates to an `ARRAY<FLOAT>` of the same
///     dimension as `left`.
/// - Returns: A ``Column`` that evaluates to a float.
public func vector_inner_product(_ left: Column, _ right: Column) -> Column {
  return fn("vector_inner_product", left, right)
}

/// Returns the Euclidean (L2) distance between two float vectors.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameters:
///   - left: The first vector. A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
///   - right: The second vector. A ``Column`` that evaluates to an `ARRAY<FLOAT>` of the same
///     dimension as `left`.
/// - Returns: A ``Column`` that evaluates to a float.
public func vector_l2_distance(_ left: Column, _ right: Column) -> Column {
  return fn("vector_l2_distance", left, right)
}

/// Returns the Lp norm of a float vector using degree `2.0`, i.e. the Euclidean norm.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameter vector: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
/// - Returns: A ``Column`` that evaluates to a float.
public func vector_norm(_ vector: Column) -> Column {
  return fn("vector_norm", vector)
}

/// Returns the Lp norm of a float vector using the specified degree.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameters:
///   - vector: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
///   - degree: A ``Column`` that evaluates to a float, `1.0` for the L1 norm, `2.0` for the L2
///     norm, or infinity for the infinity norm.
/// - Returns: A ``Column`` that evaluates to a float.
public func vector_norm(_ vector: Column, _ degree: Column) -> Column {
  return fn("vector_norm", vector, degree)
}

/// Returns the Lp norm of a float vector using the specified degree.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameters:
///   - vector: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
///   - degree: The norm degree, `1.0` for the L1 norm, `2.0` for the L2 norm, or
///     `Float.infinity` for the infinity norm.
/// - Returns: A ``Column`` that evaluates to a float.
public func vector_norm(_ vector: Column, _ degree: Float) -> Column {
  return fn("vector_norm", vector, lit(degree))
}

/// Normalizes a float vector to unit length using degree `2.0`, i.e. the Euclidean norm.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameter vector: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
/// - Returns: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
public func vector_normalize(_ vector: Column) -> Column {
  return fn("vector_normalize", vector)
}

/// Normalizes a float vector to unit length using the specified degree.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameters:
///   - vector: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
///   - degree: A ``Column`` that evaluates to a float, `1.0` for the L1 norm, `2.0` for the L2
///     norm, or infinity for the infinity norm.
/// - Returns: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
public func vector_normalize(_ vector: Column, _ degree: Column) -> Column {
  return fn("vector_normalize", vector, degree)
}

/// Normalizes a float vector to unit length using the specified degree.
/// This requires Apache Spark 4.3.0 or later.
/// - Parameters:
///   - vector: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
///   - degree: The norm degree, `1.0` for the L1 norm, `2.0` for the L2 norm, or
///     `Float.infinity` for the infinity norm.
/// - Returns: A ``Column`` that evaluates to an `ARRAY<FLOAT>`.
public func vector_normalize(_ vector: Column, _ degree: Float) -> Column {
  return fn("vector_normalize", vector, lit(degree))
}
