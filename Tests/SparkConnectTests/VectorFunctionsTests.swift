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

import Testing

@testable import SparkConnect

/// A test suite for `VectorFunctions`
@Suite(.serialized)
struct VectorFunctionsTests {

  @Test
  func vectorFunctions() throws {
    for (column, name, count) in [
      (vector_cosine_similarity(col("a"), col("b")), "vector_cosine_similarity", 2),
      (vector_inner_product(col("a"), col("b")), "vector_inner_product", 2),
      (vector_l2_distance(col("a"), col("b")), "vector_l2_distance", 2),
      (vector_norm(col("a")), "vector_norm", 1),
      (vector_norm(col("a"), col("b")), "vector_norm", 2),
      (vector_normalize(col("a")), "vector_normalize", 1),
      (vector_normalize(col("a"), col("b")), "vector_normalize", 2),
      (vector_avg(col("a")), "vector_avg", 1),
      (vector_sum(col("a")), "vector_sum", 1),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  /// A `Float` degree is a shorthand for a literal ``Column`` degree.
  @Test
  func vectorDegree() throws {
    for (column, name) in [
      (vector_norm(col("a"), 1.0), "vector_norm"),
      (vector_normalize(col("a"), 1.0), "vector_normalize"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[1].literal.float == 1.0)
    }
  }

  @Test
  func vectorDistanceFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let x = array(lit(Float(1.0)), lit(Float(0.0)))
      let y = array(lit(Float(0.0)), lit(Float(1.0)))
      let p = array(lit(Float(1.0)), lit(Float(2.0)))
      let q = array(lit(Float(4.0)), lit(Float(6.0)))
      let rows = try await spark.range(1).select(
        vector_cosine_similarity(x, y), vector_inner_product(x, y), vector_l2_distance(p, q)
      ).collect()
      #expect(rows == [Row(Float(0.0), Float(0.0), Float(5.0))])
    }
    await spark.stop()
  }

  @Test
  func vectorNorm() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let v = array(lit(Float(3.0)), lit(Float(4.0)))
      let rows = try await spark.range(1).select(
        vector_norm(v), vector_norm(v, 1.0), vector_norm(v, lit(Float.infinity))
      ).collect()
      #expect(rows == [Row(Float(5.0), Float(7.0), Float(4.0))])
    }
    await spark.stop()
  }

  @Test
  func vectorNormalize() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let v = array(lit(Float(3.0)), lit(Float(4.0)))
      let rows = try await spark.range(1).select(
        vector_normalize(v), vector_normalize(v, 1.0)
      ).collect()
      let l2 = try #require(rows[0].get(0) as? [Float])
      let l1 = try #require(rows[0].get(1) as? [Float])
      #expect(zip(l2, [0.6, 0.8]).allSatisfy { abs($0 - $1) < 1e-6 })
      #expect(zip(l1, [3.0 / 7.0, 4.0 / 7.0]).allSatisfy { abs($0 - $1) < 1e-6 })
    }
    await spark.stop()
  }

  @Test
  func vectorAggregateFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let df = try await spark.sql(
        """
        SELECT * FROM VALUES
          ('a', ARRAY(1.0f, 2.0f)), ('a', ARRAY(3.0f, 4.0f)), ('b', ARRAY(5.0f, 6.0f)) AS T(k, v)
        """)
      let rows = try await df.groupBy("k")
        .agg(vector_sum(col("v")).cast("string"), vector_avg(col("v")).cast("string"))
        .orderBy("k").collect()
      #expect(rows == [Row("a", "[4.0, 6.0]", "[2.0, 3.0]"), Row("b", "[5.0, 6.0]", "[5.0, 6.0]")])
    }
    await spark.stop()
  }
}
