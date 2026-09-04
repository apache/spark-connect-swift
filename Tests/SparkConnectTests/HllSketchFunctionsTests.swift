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

/// A test suite for HLL sketch functions
@Suite(.serialized)
struct HllSketchFunctionsTests {

  @Test
  func hllSketchFunctions() throws {
    for (column, name, count) in [
      (hll_sketch_agg(col("a")), "hll_sketch_agg", 1),
      (hll_sketch_agg(col("a"), lit(Int32(15))), "hll_sketch_agg", 2),
      (hll_union_agg(col("a")), "hll_union_agg", 1),
      (hll_union_agg(col("a"), lit(true)), "hll_union_agg", 2),
      (hll_sketch_estimate(col("a")), "hll_sketch_estimate", 1),
      (hll_union(col("a"), col("b")), "hll_union", 2),
      (hll_union(col("a"), col("b"), true), "hll_union", 3),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  /// An `Int32` `lgConfigK` is a shorthand for a literal ``Column``. The server requires the
  /// `INT` type here, so the literal must not be a `BIGINT`.
  @Test
  func hllLgConfigK() throws {
    let expr = hll_sketch_agg(col("a"), 15).expr
    #expect(expr.unresolvedFunction.functionName == "hll_sketch_agg")
    #expect(expr.unresolvedFunction.arguments[1].literal.integer == 15)
  }

  /// `allowDifferentLgConfigK` is a `Bool` shorthand for a literal ``Column`` in `hll_union_agg`,
  /// while `hll_union` accepts a `Bool` only, following the upstream signatures.
  @Test
  func hllAllowDifferentLgConfigK() throws {
    for (column, name, index) in [
      (hll_union_agg(col("a"), true), "hll_union_agg", 1),
      (hll_union(col("a"), col("b"), true), "hll_union", 2),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[index].literal.boolean == true)
    }
  }

  @Test
  func hllSketchAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (2), (3) AS T(v)")
    let rows = try await df.select(
      hll_sketch_estimate(hll_sketch_agg(col("v"))),
      hll_sketch_estimate(hll_sketch_agg(col("v"), 15)),
      hll_sketch_estimate(hll_sketch_agg(col("v"), lit(Int32(15))))
    ).collect()
    // The sketch is approximate, but it is exact for such a small number of distinct values.
    #expect(rows == [Row(Int64(3), Int64(3), Int64(3))])
    await spark.stop()
  }

  @Test
  func hllUnionAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      """
      SELECT hll_sketch_agg(v) AS sketch FROM VALUES (1), (2), (2), (3) AS T(v)
      UNION ALL
      SELECT hll_sketch_agg(v) AS sketch FROM VALUES (2), (3), (3), (4) AS T(v)
      """)
    // The two sketches hold {1, 2, 3} and {2, 3, 4}, so the union has 4 distinct values.
    let rows = try await df.select(
      hll_sketch_estimate(hll_union_agg(col("sketch"))),
      hll_sketch_estimate(hll_union_agg(col("sketch"), false)),
      hll_sketch_estimate(hll_union_agg(col("sketch"), lit(false)))
    ).collect()
    #expect(rows == [Row(Int64(4), Int64(4), Int64(4))])
    await spark.stop()
  }

  @Test
  func hllUnion() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES (1, 3), (2, 4), (3, 5), (4, 6) AS T(v1, v2)")
    let sketches = await df.select(
      hll_sketch_agg(col("v1")).alias("s1"), hll_sketch_agg(col("v2")).alias("s2"))
    // The two sketches hold {1, 2, 3, 4} and {3, 4, 5, 6}, so the union has 6 distinct values.
    let rows = try await sketches.select(
      hll_sketch_estimate(hll_union(col("s1"), col("s2"))),
      hll_sketch_estimate(hll_union(col("s1"), col("s2"), false))
    ).collect()
    #expect(rows == [Row(Int64(6), Int64(6))])
    await spark.stop()
  }
}
