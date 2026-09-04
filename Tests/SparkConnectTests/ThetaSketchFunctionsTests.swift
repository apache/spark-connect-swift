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

/// A test suite for Theta sketch functions
@Suite(.serialized)
struct ThetaSketchFunctionsTests {

  @Test
  func thetaSketchFunctions() throws {
    for (column, name, count) in [
      (theta_sketch_agg(col("a")), "theta_sketch_agg", 1),
      (theta_sketch_agg(col("a"), lit(Int32(15))), "theta_sketch_agg", 2),
      (theta_union_agg(col("a")), "theta_union_agg", 1),
      (theta_union_agg(col("a"), lit(Int32(15))), "theta_union_agg", 2),
      (theta_intersection_agg(col("a")), "theta_intersection_agg", 1),
      (theta_sketch_estimate(col("a")), "theta_sketch_estimate", 1),
      (theta_union(col("a"), col("b")), "theta_union", 2),
      (theta_union(col("a"), col("b"), lit(Int32(15))), "theta_union", 3),
      (theta_intersection(col("a"), col("b")), "theta_intersection", 2),
      (theta_difference(col("a"), col("b")), "theta_difference", 2),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  /// An `Int32` `lgNomEntries` is a shorthand for a literal ``Column``. The server requires the
  /// `INT` type here, so the literal must not be a `BIGINT`.
  @Test
  func thetaLgNomEntries() throws {
    for (column, name, index) in [
      (theta_sketch_agg(col("a"), 15), "theta_sketch_agg", 1),
      (theta_union_agg(col("a"), 15), "theta_union_agg", 1),
      (theta_union(col("a"), col("b"), 15), "theta_union", 2),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[index].literal.integer == 15)
    }
  }

  @Test
  func thetaSketchAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1") {
      let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (2), (3) AS T(v)")
      let rows = try await df.select(
        theta_sketch_estimate(theta_sketch_agg(col("v"))),
        theta_sketch_estimate(theta_sketch_agg(col("v"), 15)),
        theta_sketch_estimate(theta_sketch_agg(col("v"), lit(Int32(15))))
      ).collect()
      #expect(rows == [Row(Int64(3), Int64(3), Int64(3))])
    }
    await spark.stop()
  }

  @Test
  func thetaUnionAndIntersectionAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1") {
      let df = try await spark.sql(
        """
        SELECT theta_sketch_agg(v) AS sketch FROM VALUES (1), (2), (2), (3) AS T(v)
        UNION ALL
        SELECT theta_sketch_agg(v) AS sketch FROM VALUES (2), (3), (3), (4) AS T(v)
        """)
      let rows = try await df.select(
        theta_sketch_estimate(theta_union_agg(col("sketch"))),
        theta_sketch_estimate(theta_union_agg(col("sketch"), 15)),
        theta_sketch_estimate(theta_intersection_agg(col("sketch")))
      ).collect()
      #expect(rows == [Row(Int64(4), Int64(4), Int64(2))])
    }
    await spark.stop()
  }

  @Test
  func thetaSetOperations() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1") {
      let df = try await spark.sql(
        "SELECT * FROM VALUES (1, 3), (2, 4), (3, 5), (4, 6) AS T(v1, v2)")
      let sketches = await df.select(
        theta_sketch_agg(col("v1")).alias("s1"), theta_sketch_agg(col("v2")).alias("s2"))
      let rows = try await sketches.select(
        theta_sketch_estimate(theta_union(col("s1"), col("s2"))),
        theta_sketch_estimate(theta_union(col("s1"), col("s2"), 15)),
        theta_sketch_estimate(theta_intersection(col("s1"), col("s2"))),
        theta_sketch_estimate(theta_difference(col("s1"), col("s2")))
      ).collect()
      #expect(rows == [Row(Int64(6), Int64(6), Int64(2), Int64(2))])
    }
    await spark.stop()
  }
}
