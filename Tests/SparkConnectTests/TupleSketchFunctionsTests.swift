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

/// A test suite for Tuple sketch functions
@Suite(.serialized)
struct TupleSketchFunctionsTests {

  /// Like PySpark, the functions that take both `lgNomEntries` and `mode` always send them,
  /// while the ones that only take `mode` omit it when it is not given.
  @Test
  func tupleSketchFunctions() throws {
    for (column, name, count) in [
      (tuple_sketch_agg_double(col("a"), col("b")), "tuple_sketch_agg_double", 4),
      (
        tuple_sketch_agg_double(col("a"), col("b"), lgNomEntries: lit(Int32(15))),
        "tuple_sketch_agg_double", 4
      ),
      (
        tuple_sketch_agg_double(col("a"), col("b"), mode: lit("min")),
        "tuple_sketch_agg_double", 4
      ),
      (tuple_union_agg_double(col("a")), "tuple_union_agg_double", 3),
      (tuple_union_agg_double(col("a"), mode: lit("max")), "tuple_union_agg_double", 3),
      (tuple_intersection_agg_double(col("a")), "tuple_intersection_agg_double", 1),
      (
        tuple_intersection_agg_double(col("a"), mode: lit("min")),
        "tuple_intersection_agg_double", 2
      ),
      (tuple_sketch_estimate_double(col("a")), "tuple_sketch_estimate_double", 1),
      (tuple_sketch_summary_double(col("a")), "tuple_sketch_summary_double", 1),
      (tuple_sketch_summary_double(col("a"), mode: lit("max")), "tuple_sketch_summary_double", 2),
      (tuple_sketch_theta_double(col("a")), "tuple_sketch_theta_double", 1),
      (tuple_union_double(col("a"), col("b")), "tuple_union_double", 4),
      (tuple_union_theta_double(col("a"), col("b")), "tuple_union_theta_double", 4),
      (tuple_intersection_double(col("a"), col("b")), "tuple_intersection_double", 2),
      (
        tuple_intersection_double(col("a"), col("b"), mode: lit("min")),
        "tuple_intersection_double", 3
      ),
      (tuple_intersection_theta_double(col("a"), col("b")), "tuple_intersection_theta_double", 2),
      (
        tuple_intersection_theta_double(col("a"), col("b"), mode: lit("min")),
        "tuple_intersection_theta_double", 3
      ),
      (tuple_difference_double(col("a"), col("b")), "tuple_difference_double", 2),
      (tuple_difference_theta_double(col("a"), col("b")), "tuple_difference_theta_double", 2),
      (tuple_sketch_agg_integer(col("a"), col("b")), "tuple_sketch_agg_integer", 4),
      (
        tuple_sketch_agg_integer(col("a"), col("b"), lgNomEntries: lit(Int32(15))),
        "tuple_sketch_agg_integer", 4
      ),
      (
        tuple_sketch_agg_integer(col("a"), col("b"), mode: lit("min")),
        "tuple_sketch_agg_integer", 4
      ),
      (tuple_union_agg_integer(col("a")), "tuple_union_agg_integer", 3),
      (tuple_union_agg_integer(col("a"), mode: lit("max")), "tuple_union_agg_integer", 3),
      (tuple_intersection_agg_integer(col("a")), "tuple_intersection_agg_integer", 1),
      (
        tuple_intersection_agg_integer(col("a"), mode: lit("min")),
        "tuple_intersection_agg_integer", 2
      ),
      (tuple_sketch_estimate_integer(col("a")), "tuple_sketch_estimate_integer", 1),
      (tuple_sketch_summary_integer(col("a")), "tuple_sketch_summary_integer", 1),
      (
        tuple_sketch_summary_integer(col("a"), mode: lit("max")),
        "tuple_sketch_summary_integer", 2
      ),
      (tuple_sketch_theta_integer(col("a")), "tuple_sketch_theta_integer", 1),
      (tuple_union_integer(col("a"), col("b")), "tuple_union_integer", 4),
      (tuple_union_theta_integer(col("a"), col("b")), "tuple_union_theta_integer", 4),
      (tuple_intersection_integer(col("a"), col("b")), "tuple_intersection_integer", 2),
      (
        tuple_intersection_integer(col("a"), col("b"), mode: lit("min")),
        "tuple_intersection_integer", 3
      ),
      (
        tuple_intersection_theta_integer(col("a"), col("b")),
        "tuple_intersection_theta_integer", 2
      ),
      (
        tuple_intersection_theta_integer(col("a"), col("b"), mode: lit("min")),
        "tuple_intersection_theta_integer", 3
      ),
      (tuple_difference_integer(col("a"), col("b")), "tuple_difference_integer", 2),
      (tuple_difference_theta_integer(col("a"), col("b")), "tuple_difference_theta_integer", 2),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  /// An `Int32` `lgNomEntries` is a shorthand for a literal ``Column``.
  @Test
  func tupleLgNomEntries() throws {
    for (column, name, index) in [
      (
        tuple_sketch_agg_double(col("a"), col("b"), lgNomEntries: 15),
        "tuple_sketch_agg_double", 2
      ),
      (tuple_union_agg_double(col("a"), lgNomEntries: 15), "tuple_union_agg_double", 1),
      (tuple_union_double(col("a"), col("b"), lgNomEntries: 15), "tuple_union_double", 2),
      (
        tuple_union_theta_double(col("a"), col("b"), lgNomEntries: 15),
        "tuple_union_theta_double", 2
      ),
      (
        tuple_sketch_agg_integer(col("a"), col("b"), lgNomEntries: 15),
        "tuple_sketch_agg_integer", 2
      ),
      (tuple_union_agg_integer(col("a"), lgNomEntries: 15), "tuple_union_agg_integer", 1),
      (tuple_union_integer(col("a"), col("b"), lgNomEntries: 15), "tuple_union_integer", 2),
      (
        tuple_union_theta_integer(col("a"), col("b"), lgNomEntries: 15),
        "tuple_union_theta_integer", 2
      ),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[index].literal.integer == 15)
    }
  }

  /// The omitted `lgNomEntries` and `mode` are filled with their default literals.
  @Test
  func tupleDefaultArguments() throws {
    let expr = tuple_sketch_agg_double(col("a"), col("b")).expr
    #expect(expr.unresolvedFunction.arguments[2].literal.integer == 12)
    #expect(expr.unresolvedFunction.arguments[3].literal.string == "sum")
  }

  @Test
  func tupleSketchAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let df = try await spark.sql(
        "SELECT * FROM VALUES (1, 10.0), (2, 20.0), (2, 30.0) AS T(k, v)")
      let rows = try await df.select(
        tuple_sketch_estimate_double(tuple_sketch_agg_double(col("k"), col("v"))),
        tuple_sketch_estimate_integer(
          tuple_sketch_agg_integer(col("k"), col("v").cast("INT"), lgNomEntries: 15)),
        tuple_sketch_summary_double(tuple_sketch_agg_double(col("k"), col("v"))),
        tuple_sketch_summary_double(
          tuple_sketch_agg_double(col("k"), col("v"), mode: lit("min"))),
        tuple_sketch_theta_double(tuple_sketch_agg_double(col("k"), col("v")))
      ).collect()
      #expect(rows == [Row(2.0, 2.0, 60.0, 30.0, 1.0)])
    }
    await spark.stop()
  }

  @Test
  func tupleSetOperations() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let df = try await spark.sql(
        """
        SELECT * FROM VALUES (1, 10.0, 2, 100.0), (2, 20.0, 3, 200.0), (3, 30.0, 4, 300.0)
        AS T(k1, v1, k2, v2)
        """)
      let sketches = await df.select(
        tuple_sketch_agg_double(col("k1"), col("v1")).alias("a"),
        tuple_sketch_agg_double(col("k2"), col("v2")).alias("b"),
        theta_sketch_agg(col("k2")).alias("t")
      )
      let rows = try await sketches.select(
        tuple_sketch_estimate_double(tuple_union_double(col("a"), col("b"))),
        tuple_sketch_estimate_double(tuple_intersection_double(col("a"), col("b"))),
        tuple_sketch_estimate_double(tuple_difference_double(col("a"), col("b"))),
        tuple_sketch_estimate_double(tuple_union_theta_double(col("a"), col("t"))),
        tuple_sketch_estimate_double(tuple_intersection_theta_double(col("a"), col("t"))),
        tuple_sketch_estimate_double(tuple_difference_theta_double(col("a"), col("t"))),
        tuple_sketch_summary_double(tuple_union_double(col("a"), col("b"))),
        tuple_sketch_summary_double(
          tuple_intersection_double(col("a"), col("b"), mode: lit("min")))
      ).collect()
      #expect(rows == [Row(4.0, 2.0, 1.0, 4.0, 2.0, 1.0, 660.0, 50.0)])
    }
    await spark.stop()
  }

  @Test
  func tupleUnionAndIntersectionAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let df = try await spark.sql(
        """
        SELECT tuple_sketch_agg_integer(k, v) AS sketch FROM VALUES (1, 10), (2, 20) AS T(k, v)
        UNION ALL
        SELECT tuple_sketch_agg_integer(k, v) AS sketch FROM VALUES (2, 5), (3, 30) AS T(k, v)
        """)
      let rows = try await df.select(
        tuple_sketch_estimate_integer(tuple_union_agg_integer(col("sketch"))),
        tuple_sketch_estimate_integer(tuple_intersection_agg_integer(col("sketch"))),
        tuple_sketch_summary_integer(tuple_union_agg_integer(col("sketch"), lgNomEntries: 15)),
        tuple_sketch_summary_integer(
          tuple_intersection_agg_integer(col("sketch"), mode: lit("max")))
      ).collect()
      #expect(rows == [Row(3.0, 1.0, Int64(65), Int64(20))])
    }
    await spark.stop()
  }
}
