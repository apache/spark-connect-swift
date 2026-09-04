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

/// A test suite for KLL sketch functions
@Suite(.serialized)
struct KllSketchFunctionsTests {

  @Test
  func kllSketchFunctions() throws {
    for (column, name, count) in [
      (kll_sketch_agg_bigint(col("a")), "kll_sketch_agg_bigint", 1),
      (kll_sketch_agg_bigint(col("a"), lit(Int32(300))), "kll_sketch_agg_bigint", 2),
      (kll_merge_agg_bigint(col("a")), "kll_merge_agg_bigint", 1),
      (kll_merge_agg_bigint(col("a"), lit(Int32(300))), "kll_merge_agg_bigint", 2),
      (kll_sketch_get_n_bigint(col("a")), "kll_sketch_get_n_bigint", 1),
      (kll_sketch_get_quantile_bigint(col("a"), col("b")), "kll_sketch_get_quantile_bigint", 2),
      (kll_sketch_get_rank_bigint(col("a"), col("b")), "kll_sketch_get_rank_bigint", 2),
      (kll_sketch_merge_bigint(col("a"), col("b")), "kll_sketch_merge_bigint", 2),
      (kll_sketch_to_string_bigint(col("a")), "kll_sketch_to_string_bigint", 1),
      (kll_sketch_agg_double(col("a")), "kll_sketch_agg_double", 1),
      (kll_sketch_agg_double(col("a"), lit(Int32(300))), "kll_sketch_agg_double", 2),
      (kll_merge_agg_double(col("a")), "kll_merge_agg_double", 1),
      (kll_merge_agg_double(col("a"), lit(Int32(300))), "kll_merge_agg_double", 2),
      (kll_sketch_get_n_double(col("a")), "kll_sketch_get_n_double", 1),
      (kll_sketch_get_quantile_double(col("a"), col("b")), "kll_sketch_get_quantile_double", 2),
      (kll_sketch_get_rank_double(col("a"), col("b")), "kll_sketch_get_rank_double", 2),
      (kll_sketch_merge_double(col("a"), col("b")), "kll_sketch_merge_double", 2),
      (kll_sketch_to_string_double(col("a")), "kll_sketch_to_string_double", 1),
      (kll_sketch_agg_float(col("a")), "kll_sketch_agg_float", 1),
      (kll_sketch_agg_float(col("a"), lit(Int32(300))), "kll_sketch_agg_float", 2),
      (kll_merge_agg_float(col("a")), "kll_merge_agg_float", 1),
      (kll_merge_agg_float(col("a"), lit(Int32(300))), "kll_merge_agg_float", 2),
      (kll_sketch_get_n_float(col("a")), "kll_sketch_get_n_float", 1),
      (kll_sketch_get_quantile_float(col("a"), col("b")), "kll_sketch_get_quantile_float", 2),
      (kll_sketch_get_rank_float(col("a"), col("b")), "kll_sketch_get_rank_float", 2),
      (kll_sketch_merge_float(col("a"), col("b")), "kll_sketch_merge_float", 2),
      (kll_sketch_to_string_float(col("a")), "kll_sketch_to_string_float", 1),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  /// An `Int32` `k` is a shorthand for a literal ``Column``. The server requires the `INT` type
  /// here, so the literal must not be a `BIGINT`.
  @Test
  func kllK() throws {
    for (column, name) in [
      (kll_sketch_agg_bigint(col("a"), 300), "kll_sketch_agg_bigint"),
      (kll_merge_agg_bigint(col("a"), 300), "kll_merge_agg_bigint"),
      (kll_sketch_agg_double(col("a"), 300), "kll_sketch_agg_double"),
      (kll_merge_agg_double(col("a"), 300), "kll_merge_agg_double"),
      (kll_sketch_agg_float(col("a"), 300), "kll_sketch_agg_float"),
      (kll_merge_agg_float(col("a"), 300), "kll_merge_agg_float"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[1].literal.integer == 300)
    }
  }

  /// The sketches are exact here because five items fit well within the default `k` of 200.
  @Test
  func kllSketchAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1") {
      let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (3), (4), (5) AS T(v)")
      let sketches = await df.select(
        kll_sketch_agg_bigint(col("v")).alias("b"),
        kll_sketch_agg_double(col("v").cast("DOUBLE")).alias("d"),
        kll_sketch_agg_float(col("v").cast("FLOAT"), 300).alias("f")
      )
      let rows = try await sketches.select(
        kll_sketch_get_n_bigint(col("b")),
        kll_sketch_get_n_double(col("d")),
        kll_sketch_get_n_float(col("f")),
        kll_sketch_get_quantile_bigint(col("b"), lit(0.5)),
        kll_sketch_get_quantile_double(col("d"), lit(0.5)),
        kll_sketch_get_quantile_float(col("f"), lit(0.5)),
        kll_sketch_get_rank_bigint(col("b"), lit(3)),
        kll_sketch_get_rank_double(col("d"), lit(3.0)),
        kll_sketch_get_rank_float(col("f"), lit(Float(3.0)))
      ).collect()
      #expect(
        rows == [
          Row(
            Int64(5), Int64(5), Int64(5), Int64(3), 3.0, Float(3.0),
            0.6, 0.6, 0.6)
        ])
    }
    await spark.stop()
  }

  /// A `rank` or `quantile` array yields an array of results.
  @Test
  func kllSketchArrayArgument() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1") {
      let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (3), (4), (5) AS T(v)")
      let sketches = await df.select(kll_sketch_agg_bigint(col("v")).alias("b"))
      let rows = try await sketches.select(
        kll_sketch_get_quantile_bigint(col("b"), array(lit(0.5), lit(1.0))).cast("STRING"),
        kll_sketch_get_rank_bigint(col("b"), array(lit(2), lit(4))).cast("STRING")
      ).collect()
      #expect(rows == [Row("[3, 5]", "[0.4, 0.8]")])
    }
    await spark.stop()
  }

  @Test
  func kllSketchMerge() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1") {
      let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (3), (4), (5) AS T(v)")
      let sketches = await df.select(
        kll_sketch_agg_bigint(col("v")).alias("b"),
        kll_sketch_agg_double(col("v").cast("DOUBLE")).alias("d"),
        kll_sketch_agg_float(col("v").cast("FLOAT")).alias("f")
      )
      let rows = try await sketches.select(
        kll_sketch_get_n_bigint(kll_sketch_merge_bigint(col("b"), col("b"))),
        kll_sketch_get_n_double(kll_sketch_merge_double(col("d"), col("d"))),
        kll_sketch_get_n_float(kll_sketch_merge_float(col("f"), col("f")))
      ).collect()
      #expect(rows == [Row(Int64(10), Int64(10), Int64(10))])
    }
    await spark.stop()
  }

  /// `kll_merge_agg_*` is introduced later than the other KLL functions.
  @Test
  func kllMergeAgg() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1.2") {
      let df = try await spark.sql(
        """
        SELECT kll_sketch_agg_bigint(v) AS sketch FROM VALUES (1), (2), (3) AS T(v)
        UNION ALL
        SELECT kll_sketch_agg_bigint(v) AS sketch FROM VALUES (4), (5), (6) AS T(v)
        """)
      let rows = try await df.select(
        kll_sketch_get_n_bigint(kll_merge_agg_bigint(col("sketch"))),
        kll_sketch_get_n_bigint(kll_merge_agg_bigint(col("sketch"), 300)),
        kll_sketch_get_n_bigint(kll_merge_agg_bigint(col("sketch"), lit(Int32(300))))
      ).collect()
      #expect(rows == [Row(Int64(6), Int64(6), Int64(6))])
    }
    await spark.stop()
  }

  /// The summary is a human readable string, so only its shape is checked.
  @Test
  func kllSketchToString() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1") {
      let df = try await spark.sql("SELECT * FROM VALUES (1), (2), (3), (4), (5) AS T(v)")
      let sketches = await df.select(
        kll_sketch_agg_bigint(col("v")).alias("b"),
        kll_sketch_agg_double(col("v").cast("DOUBLE")).alias("d"),
        kll_sketch_agg_float(col("v").cast("FLOAT")).alias("f")
      )
      let rows = try await sketches.select(
        kll_sketch_to_string_bigint(col("b")),
        kll_sketch_to_string_double(col("d")),
        kll_sketch_to_string_float(col("f"))
      ).collect()
      #expect(rows.count == 1)
      for i in 0..<3 {
        let summary = try #require(rows[0][i] as? String)
        #expect(summary.contains("Summary"))
      }
    }
    await spark.stop()
  }
}
