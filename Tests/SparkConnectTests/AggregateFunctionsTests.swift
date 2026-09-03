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

/// A test suite for `AggregateFunctions`
@Suite(.serialized)
struct AggregateFunctionsTests {

  @Test
  func aggregateFunctions() throws {
    for (column, name) in [
      (any_value(col("a")), "any_value"),
      (approx_count_distinct(col("a")), "approx_count_distinct"),
      (array_agg(col("a")), "array_agg"),
      (bit_and(col("a")), "bit_and"),
      (bit_or(col("a")), "bit_or"),
      (bit_xor(col("a")), "bit_xor"),
      (bool_and(col("a")), "bool_and"),
      (bool_or(col("a")), "bool_or"),
      (collect_list(col("a")), "collect_list"),
      (collect_set(col("a")), "collect_set"),
      (collect_union(col("a")), "collect_union"),
      (count_if(col("a")), "count_if"),
      (every(col("a")), "every"),
      (first_value(col("a")), "first_value"),
      (grouping(col("a")), "grouping"),
      (grouping_id(col("a")), "grouping_id"),
      (kurtosis(col("a")), "kurtosis"),
      (last_value(col("a")), "last_value"),
      (listagg(col("a")), "listagg"),
      (median(col("a")), "median"),
      (mode(col("a")), "mode"),
      (product(col("a")), "product"),
      (skewness(col("a")), "skewness"),
      (some(col("a")), "some"),
      (std(col("a")), "std"),
      (stddev(col("a")), "stddev"),
      (stddev_pop(col("a")), "stddev_pop"),
      (stddev_samp(col("a")), "stddev_samp"),
      (string_agg(col("a")), "string_agg"),
      (try_avg(col("a")), "try_avg"),
      (try_sum(col("a")), "try_sum"),
      (var_pop(col("a")), "var_pop"),
      (var_samp(col("a")), "var_samp"),
      (variance(col("a")), "variance"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.isDistinct == false)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func aggregateFunctionArguments() throws {
    for (column, name) in [
      (any_value(col("a"), lit(true)), "any_value"),
      (corr(col("a"), col("b")), "corr"),
      (covar_pop(col("a"), col("b")), "covar_pop"),
      (covar_samp(col("a"), col("b")), "covar_samp"),
      (max_by(col("a"), col("b")), "max_by"),
      (min_by(col("a"), col("b")), "min_by"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }

    let rsd = approx_count_distinct(col("a"), 0.01).expr
    #expect(rsd.unresolvedFunction.functionName == "approx_count_distinct")
    #expect(rsd.unresolvedFunction.arguments[1].literal.double == 0.01)

    let deterministic = mode(col("a"), true).expr
    #expect(deterministic.unresolvedFunction.functionName == "mode")
    #expect(deterministic.unresolvedFunction.arguments[1].literal.boolean == true)

    let percentile = percentile_approx(col("a"), lit(0.5), lit(100)).expr
    #expect(percentile.unresolvedFunction.functionName == "percentile_approx")
    #expect(percentile.unresolvedFunction.arguments.count == 3)

    #expect(grouping_id(col("a"), col("b")).expr.unresolvedFunction.arguments.count == 2)

    let bins = histogram_numeric(col("a"), 5).expr
    #expect(bins.unresolvedFunction.functionName == "histogram_numeric")
    #expect(bins.unresolvedFunction.arguments[1].literal.integer == 5)

    let sketch = count_min_sketch(col("a"), 3.0, 0.1).expr
    #expect(sketch.unresolvedFunction.functionName == "count_min_sketch")
    #expect(sketch.unresolvedFunction.arguments.count == 4)
    #expect(sketch.unresolvedFunction.arguments[1].literal.double == 3.0)
    #expect(sketch.unresolvedFunction.arguments[2].literal.double == 0.1)

    let seeded = count_min_sketch(col("a"), 3.0, 0.1, 1).expr
    #expect(seeded.unresolvedFunction.functionName == "count_min_sketch")
    #expect(seeded.unresolvedFunction.arguments.count == 4)
    #expect(seeded.unresolvedFunction.arguments[3].literal.long == 1)

    for (column, name, isDistinct) in [
      (listagg(col("a"), ","), "listagg", false),
      (listagg_distinct(col("a"), ","), "listagg", true),
      (string_agg(col("a"), ","), "string_agg", false),
      (string_agg_distinct(col("a"), ","), "string_agg", true),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.isDistinct == isDistinct)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[1].literal.string == ",")
    }
  }

  @Test
  func percentileFunctions() throws {
    for (column, name) in [
      (approx_percentile(col("a"), 0.5), "approx_percentile"),
      (percentile(col("a"), 0.5), "percentile"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 3)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].literal.double == 0.5)
    }

    #expect(approx_percentile(col("a"), 0.5).expr.unresolvedFunction.arguments[2].literal.integer
      == 10000)
    #expect(approx_percentile(col("a"), 0.5, 100).expr.unresolvedFunction.arguments[2].literal
      .integer == 100)
    #expect(percentile(col("a"), 0.5).expr.unresolvedFunction.arguments[2].literal.integer == 1)
    #expect(percentile(col("a"), 0.5, 2).expr.unresolvedFunction.arguments[2].literal.integer == 2)

    for column in [
      approx_percentile(col("a"), [0.25, 0.75]),
      percentile(col("a"), [0.25, 0.75]),
    ] {
      let percentages = column.expr.unresolvedFunction.arguments[1].unresolvedFunction
      #expect(percentages.functionName == "array")
      #expect(percentages.arguments.count == 2)
      #expect(percentages.arguments[0].literal.double == 0.25)
      #expect(percentages.arguments[1].literal.double == 0.75)
    }
  }

  @Test
  func firstAndLast() throws {
    for (column, name, ignoreNulls) in [
      (first(col("a")), "first", false),
      (first(col("a"), true), "first", true),
      (last(col("a")), "last", false),
      (last(col("a"), true), "last", true),
      (first_value(col("a"), false), "first_value", false),
      (first_value(col("a"), true), "first_value", true),
      (last_value(col("a"), false), "last_value", false),
      (last_value(col("a"), true), "last_value", true),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].literal.boolean == ignoreNulls)
    }
  }

  @Test
  func distinctAggregateFunctions() throws {
    for (column, name, count) in [
      (countDistinct(col("a")), "count", 1),
      (countDistinct(col("a"), col("b")), "count", 2),
      (count_distinct(col("a")), "count", 1),
      (count_distinct(col("a"), col("b")), "count", 2),
      (sumDistinct(col("a")), "sum", 1),
      (sum_distinct(col("a")), "sum", 1),
      (listagg_distinct(col("a")), "listagg", 1),
      (listagg_distinct(col("a"), ","), "listagg", 2),
      (string_agg_distinct(col("a")), "string_agg", 1),
      (string_agg_distinct(col("a"), ","), "string_agg", 2),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.isDistinct)
      #expect(expr.unresolvedFunction.arguments.count == count)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func groupByAggregateFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES ('a', 1), ('a', 1), ('a', 2), ('b', 3) T(k, v)")
    let rows = try await df.groupBy("k")
      .agg(
        countDistinct(col("v")), sum_distinct(col("v")), median(col("v")), mode(col("v")),
        any_value(col("v"))
      ).orderBy("k").collect()
    #expect(rows == [Row("a", 2, 3, 1.0, 1, 1), Row("b", 1, 3, 3.0, 3, 3)])

    let collected = try await df.groupBy("k")
      .agg(
        sort_array(collect_list(col("v"))).cast("string"),
        sort_array(collect_set(col("v"))).cast("string")
      ).orderBy("k").collect()
    #expect(collected == [Row("a", "[1, 1, 2]", "[1, 2]"), Row("b", "[3]", "[3]")])
    await spark.stop()
  }

  @Test
  func selectStatisticalFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES (double(1.0), double(10.0)), (double(3.0), double(20.0)) T(v, w)")
    let variances = try await df.select(
      variance(col("v")), var_samp(col("v")), var_pop(col("v")),
      stddev(col("v")), stddev_samp(col("v")), stddev_pop(col("v"))
    ).collect()
    #expect(
      variances == [Row(2.0, 2.0, 1.0, 1.4142135623730951, 1.4142135623730951, 1.0)])

    let moments = try await df.select(skewness(col("v")), kurtosis(col("v"))).collect()
    #expect(moments == [Row(0.0, -2.0)])

    let covariances = try await df.select(
      corr(col("v"), col("w")), covar_samp(col("v"), col("w")), covar_pop(col("v"), col("w"))
    ).collect()
    #expect(covariances == [Row(1.0, 10.0, 5.0)])

    let percentiles = try await df.select(
      median(col("v")), percentile_approx(col("v"), lit(0.5), lit(100))
    ).collect()
    #expect(percentiles == [Row(2.0, 1.0)])
    await spark.stop()
  }

  @Test
  func selectFirstLastFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (NULL), (1), (2), (NULL) T(v)")
    let rows = try await df.select(
      first(col("v")), first(col("v"), true), last(col("v")), last(col("v"), true)
    ).collect()
    #expect(rows == [Row(nil, 1, nil, 2)])
    await spark.stop()
  }

  @Test
  func selectBooleanAndCountFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (true, 1), (false, NULL) T(b, v)")
    let rows = try await df.select(
      bool_and(col("b")), bool_or(col("b")), count_if(col("b")),
      countDistinct(col("v")), approx_count_distinct(col("v")),
      approx_count_distinct(col("v"), 0.01)
    ).collect()
    #expect(rows == [Row(false, true, 1, 1, 1, 1)])

    let ordered = try await spark.sql("SELECT * FROM VALUES ('a', 1), ('b', 2) T(k, v)")
      .select(max_by(col("k"), col("v")), min_by(col("k"), col("v"))).collect()
    #expect(ordered == [Row("b", "a")])
    await spark.stop()
  }

  @Test
  func selectAliasAggregateFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES (true, double(1.0)), (false, double(3.0)) T(b, v)")
    let rows = try await df.select(
      every(col("b")), some(col("b")), std(col("v")),
      sort_array(array_agg(col("v"))).cast("string")
    ).collect()
    #expect(rows == [Row(false, true, 1.4142135623730951, "[1.0, 3.0]")])

    let nulls = try await spark.sql("SELECT * FROM VALUES (NULL), (1), (2), (NULL) T(v)")
      .select(
        first_value(col("v")), first_value(col("v"), true),
        last_value(col("v")), last_value(col("v"), true)
      ).collect()
    #expect(nulls == [Row(nil, 1, nil, 2)])
    await spark.stop()
  }

  @Test
  func selectBitAggregateFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (6), (3), (NULL) T(v)")
    let rows = try await df.select(
      bit_and(col("v")), bit_or(col("v")), bit_xor(col("v"))
    ).collect()
    #expect(rows == [Row(2, 7, 5)])
    await spark.stop()
  }

  @Test
  func selectPercentileFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES (double(0.0)), (double(1.0)), (double(2.0)), (double(10.0)) T(v)")
    let rows = try await df.select(
      percentile(col("v"), 0.5), approx_percentile(col("v"), 0.5),
      percentile(col("v"), [0.0, 1.0]).cast("string"),
      approx_percentile(col("v"), [0.0, 1.0], 100).cast("string")
    ).collect()
    #expect(rows == [Row(1.5, 1.0, "[0.0, 10.0]", "[0.0, 10.0]")])

    let weighted = try await spark.sql("SELECT * FROM VALUES (double(1.0)), (double(3.0)) T(v)")
      .select(percentile(col("v"), 0.5, 2)).collect()
    #expect(weighted == [Row(2.0)])
    await spark.stop()
  }

  @Test
  func selectTryAggregateFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1), (2) T(v)")
    #expect(try await df.select(try_avg(col("v")), try_sum(col("v"))).collect() == [Row(1.5, 3)])

    let overflow = try await spark.sql(
      "SELECT * FROM VALUES (9223372036854775807L), (1L) T(v)")
      .select(try_avg(col("v")), try_sum(col("v"))).collect()
    #expect(overflow == [Row(4611686018427387904.0, nil)])
    await spark.stop()
  }

  @Test
  func selectMiscAggregateFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES (double(1.0)), (double(2.0)), (double(3.0)) T(v)")
    #expect(try await df.select(product(col("v"))).collect() == [Row(6.0)])

    let histogram = try await df.select(histogram_numeric(col("v"), 2).cast("string")).collect()
    #expect(histogram == [Row("[{1.0, 1.0}, {2.5, 2.0}]")])

    // `count_min_sketch` requires an integral, string or binary input column.
    let sketch = try await spark.sql("SELECT * FROM VALUES (1), (2), (3) T(v)").select(
      length(hex(count_min_sketch(col("v"), 3.0, 0.1))),
      hex(count_min_sketch(col("v"), 3.0, 0.1, 1))
    ).collect()
    #expect(
      sketch == [
        Row(72, "0000000100000000000000030000000100000001000000005D8D6AB90000000000000003")
      ])
    await spark.stop()
  }

  @Test
  func selectStringAggregateFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES ('a'), ('b'), ('a') T(v)")
    if await isSparkVersionAtLeast(spark.version, "4.0") {
      // `listagg` is non-deterministic in the absence of an ordering, so the results are matched
      // against patterns. The `_distinct` variants collapse the duplicated 'a' to a single one.
      let rows = try await df.select(
        listagg(col("v")).rlike("^[ab]{3}$"),
        listagg(col("v"), ",").rlike("^[ab](,[ab]){2}$"),
        listagg_distinct(col("v")).rlike("^ab|ba$"),
        listagg_distinct(col("v"), ",").rlike("^[ab],[ab]$"),
        string_agg(col("v")).rlike("^[ab]{3}$"),
        string_agg(col("v"), ",").rlike("^[ab](,[ab]){2}$"),
        string_agg_distinct(col("v")).rlike("^ab|ba$"),
        string_agg_distinct(col("v"), ",").rlike("^[ab],[ab]$")
      ).collect()
      #expect(rows == [Row(true, true, true, true, true, true, true, true)])
    }
    await spark.stop()
  }

  @Test
  func selectCollectUnion() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let df = try await spark.sql(
        "SELECT * FROM VALUES (array(1, 2)), (array(2, 3)), (array(1)) T(v)")
      let rows = try await df.select(
        sort_array(collect_union(col("v"))).cast("string")
      ).collect()
      #expect(rows == [Row("[1, 2, 3]")])
    }
    await spark.stop()
  }

  @Test
  func selectGroupingFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES ('a', 1), ('b', 2) T(k, v)")
    let rows = try await df.cube("k")
      .agg(grouping(col("k")), grouping_id(col("k")).alias("gid"))
      .orderBy("gid", "k").collect()
    #expect(rows == [Row("a", 0, 0), Row("b", 0, 0), Row(nil, 1, 1)])
    await spark.stop()
  }
}
