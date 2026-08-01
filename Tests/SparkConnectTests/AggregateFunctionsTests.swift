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
      (bool_and(col("a")), "bool_and"),
      (bool_or(col("a")), "bool_or"),
      (collect_list(col("a")), "collect_list"),
      (collect_set(col("a")), "collect_set"),
      (count_if(col("a")), "count_if"),
      (grouping(col("a")), "grouping"),
      (grouping_id(col("a")), "grouping_id"),
      (kurtosis(col("a")), "kurtosis"),
      (median(col("a")), "median"),
      (mode(col("a")), "mode"),
      (skewness(col("a")), "skewness"),
      (stddev(col("a")), "stddev"),
      (stddev_pop(col("a")), "stddev_pop"),
      (stddev_samp(col("a")), "stddev_samp"),
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
  }

  @Test
  func firstAndLast() throws {
    for (column, name, ignoreNulls) in [
      (first(col("a")), "first", false),
      (first(col("a"), true), "first", true),
      (last(col("a")), "last", false),
      (last(col("a"), true), "last", true),
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
