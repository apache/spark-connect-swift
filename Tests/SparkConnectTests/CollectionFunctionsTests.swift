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

/// A test suite for `CollectionFunctions`
@Suite(.serialized)
struct CollectionFunctionsTests {

  @Test
  func collectionFunctions() throws {
    for (column, name) in [
      (array_compact(col("a")), "array_compact"),
      (array_distinct(col("a")), "array_distinct"),
      (array_max(col("a")), "array_max"),
      (array_min(col("a")), "array_min"),
      (array_size(col("a")), "array_size"),
      (array_sort(col("a")), "array_sort"),
      (cardinality(col("a")), "cardinality"),
      (explode(col("a")), "explode"),
      (explode_outer(col("a")), "explode_outer"),
      (flatten(col("a")), "flatten"),
      (inline(col("a")), "inline"),
      (inline_outer(col("a")), "inline_outer"),
      (map_entries(col("a")), "map_entries"),
      (map_from_entries(col("a")), "map_from_entries"),
      (map_keys(col("a")), "map_keys"),
      (map_values(col("a")), "map_values"),
      (posexplode(col("a")), "posexplode"),
      (posexplode_outer(col("a")), "posexplode_outer"),
      (reverse(col("a")), "reverse"),
      (shuffle(col("a")), "shuffle"),
      (size(col("a")), "size"),
      (str_to_map(col("a")), "str_to_map"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func collectionFunctionArguments() throws {
    for (column, name) in [
      (array_append(col("a"), col("b")), "array_append"),
      (array_contains(col("a"), col("b")), "array_contains"),
      (array_except(col("a"), col("b")), "array_except"),
      (array_intersect(col("a"), col("b")), "array_intersect"),
      (array_position(col("a"), col("b")), "array_position"),
      (array_prepend(col("a"), col("b")), "array_prepend"),
      (array_remove(col("a"), col("b")), "array_remove"),
      (array_repeat(col("a"), col("b")), "array_repeat"),
      (array_union(col("a"), col("b")), "array_union"),
      (arrays_overlap(col("a"), col("b")), "arrays_overlap"),
      (element_at(col("a"), col("b")), "element_at"),
      (get(col("a"), col("b")), "get"),
      (map_contains_key(col("a"), col("b")), "map_contains_key"),
      (map_from_arrays(col("a"), col("b")), "map_from_arrays"),
      (sequence(col("a"), col("b")), "sequence"),
      (shuffle(col("a"), col("b")), "shuffle"),
      (str_to_map(col("a"), col("b")), "str_to_map"),
      (try_element_at(col("a"), col("b")), "try_element_at"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
    }

    for (column, name) in [
      (array_insert(col("a"), col("b"), col("c")), "array_insert"),
      (sequence(col("a"), col("b"), col("c")), "sequence"),
      (slice(col("a"), col("b"), col("c")), "slice"),
      (str_to_map(col("a"), col("b"), col("c")), "str_to_map"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 3)
    }
  }

  @Test
  func collectionFunctionLiteralArguments() throws {
    for (column, name) in [
      (array_append(col("a"), "x"), "array_append"),
      (array_contains(col("a"), "x"), "array_contains"),
      (array_position(col("a"), "x"), "array_position"),
      (array_prepend(col("a"), "x"), "array_prepend"),
      (array_remove(col("a"), "x"), "array_remove"),
      (element_at(col("a"), "x"), "element_at"),
      (map_contains_key(col("a"), "x"), "map_contains_key"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[1].literal.string == "x")
    }

    #expect(array_join(col("a"), ",").expr.unresolvedFunction.arguments[1].literal.string == ",")
    #expect(array_join(col("a"), ",", "*").expr.unresolvedFunction.arguments.count == 3)
    #expect(array_repeat(col("a"), 2).expr.unresolvedFunction.arguments[1].literal.integer == 2)
    #expect(sort_array(col("a")).expr.unresolvedFunction.arguments[1].literal.boolean == true)
    #expect(sort_array(col("a"), false).expr.unresolvedFunction.arguments[1].literal.boolean == false)

    let sliced = slice(col("a"), 2, 3).expr
    #expect(sliced.unresolvedFunction.arguments[1].literal.integer == 2)
    #expect(sliced.unresolvedFunction.arguments[2].literal.integer == 3)
  }

  @Test
  func collectionFunctionVariadicArguments() throws {
    for (column, name, count) in [
      (array(col("a"), col("b")), "array", 2),
      (arrays_zip(col("a"), col("b")), "arrays_zip", 2),
      (concat(col("a"), col("b"), col("c")), "concat", 3),
      (map(col("a"), col("b"), col("c"), col("d")), "map", 4),
      (map_concat(col("a"), col("b")), "map_concat", 2),
      (named_struct(col("a"), col("b"), col("c"), col("d")), "named_struct", 4),
      (stack(col("a"), col("b"), col("c")), "stack", 3),
      (`struct`(col("a"), col("b")), "struct", 2),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
    }
  }

  @Test
  func selectArrayFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let arr = array(lit(1), lit(2), lit(3))

    let modified = try await df.select(
      arr.cast("string"), array_append(arr, 4).cast("string"),
      array_prepend(arr, 0).cast("string"), array_remove(arr, 2).cast("string"),
      array_insert(arr, lit(Int32(1)), lit(0)).cast("string")
    ).collect()
    #expect(modified == [Row("[1, 2, 3]", "[1, 2, 3, 4]", "[0, 1, 2, 3]", "[1, 3]", "[0, 1, 2, 3]")])

    let setOps = try await df.select(
      array_distinct(array(lit(1), lit(1), lit(2))).cast("string"),
      array_except(arr, array(lit(2))).cast("string"),
      array_intersect(arr, array(lit(2), lit(4))).cast("string"),
      array_union(array(lit(1)), array(lit(2))).cast("string")
    ).collect()
    #expect(setOps == [Row("[1, 2]", "[1, 3]", "[2]", "[1, 2]")])

    let lookups = try await df.select(
      array_contains(arr, 2), array_position(arr, 2), element_at(arr, Int32(1)),
      try_element_at(arr, lit(Int32(1))), get(arr, lit(Int32(0)))
    ).collect()
    #expect(lookups == [Row(true, 2, 1, 1, 1)])

    let ordered = try await df.select(
      sort_array(array(lit(3), lit(1), lit(2))).cast("string"),
      sort_array(arr, false).cast("string"), reverse(arr).cast("string"),
      slice(arr, 2, 2).cast("string"), flatten(array(array(lit(1)), array(lit(2)))).cast("string")
    ).collect()
    #expect(ordered == [Row("[1, 2, 3]", "[3, 2, 1]", "[3, 2, 1]", "[2, 3]", "[1, 2]")])

    let sizes = try await df.select(
      size(arr), cardinality(arr), array_size(arr), array_min(arr), array_max(arr),
      size(shuffle(arr)), size(shuffle(arr, lit(42)))
    ).collect()
    #expect(sizes == [Row(3, 3, 3, 1, 3, 3, 3)])

    let combined = try await df.select(
      array_repeat(lit(1), 2).cast("string"), sequence(lit(1), lit(3)).cast("string"),
      sequence(lit(5), lit(1), lit(-2)).cast("string"),
      arrays_zip(array(lit(1)), array(lit("a"))).cast("string"),
      arrays_overlap(arr, array(lit(3), lit(9)))
    ).collect()
    #expect(combined == [Row("[1, 1]", "[1, 2, 3]", "[5, 3, 1]", "[{1, a}]", true)])

    let dfWithNull = try await spark.sql("SELECT array(1, null, 3) AS a")
    let compacted = try await dfWithNull.select(
      array_compact(col("a")).cast("string"), array_join(col("a"), ","),
      array_join(col("a"), ",", "*")
    ).collect()
    #expect(compacted == [Row("[1, 3]", "1,3", "1,*,3")])
    await spark.stop()
  }

  @Test
  func selectMapFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let m = map(lit("a"), lit(1), lit("b"), lit(2))

    let rows = try await df.select(
      m.cast("string"), map_keys(m).cast("string"), map_values(m).cast("string"),
      map_entries(m).cast("string")
    ).collect()
    #expect(rows == [Row("{a -> 1, b -> 2}", "[a, b]", "[1, 2]", "[{a, 1}, {b, 2}]")])

    let lookups = try await df.select(
      map_contains_key(m, "a"), element_at(m, "a"), try_element_at(m, lit("b"))
    ).collect()
    #expect(lookups == [Row(true, 1, 2)])

    let created = try await df.select(
      map_concat(map(lit("a"), lit(1)), map(lit("b"), lit(2))).cast("string"),
      map_from_arrays(array(lit("a")), array(lit(1))).cast("string"),
      map_from_entries(array(`struct`(lit("a"), lit(1)))).cast("string")
    ).collect()
    #expect(created == [Row("{a -> 1, b -> 2}", "{a -> 1}", "{a -> 1}")])

    let parsed = try await df.select(
      str_to_map(lit("a:1,b:2")).cast("string"),
      str_to_map(lit("a:1;b:2"), lit(";")).cast("string"),
      str_to_map(lit("a=1;b=2"), lit(";"), lit("=")).cast("string")
    ).collect()
    #expect(
      parsed == [Row("{a -> 1, b -> 2}", "{a -> 1, b -> 2}", "{a -> 1, b -> 2}")])
    await spark.stop()
  }

  @Test
  func selectStructAndConcatFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let rows = try await df.select(
      `struct`(lit(1), lit("a")).cast("string"),
      named_struct(lit("x"), lit(1), lit("y"), lit("a")).cast("string"),
      concat(lit("a"), lit("b"), lit("c")),
      concat(array(lit(1)), array(lit(2))).cast("string")
    ).collect()
    #expect(rows == [Row("{1, a}", "{1, a}", "abc", "[1, 2]")])
    await spark.stop()
  }

  @Test
  func selectGeneratorFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    #expect(try await df.select(explode(array(lit(1), lit(2)))).collect() == [Row(1), Row(2)])
    #expect(
      try await df.select(explode_outer(array(lit(1), lit(2)))).collect() == [Row(1), Row(2)])
    #expect(try await df.select(explode(map(lit("a"), lit(1)))).collect() == [Row("a", 1)])
    #expect(
      try await df.select(posexplode(array(lit(10), lit(20)))).collect()
        == [Row(0, 10), Row(1, 20)])
    #expect(
      try await df.select(posexplode_outer(array(lit(10), lit(20)))).collect()
        == [Row(0, 10), Row(1, 20)])

    let structArray = array(named_struct(lit("a"), lit(1), lit("b"), lit(2)))
    #expect(try await df.select(inline(structArray)).collect() == [Row(1, 2)])
    #expect(try await df.select(inline_outer(structArray)).collect() == [Row(1, 2)])
    #expect(
      try await df.select(stack(lit(Int32(2)), lit(1), lit(2), lit(3), lit(4))).collect()
        == [Row(1, 2), Row(3, 4)])
    await spark.stop()
  }
}
