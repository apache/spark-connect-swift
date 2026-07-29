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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import SparkConnect
import Testing

/// A test suite for `DataFrameNaFunctions`
@Suite(.serialized)
struct DataFrameNaFunctionsTests {
  @Test
  func naFill() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES (1, 10, 'a'), (NULL, NULL, 'b'), (3, 30, NULL) AS T(a, b, s)")
    // Fill all type-compatible (numeric) columns.
    #expect(
      try await df.na.fill(0).collect()
        == [Row(1, 10, "a"), Row(0, 0, "b"), Row(3, 30, nil)])
    // Fill a subset of columns.
    #expect(
      try await df.na.fill(0, ["a"]).collect()
        == [Row(1, 10, "a"), Row(0, nil, "b"), Row(3, 30, nil)])
    // Fill string columns.
    #expect(
      try await df.na.fill("z").collect()
        == [Row(1, 10, "a"), Row(nil, nil, "b"), Row(3, 30, "z")])
    // Fill per-column values.
    #expect(
      try await df.na.fill(["a": 0, "s": "z"]).collect()
        == [Row(1, 10, "a"), Row(0, nil, "b"), Row(3, 30, "z")])
    await spark.stop()
  }

  @Test
  func naDrop() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES (1, 10, 'a'), (NULL, NULL, 'b'), (3, 30, NULL) AS T(a, b, s)")
    // Drop rows containing any null value (default).
    #expect(try await df.na.drop().collect() == [Row(1, 10, "a")])
    // Drop rows only when every value is null.
    #expect(
      try await df.na.drop(how: "all").collect()
        == [Row(1, 10, "a"), Row(nil, nil, "b"), Row(3, 30, nil)])
    // Keep rows with at least 2 non-null values.
    #expect(
      try await df.na.drop(minNonNulls: 2).collect()
        == [Row(1, 10, "a"), Row(3, 30, nil)])
    // Consider only a subset of columns.
    #expect(
      try await df.na.drop(how: "any", ["s"]).collect()
        == [Row(1, 10, "a"), Row(nil, nil, "b")])
    await spark.stop()
  }

  @Test
  func naReplace() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'a') AS T(n, s)")
    // Replace string values in a single column.
    #expect(
      try await df.na.replace("s", ["a": "z"]).collect()
        == [Row(1, "z"), Row(2, "b"), Row(3, "z")])
    // Replace several string values across the given columns.
    #expect(
      try await df.na.replace(["s"], ["a": "z", "b": "y"]).collect()
        == [Row(1, "z"), Row(2, "y"), Row(3, "z")])
    // `*` considers all type-compatible columns.
    #expect(
      try await df.na.replace("*", ["a": "z"]).collect()
        == [Row(1, "z"), Row(2, "b"), Row(3, "z")])
    // Replace numeric (double) values.
    let df2 = try await spark.sql("SELECT * FROM VALUES (1.0D), (2.0D), (1.0D) AS T(d)")
    #expect(
      try await df2.na.replace("d", [1.0: 9.0]).collect()
        == [Row(9.0), Row(2.0), Row(9.0)])
    await spark.stop()
  }
}
