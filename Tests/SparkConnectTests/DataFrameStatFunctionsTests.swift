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

import SparkConnect
import Testing

/// A test suite for `DataFrameStatFunctions`
@Suite(.serialized)
struct DataFrameStatFunctionsTests {
  @Test
  func crosstab() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1, 1), (1, 2), (2, 1), (2, 1) AS T(c1, c2)")
    let ct = try await df.stat.crosstab("c1", "c2")
    let columns = try await ct.columns
    // The name of the first column is `<col1>_<col2>`.
    #expect(columns[0] == "c1_c2")
    // The remaining column names are the distinct values of `col2`.
    #expect(Set(columns.dropFirst()) == ["1", "2"])
    // One row per distinct value of `col1`.
    #expect(try await ct.count() == 2)
    await spark.stop()
  }

  @Test
  func cov() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1, 2), (2, 4), (3, 6) AS T(c1, c2)")
    #expect(try await df.stat.cov("c1", "c2") == 2.0)
    #expect(try await df.stat.cov("c1", "c1") == 1.0)
    await spark.stop()
  }

  @Test
  func corr() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT * FROM VALUES (1, 2), (2, 4), (3, 6) AS T(c1, c2)")
    // Perfectly positively correlated columns.
    #expect(try await df.stat.corr("c1", "c2") == 1.0)
    // `method` defaults to `pearson`.
    #expect(try await df.stat.corr("c1", "c2", method: "pearson") == 1.0)
    await spark.stop()
  }

  @Test
  func approxQuantile() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT * FROM VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50) AS T(c1, c2)")
    // Single column: exact quantiles (relativeError 0) at min, median, max.
    #expect(try await df.stat.approxQuantile("c1", [0.0, 0.5, 1.0], 0.0) == [1.0, 3.0, 5.0])
    // Multiple columns: one array of quantiles per column.
    let quantiles = try await df.stat.approxQuantile(["c1", "c2"], [0.0, 0.5, 1.0], 0.0)
    #expect(quantiles == [[1.0, 3.0, 5.0], [10.0, 30.0, 50.0]])
    await spark.stop()
  }

  @Test
  func sampleBy() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    // Strata 0, 1, 2 each have 33 rows.
    let df = try await spark.sql("SELECT id % 3 AS key FROM range(0, 99)")
    // A fraction of 1.0 keeps every row of a stratum; an unspecified stratum (or 0.0) keeps none,
    // so the result count is deterministic regardless of the seed.
    #expect(try await df.stat.sampleBy("key", [0: 1.0, 1: 0.0], 0).count() == 33)
    // `Int64` strata are also supported.
    #expect(try await df.stat.sampleBy("key", [Int64(0): 1.0, Int64(2): 1.0], 0).count() == 66)
    // The seed is optional.
    #expect(try await df.stat.sampleBy("key", [0: 1.0, 1: 1.0, 2: 1.0]).count() == 99)
    await spark.stop()
  }
}
