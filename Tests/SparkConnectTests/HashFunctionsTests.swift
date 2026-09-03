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

/// A test suite for `HashFunctions`
@Suite(.serialized)
struct HashFunctionsTests {

  @Test
  func hashFunctions() throws {
    for (column, name) in [
      (md5(col("a")), "md5"),
      (sha1(col("a")), "sha1"),
      (sha(col("a")), "sha"),
      (crc32(col("a")), "crc32"),
      (hash(col("a")), "hash"),
      (xxhash64(col("a")), "xxhash64"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func hashFunctionArguments() throws {
    let digest = sha2(col("a"), 256).expr
    #expect(digest.unresolvedFunction.functionName == "sha2")
    #expect(digest.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    #expect(digest.unresolvedFunction.arguments[1].literal.integer == 256)

    for (column, name) in [
      (hash(col("a"), col("b"), col("c")), "hash"),
      (xxhash64(col("a"), col("b"), col("c")), "xxhash64"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 3)
    }
  }

  @Test
  func selectDigestFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 'Spark' AS a")
    let rows = try await df.select(md5(col("a")), sha1(col("a")), sha(col("a")), crc32(col("a")))
      .collect()
    #expect(
      rows == [
        Row(
          "8cde774d6f7333752ed72cacddb05126",
          "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c",
          "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c",
          1_557_323_817
        )
      ])

    let sha2Rows = try await df.select(
      sha2(col("a"), 224), sha2(col("a"), 256), sha2(col("a"), 0)
    ).collect()
    #expect(
      sha2Rows == [
        Row(
          "dbeab94971678d36af2195851c0f7485775a2a7c60073d62fc04549c",
          "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b",
          "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b"
        )
      ])
    await spark.stop()
  }

  @Test
  func selectHashCodeFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 'Spark' AS a, 1 AS b")
    let rows = try await df.select(hash(col("a")), xxhash64(col("a"))).collect()
    #expect(rows == [Row(228_093_765, -4_294_468_057_691_064_905)])

    let multiple = try await df.select(hash(col("b"), col("a")), xxhash64(col("b"), col("a")))
      .collect()
    #expect(multiple == [Row(1_622_978_250, 8_223_983_067_343_925_414)])
    await spark.stop()
  }
}
