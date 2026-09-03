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

/// A test suite for `UrlFunctions`
@Suite(.serialized)
struct UrlFunctionsTests {

  @Test
  func urlFunctions() throws {
    for (column, name) in [
      (url_encode(col("a")), "url_encode"),
      (url_decode(col("a")), "url_decode"),
      (try_url_decode(col("a")), "try_url_decode"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  /// A `String` `partToExtract` is a shorthand for a literal ``Column``.
  @Test
  func parseUrl() throws {
    for (column, name) in [
      (parse_url(col("a"), "HOST"), "parse_url"),
      (try_parse_url(col("a"), "HOST"), "try_parse_url"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].literal.string == "HOST")
    }

    #expect(parse_url(col("a"), lit("HOST")).expr == parse_url(col("a"), "HOST").expr)
    #expect(try_parse_url(col("a"), lit("HOST")).expr == try_parse_url(col("a"), "HOST").expr)

    let part = parse_url(col("a"), col("p")).expr
    #expect(part.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "p")
  }

  /// A `String` `key` is a shorthand for a literal ``Column``.
  @Test
  func parseUrlQueryKey() throws {
    for (column, name) in [
      (parse_url(col("a"), "QUERY", "k"), "parse_url"),
      (try_parse_url(col("a"), "QUERY", "k"), "try_parse_url"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 3)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].literal.string == "QUERY")
      #expect(expr.unresolvedFunction.arguments[2].literal.string == "k")
    }

    #expect(
      parse_url(col("a"), lit("QUERY"), lit("k")).expr == parse_url(col("a"), "QUERY", "k").expr)
    #expect(
      try_parse_url(col("a"), lit("QUERY"), lit("k")).expr
        == try_parse_url(col("a"), "QUERY", "k").expr)
  }

  @Test
  func selectParseUrl() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let url = lit("https://spark.apache.org/path?query=1#Ref")
    let rows = try await spark.range(1).select(
      parse_url(url, "HOST"),
      parse_url(url, "PATH"),
      parse_url(url, "QUERY"),
      parse_url(url, "REF"),
      parse_url(url, "PROTOCOL"),
      parse_url(url, "QUERY", "query"),
      parse_url(url, "QUERY", "missing")
    ).collect()
    #expect(
      rows == [
        Row("spark.apache.org", "/path", "query=1", "Ref", "https", "1", nil)
      ])
    await spark.stop()
  }

  @Test
  func selectTryParseUrl() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0") {
      let url = lit("https://spark.apache.org/path?query=1")
      let rows = try await spark.range(1).select(
        try_parse_url(url, "HOST"),
        try_parse_url(url, "QUERY", "query"),
        try_parse_url(lit("inva lid"), "HOST"),
        try_parse_url(lit("inva lid"), "QUERY", "query")
      ).collect()
      #expect(rows == [Row("spark.apache.org", "1", nil, nil)])
    }
    await spark.stop()
  }

  @Test
  func selectUrlEncodeAndDecode() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      url_encode(lit("https://spark.apache.org/a b")),
      url_decode(lit("https%3A%2F%2Fspark.apache.org%2Fa+b")),
      url_decode(url_encode(lit("https://spark.apache.org/a b")))
    ).collect()
    #expect(
      rows == [
        Row(
          "https%3A%2F%2Fspark.apache.org%2Fa+b", "https://spark.apache.org/a b",
          "https://spark.apache.org/a b")
      ])
    await spark.stop()
  }

  @Test
  func selectTryUrlDecode() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0") {
      let rows = try await spark.range(1).select(
        try_url_decode(lit("https%3A%2F%2Fspark.apache.org")),
        try_url_decode(lit("https%3A%2F%2spark.apache.org"))
      ).collect()
      #expect(rows == [Row("https://spark.apache.org", nil)])
    }
    await spark.stop()
  }
}
