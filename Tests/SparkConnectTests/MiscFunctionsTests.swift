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

/// A test suite for `MiscFunctions`
@Suite(.serialized)
struct MiscFunctionsTests {

  @Test
  func miscFunctions() throws {
    for (column, name) in [
      (current_catalog(), "current_catalog"),
      (current_database(), "current_database"),
      (current_path(), "current_path"),
      (current_schema(), "current_schema"),
      (current_user(), "current_user"),
      (input_file_block_length(), "input_file_block_length"),
      (input_file_block_start(), "input_file_block_start"),
      (input_file_name(), "input_file_name"),
      (monotonically_increasing_id(), "monotonically_increasing_id"),
      (session_user(), "session_user"),
      (spark_partition_id(), "spark_partition_id"),
      (user(), "user"),
      (uuid(), "uuid"),
      (version(), "version"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.isEmpty)
    }
  }

  @Test
  func miscFunctionArguments() throws {
    let typeOfColumn = typeof(col("a")).expr
    #expect(typeOfColumn.unresolvedFunction.functionName == "typeof")
    #expect(typeOfColumn.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")

    let seededByColumn = uuid(col("a")).expr
    #expect(seededByColumn.unresolvedFunction.functionName == "uuid")
    #expect(seededByColumn.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")

    let seededByLiteral = uuid(123).expr
    #expect(seededByLiteral.unresolvedFunction.functionName == "uuid")
    #expect(seededByLiteral.unresolvedFunction.arguments[0].literal.long == 123)
  }

  @Test
  func errorReflectionAndBitmapFunctions() throws {
    for (column, name) in [
      (assert_true(col("a")), "assert_true"),
      (assert_true(col("a"), col("b")), "assert_true"),
      (assert_true(col("a"), "error"), "assert_true"),
      (bitmap_bit_position(col("a")), "bitmap_bit_position"),
      (bitmap_bucket_number(col("a")), "bitmap_bucket_number"),
      (bitmap_count(col("a")), "bitmap_count"),
      (java_method(col("a"), col("b")), "java_method"),
      (raise_error(col("a")), "raise_error"),
      (raise_error("error"), "raise_error"),
      (reflect(col("a"), col("b")), "reflect"),
      (try_reflect(col("a"), col("b")), "try_reflect"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(!expr.unresolvedFunction.arguments.isEmpty)
    }

    let assertTrueWithMessage = assert_true(col("a"), "error").expr
    #expect(assertTrueWithMessage.unresolvedFunction.arguments.count == 2)
    #expect(assertTrueWithMessage.unresolvedFunction.arguments[1].literal.string == "error")

    let raiseErrorWithMessage = raise_error("error").expr
    #expect(raiseErrorWithMessage.unresolvedFunction.arguments.count == 1)
    #expect(raiseErrorWithMessage.unresolvedFunction.arguments[0].literal.string == "error")

    let reflectColumns = reflect(col("a"), col("b"), col("c")).expr
    #expect(reflectColumns.unresolvedFunction.arguments.count == 3)
  }

  @Test
  func sessionFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      current_catalog(), current_database(), current_schema(), current_user(), user(), version()
    ).collect()
    #expect(try rows[0].get(0) as! String == "spark_catalog")
    #expect(try rows[0].get(1) as! String == "default")
    #expect(try rows[0].get(2) as! String == "default")
    let currentUser = try rows[0].get(3) as! String
    #expect(!currentUser.isEmpty)
    #expect(try rows[0].get(4) as! String == currentUser)
    // `version()` is a server-side SQL expression, `spark.version` is the already fetched string.
    #expect(try (rows[0].get(5) as! String).hasPrefix(await spark.version))
    await spark.stop()
  }

  @Test
  func sessionUser() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0.0") {
      let rows = try await spark.range(1).select(session_user(), current_user()).collect()
      #expect(try rows[0].get(0) as! String == (try rows[0].get(1) as! String))
    }
    await spark.stop()
  }

  @Test
  func currentPath() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2.0") {
      let rows = try await spark.range(1).select(current_path()).collect()
      #expect(!(try rows[0].get(0) as! String).isEmpty)
    }
    await spark.stop()
  }

  @Test
  func typeOf() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      typeof(lit(1)), typeof(lit(1.0)), typeof(lit("a"))
    ).collect()
    #expect(rows == [Row("bigint", "double", "string")])
    await spark.stop()
  }

  @Test
  func nondeterministicFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(3).select(
      monotonically_increasing_id(), spark_partition_id(), uuid()
    ).collect()
    #expect(rows.count == 3)
    for row in rows {
      #expect((try row.get(0) as? Int64) != nil)
      #expect((try row.get(1) as? Int32) != nil)
      #expect((try row.get(2) as! String).count == 36)
    }
    await spark.stop()
  }

  @Test
  func uuidWithSeed() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.1.0") {
      let rows = try await spark.range(3).select(uuid(lit(123)), uuid(456)).collect()
      #expect(rows.count == 3)
      for row in rows {
        #expect((try row.get(0) as! String).count == 36)
        #expect((try row.get(1) as! String).count == 36)
      }
    }
    await spark.stop()
  }

  @Test
  func assertTrue() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      assert_true(lit(true)), assert_true(lit(true), lit("error")), assert_true(lit(true), "error")
    ).collect()
    #expect(rows == [Row(nil, nil, nil)])

    // The server reports this as `USER_RAISED_EXCEPTION`, which has no `SparkConnectError` case.
    let error = try await #require(throws: Error.self) {
      try await spark.range(1).select(assert_true(lit(false), "assert_true failed")).count()
    }
    #expect("\(error)".contains("assert_true failed"))
    await spark.stop()
  }

  @Test
  func raiseError() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    for column in [raise_error(lit("raise_error failed")), raise_error("raise_error failed")] {
      let error = try await #require(throws: Error.self) {
        try await spark.range(1).select(column).count()
      }
      #expect("\(error)".contains("raise_error failed"))
    }
    await spark.stop()
  }

  @Test
  func reflectFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let uuidString = "a5cf6c42-0c85-418f-af6c-3e4e5b1328f2"
    let rows = try await spark.range(1).select(
      reflect(lit("java.util.UUID"), lit("fromString"), lit(uuidString)),
      java_method(lit("java.util.UUID"), lit("fromString"), lit(uuidString))
    ).collect()
    #expect(rows == [Row(uuidString, uuidString)])
    await spark.stop()
  }

  @Test
  func tryReflect() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0.0") {
      let uuidString = "a5cf6c42-0c85-418f-af6c-3e4e5b1328f2"
      let rows = try await spark.range(1).select(
        try_reflect(lit("java.util.UUID"), lit("fromString"), lit(uuidString)),
        try_reflect(lit("java.util.UUID"), lit("fromString"), lit("invalid"))
      ).collect()
      #expect(rows == [Row(uuidString, nil)])
    }
    await spark.stop()
  }

  @Test
  func bitmapFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      bitmap_bit_position(lit(1)), bitmap_bit_position(lit(123)),
      bitmap_bucket_number(lit(0)), bitmap_bucket_number(lit(123)),
      bitmap_count(unhex(lit("1010"))), bitmap_count(unhex(lit("FFFF")))
    ).collect()
    #expect(rows == [Row(0, 122, 0, 1, 2, 16)])
    await spark.stop()
  }

  @Test
  func fileMetadataFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.read.orc("../examples/src/main/resources/users.orc")
    let rows = try await df.select(
      input_file_name(), input_file_block_start(), input_file_block_length()
    ).collect()
    #expect(rows.count == 2)
    for row in rows {
      let name = try row.get(0) as! String
      #expect(name.isEmpty || name.hasSuffix("users.orc"))
      #expect((try row.get(1) as! Int64) >= -1)
      #expect((try row.get(2) as! Int64) >= -1)
    }
    await spark.stop()
  }
}
