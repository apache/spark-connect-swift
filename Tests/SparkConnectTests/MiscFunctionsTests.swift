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
