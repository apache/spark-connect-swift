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

/// A test suite for `BitwiseFunctions`
@Suite(.serialized)
struct BitwiseFunctionsTests {

  @Test
  func bitwiseFunctions() throws {
    for (column, name) in [
      (bit_count(col("a")), "bit_count"),
      (bitwise_not(col("a")), "~"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func bitwiseFunctionArguments() throws {
    for (column, name) in [
      (bit_get(col("a"), col("b")), "bit_get"),
      (getbit(col("a"), col("b")), "getbit"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "b")
    }

    for (column, name, numBits) in [
      (shiftleft(col("a"), 1), "shiftleft", Int32(1)),
      (shiftright(col("a"), 1), "shiftright", Int32(1)),
      (shiftrightunsigned(col("a"), 1), "shiftrightunsigned", Int32(1)),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[1].literal.integer == numBits)
    }
  }

  @Test
  func bitwiseColumnMethods() throws {
    for (column, name) in [
      (col("a").bitwiseAND(col("b")), "&"),
      (col("a").bitwiseOR(col("b")), "|"),
      (col("a").bitwiseXOR(col("b")), "^"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "b")
    }

    for (column, name) in [
      (col("a").bitwiseAND(1), "&"),
      (col("a").bitwiseOR(1), "|"),
      (col("a").bitwiseXOR(1), "^"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[1].literal.long == 1)
    }
  }

  @Test
  func selectBitwiseFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let rows = try await df.select(
      bit_count(lit(7)), bit_count(lit(0)),
      bit_get(lit(5), lit(0)), bit_get(lit(5), lit(1)),
      getbit(lit(5), lit(2)), bitwise_not(lit(0))
    ).collect()
    #expect(rows == [Row(3, 0, 1, 0, 1, -1)])
    await spark.stop()
  }

  @Test
  func selectShiftFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let rows = try await df.select(
      shiftleft(lit(1), 3), shiftright(lit(8), 2), shiftrightunsigned(lit(8), 2)
    ).collect()
    #expect(rows == [Row(8, 2, 2)])
    await spark.stop()
  }

  @Test
  func selectBitwiseColumnMethods() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let rows = try await df.select(
      lit(170).bitwiseAND(lit(75)), lit(170).bitwiseOR(lit(75)), lit(170).bitwiseXOR(lit(75))
    ).collect()
    #expect(rows == [Row(10, 235, 225)])

    let literals = try await df.select(
      lit(170).bitwiseAND(75), lit(170).bitwiseOR(75), lit(170).bitwiseXOR(75)
    ).collect()
    #expect(literals == [Row(10, 235, 225)])
    await spark.stop()
  }
}
