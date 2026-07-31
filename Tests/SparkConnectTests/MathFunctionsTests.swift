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

/// A test suite for `MathFunctions`
@Suite(.serialized)
struct MathFunctionsTests {

  @Test
  func mathFunctions() throws {
    for (column, name) in [
      (abs(col("a")), "abs"),
      (acos(col("a")), "acos"),
      (acosh(col("a")), "acosh"),
      (asin(col("a")), "asin"),
      (asinh(col("a")), "asinh"),
      (atan(col("a")), "atan"),
      (atanh(col("a")), "atanh"),
      (bin(col("a")), "bin"),
      (bround(col("a")), "bround"),
      (cbrt(col("a")), "cbrt"),
      (ceil(col("a")), "ceil"),
      (ceiling(col("a")), "ceiling"),
      (cos(col("a")), "cos"),
      (cosh(col("a")), "cosh"),
      (cot(col("a")), "cot"),
      (csc(col("a")), "csc"),
      (degrees(col("a")), "degrees"),
      (exp(col("a")), "exp"),
      (expm1(col("a")), "expm1"),
      (factorial(col("a")), "factorial"),
      (floor(col("a")), "floor"),
      (hex(col("a")), "hex"),
      (ln(col("a")), "ln"),
      (log(col("a")), "ln"),
      (log10(col("a")), "log10"),
      (log1p(col("a")), "log1p"),
      (log2(col("a")), "log2"),
      (negative(col("a")), "negative"),
      (positive(col("a")), "positive"),
      (radians(col("a")), "radians"),
      (rint(col("a")), "rint"),
      (round(col("a")), "round"),
      (sec(col("a")), "sec"),
      (sign(col("a")), "sign"),
      (signum(col("a")), "signum"),
      (sin(col("a")), "sin"),
      (sinh(col("a")), "sinh"),
      (sqrt(col("a")), "sqrt"),
      (tan(col("a")), "tan"),
      (tanh(col("a")), "tanh"),
      (unhex(col("a")), "unhex"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func mathFunctionArguments() throws {
    for (column, name) in [
      (atan2(col("a"), col("b")), "atan2"),
      (hypot(col("a"), col("b")), "hypot"),
      (pmod(col("a"), col("b")), "pmod"),
      (pow(col("a"), col("b")), "power"),
      (power(col("a"), col("b")), "power"),
      (ceil(col("a"), lit(1)), "ceil"),
      (ceiling(col("a"), lit(1)), "ceiling"),
      (floor(col("a"), lit(1)), "floor"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
    }

    for (column, name, scale) in [
      (round(col("a"), 2), "round", Int32(2)),
      (bround(col("a"), 2), "bround", Int32(2)),
      (shiftleft(col("a"), 1), "shiftleft", Int32(1)),
      (shiftright(col("a"), 1), "shiftright", Int32(1)),
      (shiftrightunsigned(col("a"), 1), "shiftrightunsigned", Int32(1)),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[1].literal.integer == scale)
    }

    let logarithm = log(10.0, col("a")).expr
    #expect(logarithm.unresolvedFunction.functionName == "log")
    #expect(logarithm.unresolvedFunction.arguments[0].literal.double == 10.0)

    let converted = conv(col("a"), 2, 16).expr
    #expect(converted.unresolvedFunction.functionName == "conv")
    #expect(converted.unresolvedFunction.arguments[1].literal.integer == 2)
    #expect(converted.unresolvedFunction.arguments[2].literal.integer == 16)

    #expect(e().expr.unresolvedFunction.arguments.isEmpty)
    #expect(pi().expr.unresolvedFunction.arguments.isEmpty)
    #expect(
      width_bucket(col("a"), lit(0), lit(10), lit(5)).expr.unresolvedFunction.arguments.count == 4)
  }

  @Test
  func selectMathFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT double(-4.0) AS a, double(2.345) AS b, double(2.5) AS c")
    let rows = try await df.select(
      abs(col("a")), sqrt(abs(col("a"))), round(col("b")), round(col("b"), 2)
    ).collect()
    #expect(rows == [Row(4.0, 2.0, 2.0, 2.35)])

    let rounded = try await df.select(
      ceil(col("b")), ceiling(col("b")), floor(col("b")), round(col("c")), bround(col("c")),
      rint(col("b"))
    ).collect()
    #expect(rounded == [Row(3, 3, 2, 3.0, 2.0, 2.0)])

    let signs = try await df.select(
      sign(col("a")), signum(col("a")), negative(col("a")), positive(col("a")),
      factorial(lit(5)), pmod(lit(-7), lit(3))
    ).collect()
    #expect(signs == [Row(-1.0, -1.0, 4.0, -4.0, 120, 2)])

    let constants = try await df.select(
      e(), pi(), width_bucket(lit(5.35), lit(0.024), lit(10.06), lit(5))
    ).collect()
    #expect(constants == [Row(2.718281828459045, 3.141592653589793, 3)])
    await spark.stop()
  }

  @Test
  func selectTrigonometricFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let rows = try await df.select(
      sin(lit(0.0)), cos(lit(0.0)), tan(lit(0.0)), sec(lit(0.0)), atan2(lit(0.0), lit(1.0))
    ).collect()
    #expect(rows == [Row(0.0, 1.0, 0.0, 1.0, 0.0)])

    let inverse = try await df.select(
      asin(lit(0.0)), acos(lit(1.0)), atan(lit(0.0)), degrees(lit(0.0)), radians(lit(0.0))
    ).collect()
    #expect(inverse == [Row(0.0, 0.0, 0.0, 0.0, 0.0)])

    let hyperbolic = try await df.select(
      sinh(lit(0.0)), cosh(lit(0.0)), tanh(lit(0.0)), asinh(lit(0.0)), acosh(lit(1.0)),
      atanh(lit(0.0))
    ).collect()
    #expect(hyperbolic == [Row(0.0, 1.0, 0.0, 0.0, 0.0, 0.0)])
    await spark.stop()
  }

  @Test
  func selectLogarithmicFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let rows = try await df.select(
      exp(lit(0.0)), expm1(lit(0.0)), ln(lit(1.0)), log(lit(1.0)), log1p(lit(0.0)),
      log10(lit(100.0))
    ).collect()
    #expect(rows == [Row(1.0, 0.0, 0.0, 0.0, 0.0, 2.0)])

    let powers = try await df.select(
      pow(lit(2.0), lit(10.0)), power(lit(3.0), lit(2.0)), hypot(lit(3.0), lit(4.0)),
      log2(lit(1.0)), log(2.0, lit(1.0))
    ).collect()
    #expect(powers == [Row(1024.0, 9.0, 5.0, 0.0, 0.0)])
    await spark.stop()
  }

  @Test
  func selectBaseConversionFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let rows = try await df.select(
      bin(lit(5)), hex(lit(255)), conv(lit("100"), 2, 10),
      shiftleft(lit(1), 3), shiftright(lit(8), 2), shiftrightunsigned(lit(8), 2)
    ).collect()
    #expect(rows == [Row("101", "FF", "4", 8, 2, 2)])
    await spark.stop()
  }
}
