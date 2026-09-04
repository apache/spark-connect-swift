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
import Testing

@testable import SparkConnect

/// A test suite for `UDFFunctions`
@Suite(.serialized)
struct UDFFunctionsTests {

  @Test
  func callFunctions() throws {
    let expr = call_function("abs", col("a")).expr
    #expect(expr.callFunction.functionName == "abs")
    #expect(expr.callFunction.arguments.count == 1)
    #expect(expr.callFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")

    let udf = call_udf("default.simpleUDF", col("a"), col("b")).expr
    #expect(udf.callFunction.functionName == "default.simpleUDF")
    #expect(udf.callFunction.arguments.count == 2)
  }

  @Test
  func selectCallFunction() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    #expect(try await df.select(call_function("abs", lit(-1))).collect() == [Row(1)])
    #expect(
      try await df.select(call_function("abs", lit(-1))).collect()
        == df.select(abs(lit(-1))).collect())
    await spark.stop()
  }

  @Test
  func selectCallUDF() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let funcName = "FUNC_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
    try await ErrorUtils.tryWithSafeFinally(
      {
        try await spark.sql(
          "CREATE TEMPORARY FUNCTION \(funcName)(v INT) RETURNS INT RETURN v * v"
        ).count()
        #expect(try await spark.range(1).select(call_udf(funcName, lit(4))).collect() == [Row(16)])
      },
      {
        try await spark.sql("DROP TEMPORARY FUNCTION IF EXISTS \(funcName)").count()
      })
    await spark.stop()
  }
}
