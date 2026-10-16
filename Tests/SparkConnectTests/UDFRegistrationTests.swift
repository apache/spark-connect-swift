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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Testing

@testable import SparkConnect

/// A test suite for `UDFRegistration`
@Suite(.serialized)
struct UDFRegistrationTests {

  @Test
  func registerJavaWithUnknownClass() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let error = try await #require(throws: Error.self) {
      try await spark.udf.registerJava("java_udf", "org.apache.spark.NoSuchUDF", .string)
    }
    #expect("\(error)".contains("NoSuchUDF"))
    await spark.stop()
  }

  @Test
  func registerJavaWithoutUDFInterface() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let error = try await #require(throws: Error.self) {
      try await spark.udf.registerJava("java_udf", "java.lang.String", .string)
    }
    #expect("\(error)".contains("NO_UDF_INTERFACE"))
    await spark.stop()
  }
}
