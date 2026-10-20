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

/// A test suite for `ArrowArray` types which does not require a Spark Connect server
@Suite(.serialized)
struct ArrowArrayTests {

  @Test
  func binaryArrayAsString() throws {
    let builder = try ArrowArrayBuilders.loadBinaryArrayBuilder()
    builder.append(Data("abc".utf8))
    builder.append(Data([0xff, 0x00, 0xfe]))
    builder.append(nil)
    let array = try #require(try builder.finish() as? BinaryArray)

    // UTF-8 bytes are decoded as a string.
    #expect(array.asString(0) == "abc")
    // Non-UTF-8 bytes (e.g. `VARIANT` or `GEOMETRY`) must not crash and fall back to hex.
    #expect(array.asString(1) == "ff00fe")
    #expect(array.asString(2) == "")

    array.options.printAsHex = true
    #expect(array.asString(0) == "616263")
  }

  @Test
  func structWithNonUtf8BinaryAsString() throws {
    // Same shape as `VARIANT`, `struct<value: binary, metadata: binary>`
    let fields = [
      ArrowField("value", type: ArrowType(ArrowType.ArrowBinary), isNullable: true),
      ArrowField("metadata", type: ArrowType(ArrowType.ArrowBinary), isNullable: true),
    ]
    let builder = try StructArrayBuilder(fields)
    builder.append([Data([0x02, 0xff, 0x80]), Data([0x01, 0xc0])])
    let array = try builder.finish()
    #expect(array.asString(0) == "{" + "02ff80" + "," + "01c0" + "}")
  }
}
