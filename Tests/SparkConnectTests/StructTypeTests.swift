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

/// A test suite for `StructType` and `StructField`
@Suite(.serialized)
struct StructTypeTests {
  @Test
  func simpleString() {
    let structType = StructType(fields: [
      StructField(name: "id", dataType: .long),
      StructField(name: "arr", dataType: .array(elementType: .string, containsNull: false)),
    ])
    #expect(structType.simpleString == "struct<id:bigint,arr:array<string>>")
    #expect(StructType().simpleString == "struct<>")
  }

  @Test
  func accessors() {
    let structType = StructType(fields: [
      StructField(name: "id", dataType: .long, nullable: false),
      StructField(name: "name", dataType: .string),
      StructField(name: "salary", dataType: .decimal(precision: 10, scale: 2)),
    ])
    #expect(structType.fieldNames == ["id", "name", "salary"])
    #expect(structType.count == 3)
    #expect(structType[0] == StructField(name: "id", dataType: .long, nullable: false))
    #expect(structType["name"] == StructField(name: "name", dataType: .string))
    #expect(structType["name"]?.nullable == true)
    #expect(structType["nonexistent"] == nil)
    #expect(structType.map { $0.dataType.simpleString } == ["bigint", "string", "decimal(10,2)"])

    var count = 0
    for field in structType {
      #expect(structType[count] == field)
      count += 1
    }
    #expect(count == 3)
  }

  @Test
  func toDDL() {
    let nested = StructType(fields: [
      StructField(name: "x", dataType: .integer, nullable: false),
      StructField(name: "y z", dataType: .string),
    ])
    let structType = StructType(fields: [
      StructField(name: "id", dataType: .long, nullable: false),
      StructField(name: "1st", dataType: .decimal(precision: 10, scale: 2)),
      StructField(name: "t", dataType: .time(precision: 6)),
      StructField(name: "s", dataType: .struct(nested)),
      StructField(name: "arr", dataType: .array(elementType: .struct(nested), containsNull: true)),
      StructField(
        name: "m", dataType: .map(keyType: .string, valueType: .date, valueContainsNull: true)),
    ])
    #expect(
      structType.toDDL
        == "id BIGINT NOT NULL,`1st` DECIMAL(10,2),t TIME(6),"
        + "s STRUCT<x: INT NOT NULL, `y z`: STRING>,"
        + "arr ARRAY<STRUCT<x: INT NOT NULL, `y z`: STRING>>,m MAP<STRING, DATE>")
    #expect(StructType().toDDL == "")
    #expect(StructField(name: "a`b", dataType: .string).toDDL == "`a``b` STRING")
    #expect(StructField(name: "", dataType: .string).toDDL == "`` STRING")
  }

  @Test
  func equatable() {
    let a = StructType(fields: [StructField(name: "id", dataType: .long)])
    let b = StructType(fields: [StructField(name: "id", dataType: .long)])
    let c = StructType(fields: [StructField(name: "id", dataType: .integer)])
    #expect(a == b)
    #expect(a != c)
    #expect(a != StructType())
    #expect(
      StructField(name: "id", dataType: .long)
        != StructField(name: "id", dataType: .long, nullable: false))
  }
}
