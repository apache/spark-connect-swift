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
import SparkConnect
import Testing

/// A test suite for `Row`
@Suite(.serialized)
struct RowTests {
  @Test
  func empty() {
    #expect(Row.empty.size == 0)
    #expect(Row.empty.length == 0)
    #expect(throws: SparkConnectError.InvalidArgument) {
      try Row.empty.get(0)
    }
  }

  @Test
  func create() {
    #expect(Row(nil).size == 1)
    #expect(Row(1).size == 1)
    #expect(Row(1.1).size == 1)
    #expect(Row(Decimal(1.1)).size == 1)
    #expect(Row("a").size == 1)
    #expect(Row(nil, 1, 1.1, "a", true).size == 5)
    #expect(Row(valueArray: [nil, 1, 1.1, "a", true]).size == 5)
  }

  @Test
  func string() async throws {
    #expect(Row(nil, 1, 1.1, "a", true).toString() == "[null,1,1.1,a,true]")
  }

  @Test
  func get() throws {
    let row = Row(1, 1.1, "a", true, Decimal(1.2))
    #expect(try row.get(0) as! Int == 1)
    #expect(try row.get(1) as! Double == 1.1)
    #expect(try row.get(2) as! String == "a")
    #expect(try row.get(3) as! Bool == true)
    #expect(try row.get(4) as! Decimal == Decimal(1.2))
    #expect(throws: SparkConnectError.InvalidArgument) {
      try Row.empty.get(-1)
    }
  }

  @Test
  func getAsBool() throws {
    #expect(try Row(true).getAsBool(0) == true)
    #expect(try Row(false).getAsBool(0) == false)
    #expect(try Row(valueArray: [true], schema: RowSchema(["flag"])).getAsBool(0) == true)

    // A type mismatch throws instead of trapping.
    #expect(throws: SparkConnectError.InvalidType) {
      try Row(1).getAsBool(0)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try Row("true").getAsBool(0)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try Row(nil).getAsBool(0)
    }

    #expect(throws: SparkConnectError.InvalidArgument) {
      try Row(true).getAsBool(1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try Row(true).getAsBool(-1)
    }
  }

  @Test
  func fieldIndex() throws {
    let row = Row(valueArray: [1, "a"], schema: RowSchema(["id", "name"]))
    #expect(try row.fieldIndex("id") == 0)
    #expect(try row.fieldIndex("name") == 1)
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.fieldIndex("nonexistent")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(1).fieldIndex("id")
    }
  }

  @Test
  func getByName() throws {
    let row = Row(valueArray: [1, 1.1, "a", true], schema: RowSchema(["id", "value", "name", "flag"]))
    #expect(try row.get("id") as! Int == 1)
    #expect(try row.get("value") as! Double == 1.1)
    #expect(try row.get("name") as! String == "a")
    #expect(try row.get("flag") as! Bool == true)
    #expect(try row["id"] as! Int == 1)
    #expect(try row["name"] as! String == "a")
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.get("nonexistent")
    }
  }

  @Test
  func duplicateFieldNames() throws {
    // Like Scala's `StructType.fieldIndex`, the last field wins for duplicate names.
    let row = Row(valueArray: [1, 2], schema: RowSchema(["id", "id"]))
    #expect(try row.fieldIndex("id") == 1)
    #expect(try row.get("id") as! Int == 2)
  }

  @Test
  func asDict() throws {
    let row = Row(valueArray: [1, "a", nil], schema: RowSchema(["id", "name", "none"]))
    let dict = try row.asDict()
    #expect(dict.count == 3)
    #expect(dict["id"] as! Int == 1)
    #expect(dict["name"] as! String == "a")
    #expect(dict.keys.contains("none"))
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(1).asDict()
    }
  }

  @Test
  func compareIgnoresSchema() {
    #expect(Row(valueArray: [1, "a"], schema: RowSchema(["id", "name"])) == Row(1, "a"))
    #expect(
      Row(valueArray: [1], schema: RowSchema(["a"]))
        == Row(valueArray: [1], schema: RowSchema(["b"])))
  }

  @Test
  func compare() {
    #expect(Row(nil) != Row())
    #expect(Row(nil) == Row(nil))

    #expect(Row(1) == Row(1))
    #expect(Row(1) != Row(2))
    #expect(Row(1, 2, 3) == Row(1, 2, 3))
    #expect(Row(1, 2, 3) != Row(1, 2, 4))

    #expect(Row(1.0) == Row(1.0))
    #expect(Row(1.0) != Row(2.0))

    #expect(Row(Decimal(1.0)) == Row(Decimal(1.0)))
    #expect(Row(Decimal(1.0)) != Row(Decimal(2.0)))

    #expect(Row("a") == Row("a"))
    #expect(Row("a") != Row("b"))

    #expect(Row(true) == Row(true))
    #expect(Row(true) != Row(false))

    #expect(Row(1, "a") == Row(1, "a"))
    #expect(Row(1, "a") != Row(2, "a"))
    #expect(Row(1, "a") != Row(1, "b"))

    #expect(Row(0, 1, 2) == Row(valueArray: [0, 1, 2]))

    #expect(Row(0) == Row(Optional(0)))
    #expect(Row(Optional(0)) == Row(Optional(0)))

    #expect([Row(1)] == [Row(1)])
    #expect([Row(1), Row(2)] == [Row(1), Row(2)])
    #expect([Row(1), Row(2)] != [Row(1), Row(3)])
  }

  @Test
  func compareArray() {
    #expect([Row([1, 2])] == [Row([1, 2])])
    #expect([Row([1, 2])] != [Row([1, 3])])
    #expect([Row([1, 2])] != [Row([1])])

    #expect([Row([1.0, 2.0])] == [Row([1.0, 2.0])])
    #expect([Row([1.0, 2.0])] != [Row([1.0, 3.0])])
    #expect([Row([1.0, 2.0])] != [Row([1.0])])

    #expect([Row([Decimal(1.0), Decimal(2.0)])] == [Row([Decimal(1.0), Decimal(2.0)])])
    #expect([Row([Decimal(1.0), Decimal(2.0)])] != [Row([Decimal(1.0), Decimal(3.0)])])
    #expect([Row([Decimal(1.0), Decimal(2.0)])] != [Row([Decimal(1.0)])])

    #expect([Row(["a", "b"])] == [Row(["a", "b"])])
    #expect([Row(["a", "b"])] != [Row(["a", "c"])])
    #expect([Row(["a", "b"])] != [Row(["a"])])

    #expect([Row([true, false])] == [Row([true, false])])
    #expect([Row([true, false])] != [Row([true, true])])
    #expect([Row([true, false])] != [Row([true])])
  }
}
