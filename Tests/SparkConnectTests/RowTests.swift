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

    let namedRow = Row(valueArray: [true, false, nil, 1], schema: RowSchema(["t", "f", "n", "i"]))
    #expect(try namedRow.getAsBool("t") == true)
    #expect(try namedRow.getAsBool("f") == false)
    #expect(throws: SparkConnectError.InvalidType) {
      try namedRow.getAsBool("n")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try namedRow.getAsBool("i")
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try namedRow.getAsBool("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(true).getAsBool("t")
    }
  }

  @Test
  func isNullAt() throws {
    let row = Row(valueArray: [nil, 1, "a"], schema: RowSchema(["n", "id", "name"]))
    #expect(try row.isNullAt(0) == true)
    #expect(try row.isNullAt(1) == false)
    #expect(try row.isNullAt(2) == false)
    #expect(try row.isNullAt("n") == true)
    #expect(try row.isNullAt("id") == false)
    #expect(try row.isNullAt("name") == false)

    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.isNullAt(-1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.isNullAt(3)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.isNullAt("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(nil).isNullAt("n")
    }
  }

  @Test
  func getAsGeneric() throws {
    let row = Row(
      valueArray: [true, Int32(42), "spark", nil, Float(3.5)],
      schema: RowSchema(["flag", "id", "name", "none", "score"]))

    // Contextual type inference
    let b: Bool = try row.getAs(0)
    #expect(b == true)
    let bNamed: Bool = try row.getAs("flag")
    #expect(bNamed == true)

    let i: Int = try row.getAs(1)
    #expect(i == 42)
    let iNamed: Int = try row.getAs("id")
    #expect(iNamed == 42)

    let s: String = try row.getAs(2)
    #expect(s == "spark")
    let sNamed: String = try row.getAs("name")
    #expect(sNamed == "spark")

    let d: Double = try row.getAs(4)
    #expect(d == Double(Float(3.5)))

    // Explicit type passing
    #expect(try row.getAs(0, Bool.self) == true)
    #expect(try row.getAs("flag", Bool.self) == true)
    #expect(try row.getAs(1, Int.self) == 42)
    #expect(try row.getAs("id", Int.self) == 42)
    #expect(try row.getAs(1, Int64.self) == 42)
    #expect(try row.getAs("id", Int64.self) == 42)
    #expect(try row.getAs(2, String.self) == "spark")
    #expect(try row.getAs("name", String.self) == "spark")
    #expect(try row.getAs(4, Double.self) == Double(Float(3.5)))
    #expect(try row.getAs("score", Double.self) == Double(Float(3.5)))

    // Optional type handling
    let opt: String? = try row.getAs(3)
    #expect(opt == nil)
    #expect(try row.getAs(3, String?.self) == nil)
    #expect(try row.getAs("none", String?.self) == nil)

    // Errors
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAs(3, String.self)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAs("none", String.self)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAs(2, Int.self)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAs(-1, Int.self)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAs(5, Int.self)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.getAs("missing", Int.self)
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(1).getAs("id", Int.self)
    }
  }

  @Test
  func getAsInt() throws {
    let row = Row(
      valueArray: [Int8(1), Int16(2), Int32(3), Int64(4), 5, "6", nil, 1.5],
      schema: RowSchema(["i8", "i16", "i32", "i64", "i", "s", "n", "d"]))

    #expect(try row.getAsInt(0) == 1)
    #expect(try row.getAsInt(1) == 2)
    #expect(try row.getAsInt(2) == 3)
    #expect(try row.getAsInt(3) == 4)
    #expect(try row.getAsInt(4) == 5)

    #expect(try row.getAsInt("i8") == 1)
    #expect(try row.getAsInt("i16") == 2)
    #expect(try row.getAsInt("i32") == 3)
    #expect(try row.getAsInt("i64") == 4)
    #expect(try row.getAsInt("i") == 5)

    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt(5)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt("s")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt(6)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt("n")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt(7)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt("d")
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsInt(-1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsInt(8)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.getAsInt("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(1).getAsInt("i")
    }
  }

  @Test
  func getAsInt64() throws {
    let row = Row(
      valueArray: [Int8(10), Int16(20), Int32(30), Int64(40), 50, "60", nil],
      schema: RowSchema(["i8", "i16", "i32", "i64", "i", "s", "n"]))

    #expect(try row.getAsInt64(0) == 10)
    #expect(try row.getAsInt64(1) == 20)
    #expect(try row.getAsInt64(2) == 30)
    #expect(try row.getAsInt64(3) == 40)
    #expect(try row.getAsInt64(4) == 50)

    #expect(try row.getAsInt64("i8") == 10)
    #expect(try row.getAsInt64("i16") == 20)
    #expect(try row.getAsInt64("i32") == 30)
    #expect(try row.getAsInt64("i64") == 40)
    #expect(try row.getAsInt64("i") == 50)

    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt64(5)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt64("s")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt64(6)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsInt64("n")
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsInt64(-1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsInt64(7)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.getAsInt64("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(1).getAsInt64("i")
    }
  }

  @Test
  func getAsDouble() throws {
    let row = Row(
      valueArray: [Double(1.5), Float(2.5), nil, "3.5", 10],
      schema: RowSchema(["d", "f", "n", "s", "i"]))

    #expect(try row.getAsDouble(0) == 1.5)
    #expect(try row.getAsDouble("d") == 1.5)
    #expect(try row.getAsDouble(1) == Double(Float(2.5)))
    #expect(try row.getAsDouble("f") == Double(Float(2.5)))

    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDouble(2)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDouble("n")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDouble(3)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDouble("s")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDouble(4)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsDouble(-1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsDouble(5)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.getAsDouble("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(1.5).getAsDouble("d")
    }
  }

  @Test
  func getAsString() throws {
    let row = Row(
      valueArray: ["spark", nil, 123],
      schema: RowSchema(["s", "n", "i"]))

    #expect(try row.getAsString(0) == "spark")
    #expect(try row.getAsString("s") == "spark")

    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsString(1)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsString("n")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsString(2)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsString("i")
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsString(-1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsString(3)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.getAsString("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row("a").getAsString("s")
    }
  }

  @Test
  func getAsDate() throws {
    let date = Date(timeIntervalSince1970: 1_700_000_000)
    let ts = TimestampNanos(epochMicros: 1_700_000_000_000_000, nanosWithinMicro: 500)!
    let row = Row(
      valueArray: [date, ts, nil, "2026-01-01"],
      schema: RowSchema(["date", "ts", "n", "s"]))

    #expect(try row.getAsDate(0) == date)
    #expect(try row.getAsDate("date") == date)
    #expect(try row.getAsDate(1) == ts.date)
    #expect(try row.getAsDate("ts") == ts.date)

    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDate(2)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDate("n")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDate(3)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDate("s")
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsDate(-1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsDate(4)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.getAsDate("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(date).getAsDate("date")
    }
  }

  @Test
  func getAsTimestampNanos() throws {
    let date = Date(timeIntervalSince1970: 1_700_000_000)
    let ts = TimestampNanos(epochMicros: 1_700_000_000_000_000, nanosWithinMicro: 500)!
    let row = Row(
      valueArray: [ts, date, nil, 100],
      schema: RowSchema(["ts", "date", "n", "i"]))

    #expect(try row.getAsTimestampNanos(0) == ts)
    #expect(try row.getAsTimestampNanos("ts") == ts)
    #expect(try row.getAsTimestampNanos(1) == TimestampNanos(epochNanos: Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())))
    #expect(try row.getAsTimestampNanos("date") == TimestampNanos(epochNanos: Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())))

    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsTimestampNanos(2)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsTimestampNanos("n")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsTimestampNanos(3)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsTimestampNanos("i")
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsTimestampNanos(-1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsTimestampNanos(4)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.getAsTimestampNanos("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(ts).getAsTimestampNanos("ts")
    }
  }

  @Test
  func getAsDecimal() throws {
    let dec = Decimal(12.34)
    let row = Row(
      valueArray: [dec, Int32(100), nil, "12.34"],
      schema: RowSchema(["dec", "int", "n", "s"]))

    #expect(try row.getAsDecimal(0) == dec)
    #expect(try row.getAsDecimal("dec") == dec)
    #expect(try row.getAsDecimal(1) == Decimal(100))
    #expect(try row.getAsDecimal("int") == Decimal(100))

    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDecimal(2)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDecimal("n")
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDecimal(3)
    }
    #expect(throws: SparkConnectError.InvalidType) {
      try row.getAsDecimal("s")
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsDecimal(-1)
    }
    #expect(throws: SparkConnectError.InvalidArgument) {
      try row.getAsDecimal(4)
    }
    #expect(throws: SparkConnectError.ColumnNotFound) {
      try row.getAsDecimal("missing")
    }
    #expect(throws: SparkConnectError.UnsupportedOperation) {
      try Row(dec).getAsDecimal("dec")
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
