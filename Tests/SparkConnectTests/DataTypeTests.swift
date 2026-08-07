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

/// A test suite for `DataType`, `StructField`, and `StructType`
@Suite(.serialized)
struct DataTypeTests {
  @Test
  func simpleString() {
    #expect(DataType.null.simpleString == "void")
    #expect(DataType.binary.simpleString == "binary")
    #expect(DataType.boolean.simpleString == "boolean")
    #expect(DataType.byte.simpleString == "tinyint")
    #expect(DataType.short.simpleString == "smallint")
    #expect(DataType.integer.simpleString == "int")
    #expect(DataType.long.simpleString == "bigint")
    #expect(DataType.float.simpleString == "float")
    #expect(DataType.double.simpleString == "double")
    #expect(DataType.decimal(precision: 10, scale: 2).simpleString == "decimal(10,2)")
    #expect(DataType.string.simpleString == "string")
    #expect(DataType.char(length: 5).simpleString == "char(5)")
    #expect(DataType.varchar(length: 10).simpleString == "varchar(10)")
    #expect(DataType.date.simpleString == "date")
    #expect(DataType.timestamp.simpleString == "timestamp")
    #expect(DataType.timestampNtz.simpleString == "timestamp_ntz")
    #expect(DataType.time(precision: 6).simpleString == "time(6)")
    #expect(DataType.calendarInterval.simpleString == "interval")
    #expect(DataType.variant.simpleString == "variant")
    #expect(DataType.geometry(srid: -1).simpleString == "geometry(any)")
    #expect(DataType.geometry(srid: 4326).simpleString == "geometry(4326)")
    #expect(DataType.geography(srid: -1).simpleString == "geography(any)")
    #expect(DataType.geography(srid: 4326).simpleString == "geography(4326)")
    #expect(DataType.udt(UserDefinedType()).simpleString == "udt")
    #expect(DataType.unparsed("unknown").simpleString == "unknown")
  }

  @Test
  func intervalSimpleString() {
    #expect(
      DataType.yearMonthInterval(startField: .year, endField: .year).simpleString
        == "interval year")
    #expect(
      DataType.yearMonthInterval(startField: .year, endField: .month).simpleString
        == "interval year to month")
    #expect(
      DataType.dayTimeInterval(startField: .day, endField: .day).simpleString == "interval day")
    #expect(
      DataType.dayTimeInterval(startField: .day, endField: .second).simpleString
        == "interval day to second")
    #expect(
      DataType.dayTimeInterval(startField: .hour, endField: .minute).simpleString
        == "interval hour to minute")
  }

  @Test
  func nestedSimpleString() {
    #expect(
      DataType.array(elementType: .integer, containsNull: true).simpleString == "array<int>")
    #expect(
      DataType.map(keyType: .string, valueType: .long, valueContainsNull: true).simpleString
        == "map<string,bigint>")
    let structType = StructType(fields: [
      StructField(name: "id", dataType: .long),
      StructField(name: "arr", dataType: .array(elementType: .string, containsNull: false)),
    ])
    #expect(DataType.struct(structType).simpleString == "struct<id:bigint,arr:array<string>>")
  }

  @Test
  func schema() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      """
      SELECT
        CAST(1 AS TINYINT) t,
        CAST(1 AS SMALLINT) s,
        CAST(1 AS INT) i,
        CAST(1 AS BIGINT) l,
        CAST(1.0 AS FLOAT) f,
        CAST(1.0 AS DOUBLE) d,
        CAST(1.5 AS DECIMAL(10,2)) dec,
        'a' str,
        CAST('a' AS BINARY) bin,
        true b,
        DATE'2025-01-01' dt,
        TIMESTAMP'2025-01-01 00:00:00' ts
      """)
    let schema = try await df.schema
    #expect(schema.count == 12)
    #expect(schema["t"]?.dataType == .byte)
    #expect(schema["s"]?.dataType == .short)
    #expect(schema["i"]?.dataType == .integer)
    #expect(schema["l"]?.dataType == .long)
    #expect(schema["f"]?.dataType == .float)
    #expect(schema["d"]?.dataType == .double)
    #expect(schema["dec"]?.dataType == .decimal(precision: 10, scale: 2))
    #expect(schema["str"]?.dataType == .string)
    #expect(schema["bin"]?.dataType == .binary)
    #expect(schema["b"]?.dataType == .boolean)
    #expect(schema["dt"]?.dataType == .date)
    #expect(schema["ts"]?.dataType == .timestamp)
    await spark.stop()
  }

  @Test
  func schemaComplexTypes() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      """
      SELECT
        ARRAY(1, 2) arr,
        MAP('k', 1) m,
        NAMED_STRUCT('a', 1, 'b', ARRAY('x')) st
      """)
    let schema = try await df.schema
    #expect(schema.fieldNames == ["arr", "m", "st"])
    #expect(schema["arr"]?.dataType == .array(elementType: .integer, containsNull: false))
    #expect(
      schema["m"]?.dataType
        == .map(keyType: .string, valueType: .integer, valueContainsNull: false))
    #expect(
      schema["st"]?.dataType
        == .struct(
          StructType(fields: [
            StructField(name: "a", dataType: .integer, nullable: false),
            StructField(
              name: "b", dataType: .array(elementType: .string, containsNull: false),
              nullable: false),
          ])))
    await spark.stop()
  }

  @Test
  func schemaIntervalTypes() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql(
      "SELECT INTERVAL '1' YEAR ym, INTERVAL '1 2:3:4' DAY TO SECOND dts")
    let schema = try await df.schema
    #expect(schema["ym"]?.dataType == .yearMonthInterval(startField: .year, endField: .year))
    #expect(schema["dts"]?.dataType == .dayTimeInterval(startField: .day, endField: .second))
    await spark.stop()
  }

  @Test
  func schemaTimeType() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await spark.version >= "4.2" {
      try await spark.conf.set("spark.sql.timeType.enabled", "true")
      let df = try await spark.sql(
        "SELECT TIME'12:34:56' t6, CAST(TIME'12:34:56' AS TIME(0)) t0")
      let schema = try await df.schema
      #expect(schema["t6"]?.dataType == .time(precision: 6))
      #expect(schema["t0"]?.dataType == .time(precision: 0))
    }
    await spark.stop()
  }

  @Test
  func schemaNullType() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let schema = try await spark.sql("SELECT NULL n").schema
    #expect(schema["n"]?.dataType == .null)
    #expect(schema["n"]?.nullable == true)
    await spark.stop()
  }
}
