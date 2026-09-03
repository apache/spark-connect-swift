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

/// A test suite for `GeospatialFunctions`
///
/// The ST functions were added in Apache Spark 4.1.0, but `spark.sql.geospatial.enabled` only
/// defaults to `true` since Apache Spark 4.2.0. So, the server-side tests are gated by `4.2` like
/// the existing `DataFrameTests.dtypesGeospatial` test.
@Suite(.serialized)
struct GeospatialFunctionsTests {

  /// The WKB representation of `POINT(1 2)` in little-endian order.
  static let wkbHex = "0101000000000000000000F03F0000000000000040"

  /// The WKB representation of `POINT(1 2)` in big-endian order.
  static let wkbHexBigEndian = "00000000013FF00000000000004000000000000000"

  static let wkb = unhex(lit(wkbHex))

  @Test
  func geospatialFunctions() throws {
    for (column, name) in [
      (st_asbinary(col("a")), "st_asbinary"),
      (st_geogfromwkb(col("a")), "st_geogfromwkb"),
      (st_geomfromwkb(col("a")), "st_geomfromwkb"),
      (st_srid(col("a")), "st_srid"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 1)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  /// An `Int32` SRID is a shorthand for a literal ``Column`` SRID.
  @Test
  func srid() throws {
    for (column, name) in [
      (st_geomfromwkb(col("a"), 4326), "st_geomfromwkb"),
      (st_setsrid(col("a"), 4326), "st_setsrid"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
      #expect(expr.unresolvedFunction.arguments[1].literal.integer == 4326)
    }

    #expect(
      st_geomfromwkb(col("a"), lit(4326 as Int32)).expr == st_geomfromwkb(col("a"), 4326).expr)
    #expect(st_setsrid(col("a"), lit(4326 as Int32)).expr == st_setsrid(col("a"), 4326).expr)

    let srid = st_setsrid(col("a"), col("s")).expr
    #expect(srid.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "s")
  }

  /// A `String` endianness is a shorthand for a literal ``Column`` endianness.
  @Test
  func endianness() throws {
    let expr = st_asbinary(col("a"), "XDR").expr
    #expect(expr.unresolvedFunction.functionName == "st_asbinary")
    #expect(expr.unresolvedFunction.arguments.count == 2)
    #expect(expr.unresolvedFunction.arguments[1].literal.string == "XDR")

    #expect(st_asbinary(col("a"), lit("XDR")).expr == st_asbinary(col("a"), "XDR").expr)
  }

  @Test
  func geomFromWkb() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let df = try await spark.range(1).select(st_geomfromwkb(Self.wkb))
      #expect(try await df.dtypes[0].1 == "geometry(0)")
      let rows = try await spark.range(1).select(st_srid(st_geomfromwkb(Self.wkb))).collect()
      #expect(rows == [Row(Int32(0))])
    }
    await spark.stop()
  }

  @Test
  func geogFromWkb() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let df = try await spark.range(1).select(st_geogfromwkb(Self.wkb))
      #expect(try await df.dtypes[0].1 == "geography(4326)")
      let rows = try await spark.range(1).select(st_srid(st_geogfromwkb(Self.wkb))).collect()
      #expect(rows == [Row(Int32(4326))])
    }
    await spark.stop()
  }

  @Test
  func setSrid() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let df = try await spark.range(1).select(st_setsrid(st_geomfromwkb(Self.wkb), 4326))
      #expect(try await df.dtypes[0].1 == "geometry(4326)")
      let rows = try await spark.range(1).select(
        st_srid(st_setsrid(st_geomfromwkb(Self.wkb), 4326)),
        st_srid(st_setsrid(st_geomfromwkb(Self.wkb), lit(3857 as Int32)))
      ).collect()
      #expect(rows == [Row(Int32(4326), Int32(3857))])
    }
    await spark.stop()
  }

  @Test
  func asBinary() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let rows = try await spark.range(1).select(
        hex(st_asbinary(st_geomfromwkb(Self.wkb))),
        hex(st_asbinary(st_geogfromwkb(Self.wkb)))
      ).collect()
      #expect(rows == [Row(Self.wkbHex, Self.wkbHex)])
    }
    await spark.stop()
  }

  /// The `endianness` argument of `st_asbinary` requires Apache Spark 4.2.0 or later.
  @Test
  func asBinaryWithEndianness() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let rows = try await spark.range(1).select(
        hex(st_asbinary(st_geomfromwkb(Self.wkb), "NDR")),
        hex(st_asbinary(st_geomfromwkb(Self.wkb), "XDR")),
        hex(st_asbinary(st_geogfromwkb(Self.wkb), lit("XDR")))
      ).collect()
      #expect(rows == [Row(Self.wkbHex, Self.wkbHexBigEndian, Self.wkbHexBigEndian)])
    }
    await spark.stop()
  }

  /// The `srid` argument of `st_geomfromwkb` requires Apache Spark 4.2.0 or later.
  @Test
  func geomFromWkbWithSrid() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.2") {
      let df = try await spark.range(1).select(st_geomfromwkb(Self.wkb, 4326))
      #expect(try await df.dtypes[0].1 == "geometry(4326)")
      let rows = try await spark.range(1).select(
        st_srid(st_geomfromwkb(Self.wkb, 4326)),
        st_srid(st_geomfromwkb(Self.wkb, lit(3857 as Int32)))
      ).collect()
      #expect(rows == [Row(Int32(4326), Int32(3857))])
    }
    await spark.stop()
  }
}
