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

// MARK: - Geospatial ST functions

/// Returns the input `GEOGRAPHY` or `GEOMETRY` value in WKB (Well-Known Binary) format.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter geo: A ``Column`` that evaluates to a `GEOGRAPHY` or a `GEOMETRY`.
/// - Returns: A ``Column`` that evaluates to a binary.
public func st_asbinary(_ geo: Column) -> Column {
  return fn("st_asbinary", geo)
}

/// Returns the input `GEOGRAPHY` or `GEOMETRY` value in WKB (Well-Known Binary) format using the
/// specified endianness.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - geo: A ``Column`` that evaluates to a `GEOGRAPHY` or a `GEOMETRY`.
///   - endianness: A ``Column`` that evaluates to a string, `NDR` for little-endian or `XDR` for
///     big-endian.
/// - Returns: A ``Column`` that evaluates to a binary.
public func st_asbinary(_ geo: Column, _ endianness: Column) -> Column {
  return fn("st_asbinary", geo, endianness)
}

/// Returns the input `GEOGRAPHY` or `GEOMETRY` value in WKB (Well-Known Binary) format using the
/// specified endianness.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - geo: A ``Column`` that evaluates to a `GEOGRAPHY` or a `GEOMETRY`.
///   - endianness: The endianness of the output WKB, `NDR` for little-endian or `XDR` for
///     big-endian.
/// - Returns: A ``Column`` that evaluates to a binary.
public func st_asbinary(_ geo: Column, _ endianness: String) -> Column {
  return fn("st_asbinary", geo, lit(endianness))
}

/// Parses the WKB (Well-Known Binary) description of a geography and returns the corresponding
/// `GEOGRAPHY` value.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter wkb: A ``Column`` that evaluates to a binary in WKB format.
/// - Returns: A ``Column`` that evaluates to a `GEOGRAPHY`.
public func st_geogfromwkb(_ wkb: Column) -> Column {
  return fn("st_geogfromwkb", wkb)
}

/// Parses the WKB (Well-Known Binary) description of a geometry and returns the corresponding
/// `GEOMETRY` value whose SRID is `0`.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter wkb: A ``Column`` that evaluates to a binary in WKB format.
/// - Returns: A ``Column`` that evaluates to a `GEOMETRY`.
public func st_geomfromwkb(_ wkb: Column) -> Column {
  return fn("st_geomfromwkb", wkb)
}

/// Parses the WKB (Well-Known Binary) description of a geometry and returns the corresponding
/// `GEOMETRY` value.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - wkb: A ``Column`` that evaluates to a binary in WKB format.
///   - srid: A ``Column`` that evaluates to an integer, the SRID (Spatial Reference System
///     Identifier) of the geometry.
/// - Returns: A ``Column`` that evaluates to a `GEOMETRY`.
public func st_geomfromwkb(_ wkb: Column, _ srid: Column) -> Column {
  return fn("st_geomfromwkb", wkb, srid)
}

/// Parses the WKB (Well-Known Binary) description of a geometry and returns the corresponding
/// `GEOMETRY` value.
/// This requires Apache Spark 4.2.0 or later.
/// - Parameters:
///   - wkb: A ``Column`` that evaluates to a binary in WKB format.
///   - srid: The SRID (Spatial Reference System Identifier) of the geometry.
/// - Returns: A ``Column`` that evaluates to a `GEOMETRY`.
public func st_geomfromwkb(_ wkb: Column, _ srid: Int32) -> Column {
  return fn("st_geomfromwkb", wkb, lit(srid))
}

/// Returns a new `GEOGRAPHY` or `GEOMETRY` value whose SRID is the specified SRID value.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - geo: A ``Column`` that evaluates to a `GEOGRAPHY` or a `GEOMETRY`.
///   - srid: A ``Column`` that evaluates to an integer, the new SRID (Spatial Reference System
///     Identifier) of the geospatial value.
/// - Returns: A ``Column`` that evaluates to a `GEOGRAPHY` or a `GEOMETRY`.
public func st_setsrid(_ geo: Column, _ srid: Column) -> Column {
  return fn("st_setsrid", geo, srid)
}

/// Returns a new `GEOGRAPHY` or `GEOMETRY` value whose SRID is the specified SRID value.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameters:
///   - geo: A ``Column`` that evaluates to a `GEOGRAPHY` or a `GEOMETRY`.
///   - srid: The new SRID (Spatial Reference System Identifier) of the geospatial value.
/// - Returns: A ``Column`` that evaluates to a `GEOGRAPHY` or a `GEOMETRY`.
public func st_setsrid(_ geo: Column, _ srid: Int32) -> Column {
  return fn("st_setsrid", geo, lit(srid))
}

/// Returns the SRID (Spatial Reference System Identifier) of the input `GEOGRAPHY` or `GEOMETRY`
/// value.
/// This requires Apache Spark 4.1.0 or later.
/// - Parameter geo: A ``Column`` that evaluates to a `GEOGRAPHY` or a `GEOMETRY`.
/// - Returns: A ``Column`` that evaluates to an integer.
public func st_srid(_ geo: Column) -> Column {
  return fn("st_srid", geo)
}
