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

/// A schema of ``Row`` holding the field names. Like Scala's `StructType`, a single
/// `RowSchema` instance is shared by all ``Row``s of the same result.
public final class RowSchema: Sendable {
  public let fieldNames: [String]
  let nameToIndex: [String: Int]

  public init(_ fieldNames: [String]) {
    self.fieldNames = fieldNames
    var nameToIndex = [String: Int]()
    for (index, name) in fieldNames.enumerated() {
      nameToIndex[name] = index
    }
    self.nameToIndex = nameToIndex
  }
}

public struct Row: Sendable, Equatable {
  let values: [Sendable?]
  let schema: RowSchema?

  public init(_ values: Sendable?...) {
    self.values = values
    self.schema = nil
  }

  public init(valueArray: [Sendable?], schema: RowSchema? = nil) {
    self.values = valueArray
    self.schema = schema
  }

  public static var empty: Row {
    return Row()
  }

  public var size: Int { return length }

  public var length: Int { return values.count }

  subscript(index: Int) -> Sendable {
    get throws {
      return try get(index)
    }
  }

  public subscript(name: String) -> Sendable {
    get throws {
      return try get(name)
    }
  }

  public func get(_ i: Int) throws -> Sendable {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    return values[i]
  }

  /// Returns the value of the field with the given name.
  /// - Parameter name: A field name.
  /// - Returns: A value of the field.
  public func get(_ name: String) throws -> Sendable {
    return values[try fieldIndex(name)]
  }

  /// Returns the index of the field with the given name. If multiple fields have the same
  /// name, the last one is returned like Scala's `StructType.fieldIndex`.
  /// - Parameter name: A field name.
  /// - Returns: An index of the field.
  public func fieldIndex(_ name: String) throws -> Int {
    guard let schema = self.schema else {
      throw SparkConnectError.UnsupportedOperation
    }
    guard let index = schema.nameToIndex[name] else {
      throw SparkConnectError.ColumnNotFound
    }
    return index
  }

  /// Returns the row as a dictionary from field names to values. If multiple fields have
  /// the same name, the last value is used.
  /// - Returns: A dictionary from field names to values.
  public func asDict() throws -> [String: Sendable?] {
    guard let schema = self.schema else {
      throw SparkConnectError.UnsupportedOperation
    }
    var dict = [String: Sendable?]()
    for (index, name) in schema.fieldNames.enumerated() {
      dict.updateValue(values[index], forKey: name)
    }
    return dict
  }

  /// Returns whether the value at the given index is null.
  /// - Parameter i: A 0-based column index.
  /// - Returns: `true` if the column is null, `false` otherwise.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range.
  public func isNullAt(_ i: Int) throws -> Bool {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    return values[i] == nil
  }

  /// Returns whether the value of the field with the given name is null.
  /// - Parameter name: A field name.
  /// - Returns: `true` if the column is null, `false` otherwise.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema, or
  /// `SparkConnectError.ColumnNotFound` if the field does not exist.
  public func isNullAt(_ name: String) throws -> Bool {
    return try isNullAt(fieldIndex(name))
  }

  /// Returns the value at the given index cast or coerced to the specified type `T`.
  /// - Parameters:
  ///   - i: A 0-based column index.
  ///   - type: The expected type to return.
  /// - Returns: The value as type `T`.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value cannot be converted to `T`.
  public func getAs<T>(_ i: Int, _ type: T.Type = T.self) throws -> T {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    if let value = values[i] as? T {
      return value
    }
    if T.self == Int.self, let intVal = values[i] as? any FixedWidthInteger,
      let result = Int(exactly: intVal) as? T
    {
      return result
    }
    if T.self == Int64.self, let intVal = values[i] as? any FixedWidthInteger,
      let result = Int64(exactly: intVal) as? T
    {
      return result
    }
    if T.self == Double.self, let floatVal = values[i] as? Float,
      let result = Double(floatVal) as? T
    {
      return result
    }
    if T.self == Date.self, let ts = values[i] as? TimestampNanos,
      let result = ts.date as? T
    {
      return result
    }
    throw SparkConnectError.InvalidType
  }

  /// Returns the value of the field with the given name cast or coerced to the specified type `T`.
  /// - Parameters:
  ///   - name: A field name.
  ///   - type: The expected type to return.
  /// - Returns: The value as type `T`.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value cannot be converted to `T`.
  public func getAs<T>(_ name: String, _ type: T.Type = T.self) throws -> T {
    return try getAs(fieldIndex(name), type)
  }

  /// Returns the value at the given index as a `Bool`.
  /// - Parameter i: A 0-based column index.
  /// - Returns: A `Bool` value of the field.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not a `Bool`.
  public func getAsBool(_ i: Int) throws -> Bool {
    guard let value = try get(i) as? Bool else {
      throw SparkConnectError.InvalidType
    }
    return value
  }

  /// Returns the value of the field with the given name as a `Bool`.
  /// - Parameter name: A field name.
  /// - Returns: A `Bool` value of the field.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not a `Bool`.
  public func getAsBool(_ name: String) throws -> Bool {
    return try getAsBool(fieldIndex(name))
  }

  /// Returns the value at the given index as an `Int`, supporting integer type coercion.
  /// - Parameter i: A 0-based column index.
  /// - Returns: An `Int` value of the field.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value is `nil`, not an integer, or overflows `Int`.
  public func getAsInt(_ i: Int) throws -> Int {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    guard let raw = values[i],
      let intVal = raw as? any FixedWidthInteger,
      let result = Int(exactly: intVal)
    else {
      throw SparkConnectError.InvalidType
    }
    return result
  }

  /// Returns the value of the field with the given name as an `Int`, supporting integer type coercion.
  /// - Parameter name: A field name.
  /// - Returns: An `Int` value of the field.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value is `nil`, not an integer, or overflows `Int`.
  public func getAsInt(_ name: String) throws -> Int {
    return try getAsInt(fieldIndex(name))
  }

  /// Returns the value at the given index as an `Int64`, supporting integer type coercion.
  /// - Parameter i: A 0-based column index.
  /// - Returns: An `Int64` value of the field.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value is `nil`, not an integer, or overflows `Int64`.
  public func getAsInt64(_ i: Int) throws -> Int64 {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    guard let raw = values[i],
      let intVal = raw as? any FixedWidthInteger,
      let result = Int64(exactly: intVal)
    else {
      throw SparkConnectError.InvalidType
    }
    return result
  }

  /// Returns the value of the field with the given name as an `Int64`, supporting integer type coercion.
  /// - Parameter name: A field name.
  /// - Returns: An `Int64` value of the field.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value is `nil`, not an integer, or overflows `Int64`.
  public func getAsInt64(_ name: String) throws -> Int64 {
    return try getAsInt64(fieldIndex(name))
  }

  /// Returns the value at the given index as a `Double`, supporting `Float` and `Double`.
  /// - Parameter i: A 0-based column index.
  /// - Returns: A `Double` value of the field.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not a floating-point number.
  public func getAsDouble(_ i: Int) throws -> Double {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    guard let raw = values[i] else {
      throw SparkConnectError.InvalidType
    }
    if let d = raw as? Double {
      return d
    }
    if let f = raw as? Float {
      return Double(f)
    }
    throw SparkConnectError.InvalidType
  }

  /// Returns the value of the field with the given name as a `Double`, supporting `Float` and `Double`.
  /// - Parameter name: A field name.
  /// - Returns: A `Double` value of the field.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not a floating-point number.
  public func getAsDouble(_ name: String) throws -> Double {
    return try getAsDouble(fieldIndex(name))
  }

  /// Returns the value at the given index as a `String`.
  /// - Parameter i: A 0-based column index.
  /// - Returns: A `String` value of the field.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not a `String`.
  public func getAsString(_ i: Int) throws -> String {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    guard let raw = values[i], let s = raw as? String else {
      throw SparkConnectError.InvalidType
    }
    return s
  }

  /// Returns the value of the field with the given name as a `String`.
  /// - Parameter name: A field name.
  /// - Returns: A `String` value of the field.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not a `String`.
  public func getAsString(_ name: String) throws -> String {
    return try getAsString(fieldIndex(name))
  }

  /// Returns the value at the given index as a `Date`.
  /// - Parameter i: A 0-based column index.
  /// - Returns: A `Date` value of the field.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not convertible to `Date`.
  public func getAsDate(_ i: Int) throws -> Date {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    guard let raw = values[i] else {
      throw SparkConnectError.InvalidType
    }
    if let date = raw as? Date {
      return date
    }
    if let ts = raw as? TimestampNanos {
      return ts.date
    }
    throw SparkConnectError.InvalidType
  }

  /// Returns the value of the field with the given name as a `Date`.
  /// - Parameter name: A field name.
  /// - Returns: A `Date` value of the field.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not convertible to `Date`.
  public func getAsDate(_ name: String) throws -> Date {
    return try getAsDate(fieldIndex(name))
  }

  /// Returns the value at the given index as a `TimestampNanos`.
  /// - Parameter i: A 0-based column index.
  /// - Returns: A `TimestampNanos` value of the field.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not convertible to `TimestampNanos`.
  public func getAsTimestampNanos(_ i: Int) throws -> TimestampNanos {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    guard let raw = values[i] else {
      throw SparkConnectError.InvalidType
    }
    if let ts = raw as? TimestampNanos {
      return ts
    }
    if let date = raw as? Date {
      return TimestampNanos(epochNanos: Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded()))
    }
    throw SparkConnectError.InvalidType
  }

  /// Returns the value of the field with the given name as a `TimestampNanos`.
  /// - Parameter name: A field name.
  /// - Returns: A `TimestampNanos` value of the field.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not convertible to `TimestampNanos`.
  public func getAsTimestampNanos(_ name: String) throws -> TimestampNanos {
    return try getAsTimestampNanos(fieldIndex(name))
  }

  /// Returns the value at the given index as a `Decimal`.
  /// - Parameter i: A 0-based column index.
  /// - Returns: A `Decimal` value of the field.
  /// - Throws: `SparkConnectError.InvalidArgument` if the index is out of range, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not a `Decimal`.
  public func getAsDecimal(_ i: Int) throws -> Decimal {
    if i < 0 || i >= self.length {
      throw SparkConnectError.InvalidArgument
    }
    guard let raw = values[i] else {
      throw SparkConnectError.InvalidType
    }
    if let dec = raw as? Decimal {
      return dec
    }
    if let intVal = raw as? any FixedWidthInteger, let int64 = Int64(exactly: intVal) {
      return Decimal(int64)
    }
    throw SparkConnectError.InvalidType
  }

  /// Returns the value of the field with the given name as a `Decimal`.
  /// - Parameter name: A field name.
  /// - Returns: A `Decimal` value of the field.
  /// - Throws: `SparkConnectError.UnsupportedOperation` if the row has no schema,
  /// `SparkConnectError.ColumnNotFound` if the field does not exist, or
  /// `SparkConnectError.InvalidType` if the value is `nil` or not a `Decimal`.
  public func getAsDecimal(_ name: String) throws -> Decimal {
    return try getAsDecimal(fieldIndex(name))
  }

  public static func == (lhs: Row, rhs: Row) -> Bool {
    if lhs.values.count != rhs.values.count {
      return false
    }
    return lhs.values.elementsEqual(rhs.values) { (x, y) in
      if x == nil && y == nil {
        return true
      } else if let a = x as? Bool, let b = y as? Bool {
        return a == b
      } else if let a = x as? any FixedWidthInteger, let b = y as? any FixedWidthInteger {
        return Int64(a) == Int64(b)
      } else if let a = x as? Float, let b = y as? Float {
        return a == b
      } else if let a = x as? Double, let b = y as? Double {
        return a == b
      } else if let a = x as? Decimal, let b = y as? Decimal {
        return a == b
      } else if let a = x as? Date, let b = y as? Date {
        return a == b
      } else if let a = x as? LocalTime, let b = y as? LocalTime {
        return a == b
      } else if let a = x as? TimestampNanos, let b = y as? TimestampNanos {
        return a == b
      } else if let a = x as? String, let b = y as? String {
        return a == b
      } else if let a = x as? [Bool], let b = y as? [Bool] {
        return a == b
      } else if let a = x as? [any FixedWidthInteger], let b = y as? [any FixedWidthInteger] {
        return a.map { Int64($0) } == b.map { Int64($0) }
      } else if let a = x as? [Float], let b = y as? [Float] {
        return a == b
      } else if let a = x as? [Double], let b = y as? [Double] {
        return a == b
      } else if let a = x as? [Decimal], let b = y as? [Decimal] {
        return a == b
      } else if let a = x as? [Date], let b = y as? [Date] {
        return a == b
      } else if let a = x as? [String], let b = y as? [String] {
        return a == b
      } else {
        return false
      }
    }
  }

  public func toString() -> String {
    return "[\(self.values.map { "\($0 ?? "null")" }.joined(separator: ","))]"
  }
}

extension Row {
}
