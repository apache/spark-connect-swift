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

  public func getAsBool(_ i: Int) throws -> Bool {
    return try get(i) as! Bool
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
