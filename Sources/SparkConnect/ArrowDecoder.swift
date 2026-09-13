// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
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

/// @nodoc
public class ArrowDecoder: Decoder {
  var rbIndex: UInt = 0
  var singleRBCol: Int = 0
  public var codingPath: [CodingKey] = []
  public var userInfo: [CodingUserInfoKey: Any] = [:]
  public let rb: RecordBatch
  public let nameToCol: [String: ArrowArrayHolder]
  public let columns: [ArrowArrayHolder]
  public init(_ decoder: ArrowDecoder) {
    self.userInfo = decoder.userInfo
    self.codingPath = decoder.codingPath
    self.rb = decoder.rb
    self.columns = decoder.columns
    self.nameToCol = decoder.nameToCol
    self.rbIndex = decoder.rbIndex
  }

  public init(_ rb: RecordBatch) {
    self.rb = rb
    var colMapping = [String: ArrowArrayHolder]()
    var columns = [ArrowArrayHolder]()
    for index in 0..<self.rb.schema.fields.count {
      let field = self.rb.schema.fields[index]
      columns.append(self.rb.column(index))
      colMapping[field.name] = self.rb.column(index)
    }

    self.columns = columns
    self.nameToCol = colMapping
  }

  public func decode<T: Decodable, U: Decodable>(_ type: [T: U].Type) throws -> [T: U] {
    var output = [T: U]()
    if rb.columnCount != 2 {
      throw ArrowError.invalid("RecordBatch column count of 2 is required to decode to map")
    }

    for index in 0..<rb.length {
      self.rbIndex = index
      self.singleRBCol = 0
      let key = try T.init(from: self)
      self.singleRBCol = 1
      let value = try U.init(from: self)
      output[key] = value
    }

    self.singleRBCol = 0
    return output
  }

  public func decode<T: Decodable>(_ type: T.Type) throws -> [T] {
    var output = [T]()
    for index in 0..<rb.length {
      self.rbIndex = index
      output.append(try type.init(from: self))
    }

    return output
  }

  public func container<Key>(
    keyedBy type: Key.Type
  ) -> KeyedDecodingContainer<Key> where Key: CodingKey {
    let container = ArrowKeyedDecoding<Key>(self, codingPath: codingPath)
    return KeyedDecodingContainer(container)
  }

  public func unkeyedContainer() -> UnkeyedDecodingContainer {
    return ArrowUnkeyedDecoding(self, codingPath: codingPath)
  }

  public func singleValueContainer() -> SingleValueDecodingContainer {
    return ArrowSingleValueDecoding(self, codingPath: codingPath)
  }

  func getCol(_ name: String) throws -> AnyArray {
    guard let col = self.nameToCol[name] else {
      throw ArrowError.invalid("Column for key \"\(name)\" not found")
    }

    return col.array
  }

  func getCol(_ index: Int) throws -> AnyArray {
    if index >= self.columns.count {
      throw ArrowError.outOfBounds(index: Int64(index))
    }

    return self.columns[index].array
  }

  func doDecode<T>(_ key: CodingKey) throws -> T {
    let array: AnyArray = try self.getCol(key.stringValue)
    return try self.decodeValue(from: array, keyDescription: key.stringValue)
  }

  func doDecode<T>(_ col: Int) throws -> T {
    let array: AnyArray = try self.getCol(col)
    return try self.decodeValue(from: array, keyDescription: "column \(col)")
  }

  func decodeValue<T>(from array: AnyArray, keyDescription: String) throws -> T {
    let raw = array.asAny(self.rbIndex)
    if let val = raw as? T {
      return val
    }
    // Allow safe upcasts like Scala Dataset `as[T]`, following Spark's numeric precedence:
    // Byte < Short < Int < Long < Float < Double.
    if let val = raw as? any FixedWidthInteger & SignedInteger,
      let intType = T.self as? any (FixedWidthInteger & SignedInteger).Type,
      val.bitWidth < intType.bitWidth
    {
      return intType.init(val) as! T
    } else if let val = raw as? any FixedWidthInteger & SignedInteger, T.self == Float.self {
      return Float(val) as! T
    } else if let val = raw as? any FixedWidthInteger & SignedInteger, T.self == Double.self {
      return Double(val) as! T
    } else if let val = raw as? Float, T.self == Double.self {
      return Double(val) as! T
    }
    throw ArrowError.invalid("Cannot decode \(T.self) for \(keyDescription)")
  }

  func isNull(_ key: CodingKey) throws -> Bool {
    let array: AnyArray = try self.getCol(key.stringValue)
    return array.asAny(self.rbIndex) == nil
  }

  func isNull(_ col: Int) throws -> Bool {
    let array: AnyArray = try self.getCol(col)
    return array.asAny(self.rbIndex) == nil
  }

  func decodeInt(from array: AnyArray, keyDescription: String) throws -> Int {
    let raw = array.asAny(self.rbIndex)
    if let val = raw as? Int64 {
      return Int(val)
    } else if let val = raw as? Int32 {
      return Int(val)
    } else if let val = raw as? Int16 {
      return Int(val)
    } else if let val = raw as? Int8 {
      return Int(val)
    } else if let val = raw as? UInt64 {
      return Int(val)
    } else if let val = raw as? UInt32 {
      return Int(val)
    } else if let val = raw as? UInt16 {
      return Int(val)
    } else if let val = raw as? UInt8 {
      return Int(val)
    }
    throw ArrowError.invalid("Cannot decode Int for \(keyDescription)")
  }

  func decodeUInt(from array: AnyArray, keyDescription: String) throws -> UInt {
    let raw = array.asAny(self.rbIndex)
    if let val = raw as? UInt64 {
      return UInt(val)
    } else if let val = raw as? UInt32 {
      return UInt(val)
    } else if let val = raw as? UInt16 {
      return UInt(val)
    } else if let val = raw as? UInt8 {
      return UInt(val)
    } else if let val = raw as? Int64, val >= 0 {
      return UInt(val)
    } else if let val = raw as? Int32, val >= 0 {
      return UInt(val)
    } else if let val = raw as? Int16, val >= 0 {
      return UInt(val)
    } else if let val = raw as? Int8, val >= 0 {
      return UInt(val)
    }
    throw ArrowError.invalid("Cannot decode UInt for \(keyDescription)")
  }

  func decodeDate(from array: AnyArray, keyDescription: String) throws -> Date {
    if let date = array.asAny(self.rbIndex) as? Date {
      return date
    }
    if let timestamp = array.asAny(self.rbIndex) as? Int64,
      let timestampType = array.arrowData.type as? ArrowTypeTimestamp
    {
      switch timestampType.unit {
      case .seconds:
        return Date(timeIntervalSince1970: TimeInterval(timestamp))
      case .milliseconds:
        return Date(timeIntervalSince1970: TimeInterval(timestamp) / 1_000)
      case .microseconds:
        return Date(timeIntervalSince1970: TimeInterval(timestamp) / 1_000_000)
      case .nanoseconds:
        return Date(timeIntervalSince1970: TimeInterval(timestamp) / 1_000_000_000)
      }
    }
    throw ArrowError.invalid("Cannot decode Date for \(keyDescription)")
  }

  func decodeTimestampNanos(from array: AnyArray, keyDescription: String) throws -> TimestampNanos {
    if let timestamp = array.asAny(self.rbIndex) as? Int64,
      let timestampType = array.arrowData.type as? ArrowTypeTimestamp
    {
      let epochNanos: Int64
      switch timestampType.unit {
      case .seconds: epochNanos = timestamp * 1_000_000_000
      case .milliseconds: epochNanos = timestamp * 1_000_000
      case .microseconds: epochNanos = timestamp * 1_000
      case .nanoseconds: epochNanos = timestamp
      }
      return TimestampNanos(epochNanos: epochNanos)
    }
    throw ArrowError.invalid("Cannot decode TimestampNanos for \(keyDescription)")
  }
}

private struct ArrowUnkeyedDecoding: UnkeyedDecodingContainer {
  var codingPath: [CodingKey]
  var count: Int? = 0
  var isAtEnd: Bool = false
  var currentIndex: Int = 0
  let decoder: ArrowDecoder

  init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
    self.decoder = decoder
    self.codingPath = codingPath
    self.count = self.decoder.columns.count
  }

  mutating func increment() {
    self.currentIndex += 1
    self.isAtEnd = self.currentIndex >= self.count!
  }

  mutating func decodeNil() throws -> Bool {
    defer { increment() }
    return try self.decoder.isNull(self.currentIndex)
  }

  mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
    if type == Int.self {
      defer { increment() }
      let col = try self.decoder.getCol(self.currentIndex)
      return try self.decoder.decodeInt(from: col, keyDescription: "column \(self.currentIndex)") as! T
    } else if type == UInt.self {
      defer { increment() }
      let col = try self.decoder.getCol(self.currentIndex)
      return try self.decoder.decodeUInt(from: col, keyDescription: "column \(self.currentIndex)") as! T
    } else if type == Date.self {
      defer { increment() }
      let col = try self.decoder.getCol(self.currentIndex)
      return try self.decoder.decodeDate(from: col, keyDescription: "column \(self.currentIndex)") as! T
    } else if type == TimestampNanos.self {
      defer { increment() }
      let col = try self.decoder.getCol(self.currentIndex)
      return try self.decoder.decodeTimestampNanos(
        from: col, keyDescription: "column \(self.currentIndex)") as! T
    } else if type == Int8?.self || type == Int16?.self || type == Int32?.self || type == Int64?.self
      || type == UInt8?.self || type == UInt16?.self || type == UInt32?.self || type == UInt64?.self
      || type == String?.self || type == Double?.self || type == Float?.self || type == Date?.self
      || type == Bool?.self || type == Bool.self || type == Int8.self || type == Int16.self
      || type == Int32.self || type == Int64.self || type == UInt8.self || type == UInt16.self
      || type == UInt32.self || type == UInt64.self || type == String.self || type == Double.self
      || type == Float.self || type == Date.self || type == Decimal.self || type == Decimal?.self
    {
      defer { increment() }
      return try self.decoder.doDecode(self.currentIndex)
    } else {
      throw ArrowError.invalid("Type \(type) is currently not supported")
    }
  }

  func nestedContainer<NestedKey>(
    keyedBy type: NestedKey.Type
  ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
    throw ArrowError.invalid("Nested decoding is currently not supported.")
  }

  func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
    throw ArrowError.invalid("Nested decoding is currently not supported.")
  }

  func superDecoder() throws -> Decoder {
    throw ArrowError.invalid("super decoding is currently not supported.")
  }
}

private struct ArrowKeyedDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
  var codingPath = [CodingKey]()
  var allKeys = [Key]()
  let decoder: ArrowDecoder

  init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
    self.decoder = decoder
    self.codingPath = codingPath
  }

  func contains(_ key: Key) -> Bool {
    return self.decoder.nameToCol.keys.contains(key.stringValue)
  }

  func decodeNil(forKey key: Key) throws -> Bool {
    try self.decoder.isNull(key)
  }

  func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: String.Type, forKey key: Key) throws -> String {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
    let col = try self.decoder.getCol(key.stringValue)
    return try self.decoder.decodeInt(from: col, keyDescription: key.stringValue)
  }

  func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
    let col = try self.decoder.getCol(key.stringValue)
    return try self.decoder.decodeUInt(from: col, keyDescription: key.stringValue)
  }

  func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: Decimal.Type, forKey key: Key) throws -> Decimal {
    return try self.decoder.doDecode(key)
  }

  func decode(_ type: Date.Type, forKey key: Key) throws -> Date {
    let col = try self.decoder.getCol(key.stringValue)
    return try self.decoder.decodeDate(from: col, keyDescription: key.stringValue)
  }

  func decode(_ type: TimestampNanos.Type, forKey key: Key) throws -> TimestampNanos {
    let col = try self.decoder.getCol(key.stringValue)
    return try self.decoder.decodeTimestampNanos(from: col, keyDescription: key.stringValue)
  }

  func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
    if type == Int.self {
      return try decode(Int.self, forKey: key) as! T
    } else if type == UInt.self {
      return try decode(UInt.self, forKey: key) as! T
    } else if type == Date.self {
      return try decode(Date.self, forKey: key) as! T
    } else if type == TimestampNanos.self {
      return try decode(TimestampNanos.self, forKey: key) as! T
    } else if ArrowArrayBuilders.isValidBuilderType(type) || type == Decimal.self {
      return try self.decoder.doDecode(key)
    } else {
      throw ArrowError.invalid("Type \(type) is currently not supported")
    }
  }

  func nestedContainer<NestedKey>(
    keyedBy type: NestedKey.Type,
    forKey key: Key
  ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
    throw ArrowError.invalid("Nested decoding is currently not supported.")
  }

  func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
    throw ArrowError.invalid("Nested decoding is currently not supported.")
  }

  func superDecoder() throws -> Decoder {
    throw ArrowError.invalid("super decoding is currently not supported.")
  }

  func superDecoder(forKey key: Key) throws -> Decoder {
    throw ArrowError.invalid("super decoding is currently not supported.")
  }
}

private struct ArrowSingleValueDecoding: SingleValueDecodingContainer {
  var codingPath = [CodingKey]()
  let decoder: ArrowDecoder

  init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
    self.decoder = decoder
    self.codingPath = codingPath
  }

  func decodeNil() -> Bool {
    do {
      return try self.decoder.isNull(self.decoder.singleRBCol)
    } catch {
      return false
    }
  }

  func decode(_ type: Bool.Type) throws -> Bool {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: String.Type) throws -> String {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: Double.Type) throws -> Double {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: Float.Type) throws -> Float {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: Int.Type) throws -> Int {
    let col = try self.decoder.getCol(self.decoder.singleRBCol)
    return try self.decoder.decodeInt(from: col, keyDescription: "column \(self.decoder.singleRBCol)")
  }

  func decode(_ type: Int8.Type) throws -> Int8 {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: Int16.Type) throws -> Int16 {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: Int32.Type) throws -> Int32 {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: Int64.Type) throws -> Int64 {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: UInt.Type) throws -> UInt {
    let col = try self.decoder.getCol(self.decoder.singleRBCol)
    return try self.decoder.decodeUInt(from: col, keyDescription: "column \(self.decoder.singleRBCol)")
  }

  func decode(_ type: UInt8.Type) throws -> UInt8 {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: UInt16.Type) throws -> UInt16 {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: UInt32.Type) throws -> UInt32 {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: UInt64.Type) throws -> UInt64 {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: Decimal.Type) throws -> Decimal {
    return try self.decoder.doDecode(self.decoder.singleRBCol)
  }

  func decode(_ type: Date.Type) throws -> Date {
    let col = try self.decoder.getCol(self.decoder.singleRBCol)
    return try self.decoder.decodeDate(from: col, keyDescription: "column \(self.decoder.singleRBCol)")
  }

  func decode(_ type: TimestampNanos.Type) throws -> TimestampNanos {
    let col = try self.decoder.getCol(self.decoder.singleRBCol)
    return try self.decoder.decodeTimestampNanos(
      from: col, keyDescription: "column \(self.decoder.singleRBCol)")
  }

  func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
    if type == Int.self {
      return try decode(Int.self) as! T
    } else if type == UInt.self {
      return try decode(UInt.self) as! T
    } else if type == Date.self {
      return try decode(Date.self) as! T
    } else if type == TimestampNanos.self {
      return try decode(TimestampNanos.self) as! T
    } else if ArrowArrayBuilders.isValidBuilderType(type) || type == Decimal.self {
      return try self.decoder.doDecode(self.decoder.singleRBCol)
    } else {
      throw ArrowError.invalid("Type \(type) is currently not supported")
    }
  }
}
