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

/// A field of Spark's `YEAR-MONTH INTERVAL` type.
public enum YearMonthIntervalField: Int32, Sendable, Equatable {
  case year = 0
  case month = 1

  var fieldName: String {
    switch self {
    case .year: "year"
    case .month: "month"
    }
  }
}

/// A field of Spark's `DAY-TIME INTERVAL` type.
public enum DayTimeIntervalField: Int32, Sendable, Equatable {
  case day = 0
  case hour = 1
  case minute = 2
  case second = 3

  var fieldName: String {
    switch self {
    case .day: "day"
    case .hour: "hour"
    case .minute: "minute"
    case .second: "second"
    }
  }
}

/// A user-defined type (UDT) definition delivered by the `Spark Connect` server.
public struct UserDefinedType: Sendable, Equatable {
  /// The JVM class implementing this UDT, if any.
  public var jvmClass: String?
  /// The Python class implementing this UDT, if any.
  public var pythonClass: String?
  /// The serialized Python class of this UDT, if any.
  public var serializedPythonClass: String?
  /// The underlying SQL type of this UDT, if any.
  public var sqlType: DataType?

  public init(
    jvmClass: String? = nil, pythonClass: String? = nil, serializedPythonClass: String? = nil,
    sqlType: DataType? = nil
  ) {
    self.jvmClass = jvmClass
    self.pythonClass = pythonClass
    self.serializedPythonClass = serializedPythonClass
    self.sqlType = sqlType
  }
}

/// The data type of a ``DataFrame`` column or of a ``StructField``, mirroring Spark SQL's
/// `org.apache.spark.sql.types.DataType` hierarchy as a Swift enum.
public indirect enum DataType: Sendable, Equatable {
  case null
  case binary
  case boolean
  case byte
  case short
  case integer
  case long
  case float
  case double
  case decimal(precision: Int32, scale: Int32)
  case string
  case char(length: Int32)
  case varchar(length: Int32)
  case date
  case timestamp
  case timestampNtz
  case time(precision: Int32)
  case calendarInterval
  case yearMonthInterval(startField: YearMonthIntervalField, endField: YearMonthIntervalField)
  case dayTimeInterval(startField: DayTimeIntervalField, endField: DayTimeIntervalField)
  case array(elementType: DataType, containsNull: Bool)
  case map(keyType: DataType, valueType: DataType, valueContainsNull: Bool)
  case `struct`(StructType)
  case variant
  case geometry(srid: Int32)
  case geography(srid: Int32)
  case udt(UserDefinedType)
  case unparsed(String)

  /// A compact string representation like Spark SQL's `DataType.simpleString`,
  /// e.g. `int`, `decimal(10,2)`, or `array<string>`.
  public var simpleString: String {
    switch self {
    case .null:
      "void"
    case .binary:
      "binary"
    case .boolean:
      "boolean"
    case .byte:
      "tinyint"
    case .short:
      "smallint"
    case .integer:
      "int"
    case .long:
      "bigint"
    case .float:
      "float"
    case .double:
      "double"
    case .decimal(let precision, let scale):
      "decimal(\(precision),\(scale))"
    case .string:
      "string"
    case .char(let length):
      "char(\(length))"
    case .varchar(let length):
      "varchar(\(length))"
    case .date:
      "date"
    case .timestamp:
      "timestamp"
    case .timestampNtz:
      "timestamp_ntz"
    case .time(let precision):
      "time(\(precision))"
    case .calendarInterval:
      "interval"
    case .yearMonthInterval(let startField, let endField):
      if startField == endField {
        "interval \(startField.fieldName)"
      } else {
        "interval \(startField.fieldName) to \(endField.fieldName)"
      }
    case .dayTimeInterval(let startField, let endField):
      if startField == endField {
        "interval \(startField.fieldName)"
      } else {
        "interval \(startField.fieldName) to \(endField.fieldName)"
      }
    case .array(let elementType, _):
      "array<\(elementType.simpleString)>"
    case .map(let keyType, let valueType, _):
      "map<\(keyType.simpleString),\(valueType.simpleString)>"
    case .struct(let structType):
      structType.simpleString
    case .variant:
      "variant"
    case .geometry(let srid):
      if srid == -1 {
        "geometry(any)"
      } else {
        "geometry(\(srid))"
      }
    case .geography(let srid):
      if srid == -1 {
        "geography(any)"
      } else {
        "geography(\(srid))"
      }
    case .udt:
      "udt"
    case .unparsed(let dataTypeString):
      dataTypeString
    }
  }
}

extension DataType {
  /// Create a public ``DataType`` from the `Spark Connect` protobuf representation.
  init(_ proto: ProtoDataType) throws {
    switch proto.kind {
    case .null:
      self = .null
    case .binary:
      self = .binary
    case .boolean:
      self = .boolean
    case .byte:
      self = .byte
    case .short:
      self = .short
    case .integer:
      self = .integer
    case .long:
      self = .long
    case .float:
      self = .float
    case .double:
      self = .double
    case .decimal(let decimal):
      self = .decimal(
        precision: decimal.hasPrecision ? decimal.precision : 10,
        scale: decimal.hasScale ? decimal.scale : 0)
    case .string:
      self = .string
    case .char(let char):
      self = .char(length: char.length)
    case .varChar(let varChar):
      self = .varchar(length: varChar.length)
    case .date:
      self = .date
    case .timestamp:
      self = .timestamp
    case .timestampNtz:
      self = .timestampNtz
    case .time(let time):
      self = .time(precision: time.hasPrecision ? time.precision : 6)
    case .calendarInterval:
      self = .calendarInterval
    case .yearMonthInterval(let interval):
      guard let startField = YearMonthIntervalField(rawValue: interval.startField),
        let endField = YearMonthIntervalField(rawValue: interval.endField)
      else {
        throw SparkConnectError.InvalidType
      }
      self = .yearMonthInterval(startField: startField, endField: endField)
    case .dayTimeInterval(let interval):
      guard let startField = DayTimeIntervalField(rawValue: interval.startField),
        let endField = DayTimeIntervalField(rawValue: interval.endField)
      else {
        throw SparkConnectError.InvalidType
      }
      self = .dayTimeInterval(startField: startField, endField: endField)
    case .array(let array):
      self = .array(
        elementType: try DataType(array.elementType), containsNull: array.containsNull)
    case .map(let map):
      self = .map(
        keyType: try DataType(map.keyType), valueType: try DataType(map.valueType),
        valueContainsNull: map.valueContainsNull)
    case .struct(let structType):
      self = .struct(try StructType(structType))
    case .variant:
      self = .variant
    case .geometry(let geometry):
      self = .geometry(srid: geometry.srid)
    case .geography(let geography):
      self = .geography(srid: geography.srid)
    case .udt(let udt):
      self = .udt(
        UserDefinedType(
          jvmClass: udt.hasJvmClass ? udt.jvmClass : nil,
          pythonClass: udt.hasPythonClass ? udt.pythonClass : nil,
          serializedPythonClass: udt.hasSerializedPythonClass ? udt.serializedPythonClass : nil,
          sqlType: udt.hasSqlType ? try DataType(udt.sqlType) : nil))
    case .unparsed(let unparsed):
      self = .unparsed(unparsed.dataTypeString)
    case .none:
      throw SparkConnectError.InvalidType
    }
  }
}
