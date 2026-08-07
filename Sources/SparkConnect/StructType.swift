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

/// A field inside a ``StructType`` with a name, a ``DataType``, a nullability flag, and
/// an optional JSON-encoded metadata string.
public struct StructField: Sendable, Equatable {
  public var name: String
  public var dataType: DataType
  public var nullable: Bool
  public var metadata: String?

  public init(name: String, dataType: DataType, nullable: Bool = true, metadata: String? = nil) {
    self.name = name
    self.dataType = dataType
    self.nullable = nullable
    self.metadata = metadata
  }

  /// A string containing this field in DDL format like Spark SQL's `StructField.toDDL`,
  /// e.g. `` `eventId` INT NOT NULL ``.
  public var toDDL: String {
    "\(quoteIfNeeded(name)) \(dataType.sql)\(nullable ? "" : " NOT NULL")"
  }

  /// A string usable inside a struct type string like Spark SQL's `StructField.sql`,
  /// e.g. `` `eventId`: INT NOT NULL ``.
  var sql: String {
    "\(quoteIfNeeded(name)): \(dataType.sql)\(nullable ? "" : " NOT NULL")"
  }
}

/// Quote the given name with backticks like Spark SQL's `QuotingUtils.quoteIfNeeded`
/// unless it is a valid identifier matching `[a-zA-Z_][a-zA-Z0-9_]*`.
private func quoteIfNeeded(_ name: String) -> String {
  let isValidIdentifier =
    !name.isEmpty
    && name.utf8.enumerated().allSatisfy { (index, byte) in
      byte == UInt8(ascii: "_")
        || (UInt8(ascii: "a")...UInt8(ascii: "z")).contains(byte)
        || (UInt8(ascii: "A")...UInt8(ascii: "Z")).contains(byte)
        || (index > 0 && (UInt8(ascii: "0")...UInt8(ascii: "9")).contains(byte))
    }
  return isValidIdentifier ? name : "`\(name.replacing("`", with: "``"))`"
}

/// A struct type holding an ordered collection of ``StructField``s, mirroring Spark SQL's
/// `org.apache.spark.sql.types.StructType`. This is the schema representation of
/// a ``DataFrame`` returned by ``DataFrame/schema``.
public struct StructType: Sendable, Equatable {
  public var fields: [StructField]

  public init(fields: [StructField] = []) {
    self.fields = fields
  }

  /// The names of all fields in order.
  public var fieldNames: [String] {
    fields.map { $0.name }
  }

  /// The first field whose name is equal to the given name, or nil if it doesn't exist.
  public subscript(name: String) -> StructField? {
    fields.first { $0.name == name }
  }

  /// A compact string representation like Spark SQL's `StructType.simpleString`,
  /// e.g. `struct<id:bigint,name:string>`.
  public var simpleString: String {
    "struct<\(fields.map { "\($0.name):\($0.dataType.simpleString)" }.joined(separator: ","))>"
  }

  /// A string containing this schema in DDL format like Spark SQL's `StructType.toDDL`,
  /// e.g. `id BIGINT NOT NULL,name STRING`.
  public var toDDL: String {
    fields.map { $0.toDDL }.joined(separator: ",")
  }

  /// A type string usable in DDL like Spark SQL's `StructType.sql`,
  /// e.g. `STRUCT<id: BIGINT NOT NULL, name: STRING>`.
  var sql: String {
    "STRUCT<\(fields.map { $0.sql }.joined(separator: ", "))>"
  }
}

extension StructType: RandomAccessCollection {
  public var startIndex: Int { fields.startIndex }
  public var endIndex: Int { fields.endIndex }

  public subscript(position: Int) -> StructField {
    fields[position]
  }
}

extension StructType {
  /// Convert to the `Spark Connect` protobuf representation.
  var toProtoStructType: ProtoStructType {
    var proto = ProtoStructType()
    proto.fields = fields.map { field in
      var protoField = ProtoStructField()
      protoField.name = field.name
      protoField.dataType = field.dataType.toProtoDataType
      protoField.nullable = field.nullable
      if let metadata = field.metadata {
        protoField.metadata = metadata
      }
      return protoField
    }
    return proto
  }

  /// Create a public ``StructType`` from the `Spark Connect` protobuf representation.
  init(_ proto: ProtoStructType) throws {
    self.fields = try proto.fields.map {
      StructField(
        name: $0.name,
        dataType: try DataType($0.dataType),
        nullable: $0.nullable,
        metadata: $0.hasMetadata ? $0.metadata : nil)
    }
  }
}
