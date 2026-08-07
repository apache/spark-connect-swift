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
}

extension StructType: RandomAccessCollection {
  public var startIndex: Int { fields.startIndex }
  public var endIndex: Int { fields.endIndex }

  public subscript(position: Int) -> StructField {
    fields[position]
  }
}

extension StructType {
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
