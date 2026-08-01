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

/// A utility to convert local data into an `Apache Arrow` IPC stream for `LocalRelation`.
enum ConvertToArrow {
  /// Convert the given rows into an `Apache Arrow` IPC stream according to the Spark schema.
  /// - Parameters:
  ///   - data: An array of rows whose values are ordered like the schema fields.
  ///   - schema: A ``StructType`` describing the field names and types.
  /// - Returns: A serialized `Apache Arrow` IPC stream.
  static func toArrowIPCStream(_ data: [[Sendable?]], _ schema: StructType) throws -> Data {
    for row in data where row.count != schema.fields.count {
      throw SparkConnectError.InvalidArgument
    }
    let batchBuilder = RecordBatch.Builder()
    for (i, field) in schema.fields.enumerated() {
      let column = data.map { $0[i] }
      let holder = try toArrowColumn(column, field.dataType)
      batchBuilder.addColumn(
        ArrowField(field.name, type: holder.type, isNullable: field.nullable), arrowArray: holder)
    }
    switch batchBuilder.finish() {
    case .success(let batch):
      switch ArrowWriter().writeStreaming(
        ArrowWriter.Info(.recordbatch, schema: batch.schema, batches: [batch]))
      {
      case .success(let ipcStream):
        return ipcStream
      case .failure:
        throw SparkConnectError.InvalidArrowData
      }
    case .failure:
      throw SparkConnectError.InvalidArrowData
    }
  }

  /// Build an Arrow column from the given values according to the Spark data type.
  private static func toArrowColumn(_ column: [Sendable?], _ dataType: DataType) throws
    -> ArrowArrayHolder
  {
    switch dataType.kind {
    case .boolean:
      return try fill(ArrowArrayBuilders.loadBoolArrayBuilder(), column) { $0 as? Bool }
    case .byte:
      return try fill(ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Int8>,
        column) { toInt64($0).flatMap { Int8(exactly: $0) } }
    case .short:
      return try fill(ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Int16>,
        column) { toInt64($0).flatMap { Int16(exactly: $0) } }
    case .integer:
      return try fill(ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Int32>,
        column) { toInt64($0).flatMap { Int32(exactly: $0) } }
    case .long:
      return try fill(ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Int64>,
        column) { toInt64($0) }
    case .float:
      return try fill(ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Float>,
        column) { $0 as? Float }
    case .double:
      return try fill(ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Double>,
        column) { ($0 as? Double) ?? ($0 as? Float).map(Double.init) }
    case .string:
      return try fill(ArrowArrayBuilders.loadStringArrayBuilder(), column) { $0 as? String }
    case .binary:
      return try fill(ArrowArrayBuilders.loadBinaryArrayBuilder(), column) { $0 as? Data }
    case .date:
      return try fill(ArrowArrayBuilders.loadDate32ArrayBuilder(), column) { $0 as? Date }
    case .timestamp:
      return try fill(
        ArrowArrayBuilders.loadTimestampArrayBuilder(.microseconds, timezone: "UTC"), column
      ) { ($0 as? Date).map { Int64(($0.timeIntervalSince1970 * 1_000_000).rounded()) } }
    default:
      throw SparkConnectError.InvalidType
    }
  }

  /// Append the given values to the builder while mapping `nil` to `null`.
  /// Unlike `appendAny`, a value which is not convertible throws ``SparkConnectError/InvalidType``
  /// instead of being appended as `null` silently.
  private static func fill<T, U>(
    _ builder: ArrowArrayBuilder<T, U>, _ column: [Sendable?],
    _ convert: (Sendable) -> T.ItemType?
  ) throws -> ArrowArrayHolder {
    for value in column {
      if let value {
        guard let converted = convert(value) else {
          throw SparkConnectError.InvalidType
        }
        builder.append(converted)
      } else {
        builder.append(nil)
      }
    }
    return try builder.toHolder()
  }

  private static func toInt64(_ value: Sendable) -> Int64? {
    switch value {
    case let v as Int: return Int64(v)
    case let v as Int8: return Int64(v)
    case let v as Int16: return Int64(v)
    case let v as Int32: return Int64(v)
    case let v as Int64: return v
    default: return nil
    }
  }
}
