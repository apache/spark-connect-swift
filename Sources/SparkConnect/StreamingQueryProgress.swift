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

/// A JSON value used to preserve arbitrary JSON fragments (e.g. source offsets) as strings.
private enum JSONValue: Decodable, Sendable {
  case null
  case bool(Bool)
  case int(Int64)
  case double(Double)
  case string(String)
  case array([JSONValue])
  case object([String: JSONValue])

  init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    if container.decodeNil() {
      self = .null
    } else if let value = try? container.decode(Bool.self) {
      self = .bool(value)
    } else if let value = try? container.decode(Int64.self) {
      self = .int(value)
    } else if let value = try? container.decode(Double.self) {
      self = .double(value)
    } else if let value = try? container.decode(String.self) {
      self = .string(value)
    } else if let value = try? container.decode([JSONValue].self) {
      self = .array(value)
    } else {
      self = .object(try container.decode([String: JSONValue].self))
    }
  }

  /// A compact JSON text of this value with alphabetically sorted object keys.
  var compactString: String {
    switch self {
    case .null: return "null"
    case .bool(let value): return value ? "true" : "false"
    case .int(let value): return String(value)
    case .double(let value): return String(value)
    case .string(let value): return JSONValue.escape(value)
    case .array(let values):
      return "[" + values.map { $0.compactString }.joined(separator: ",") + "]"
    case .object(let values):
      let members = values.keys.sorted().map { "\(JSONValue.escape($0)):\(values[$0]!.compactString)" }
      return "{" + members.joined(separator: ",") + "}"
    }
  }

  /// A string form following the Spark clients' offset handling; a JSON string is returned as-is
  /// and any other JSON value is converted to its compact JSON text.
  var offsetString: String {
    if case .string(let value) = self {
      return value
    }
    return compactString
  }

  private static func escape(_ string: String) -> String {
    var result = "\""
    for scalar in string.unicodeScalars {
      switch scalar {
      case "\"": result += "\\\""
      case "\\": result += "\\\\"
      case "\n": result += "\\n"
      case "\r": result += "\\r"
      case "\t": result += "\\t"
      default:
        if scalar.value < 0x20 {
          let hex = String(scalar.value, radix: 16)
          result += "\\u" + String(repeating: "0", count: 4 - hex.count) + hex
        } else {
          result.unicodeScalars.append(scalar)
        }
      }
    }
    return result + "\""
  }
}

/// Decodes a `Double` that servers may emit as a quoted string like `"NaN"` or omit entirely.
private func decodeLenientDouble<Key: CodingKey>(
  _ container: KeyedDecodingContainer<Key>, _ key: Key
) -> Double {
  if let value = try? container.decode(Double.self, forKey: key) {
    return value
  }
  if let value = try? container.decode(String.self, forKey: key), let double = Double(value) {
    return double
  }
  return .nan
}

/// Information about progress made for a source in the execution of a ``StreamingQuery``
/// during a trigger. See ``StreamingQueryProgress`` for more information.
public struct SourceProgress: Sendable, Codable {
  /// Description of the source.
  public let description: String
  /// The starting offset for data being read, as a JSON string.
  public let startOffset: String
  /// The ending offset for data being read, as a JSON string.
  public let endOffset: String
  /// The latest offset from this source, as a JSON string.
  public let latestOffset: String
  /// The number of records read from this source.
  public let numInputRows: Int64
  /// The rate at which data is arriving from this source, `nan` if not available.
  public let inputRowsPerSecond: Double
  /// The rate at which data from this source is being processed by Spark, `nan` if not available.
  public let processedRowsPerSecond: Double
  /// Custom metrics of this source.
  public let metrics: [String: String]

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.description = try container.decodeIfPresent(String.self, forKey: .description) ?? ""
    self.startOffset =
      container.contains(.startOffset)
      ? try container.decode(JSONValue.self, forKey: .startOffset).offsetString : ""
    self.endOffset =
      container.contains(.endOffset)
      ? try container.decode(JSONValue.self, forKey: .endOffset).offsetString : ""
    self.latestOffset =
      container.contains(.latestOffset)
      ? try container.decode(JSONValue.self, forKey: .latestOffset).offsetString : ""
    self.numInputRows = try container.decodeIfPresent(Int64.self, forKey: .numInputRows) ?? 0
    self.inputRowsPerSecond = decodeLenientDouble(container, .inputRowsPerSecond)
    self.processedRowsPerSecond = decodeLenientDouble(container, .processedRowsPerSecond)
    self.metrics = try container.decodeIfPresent([String: String].self, forKey: .metrics) ?? [:]
  }
}

/// Information about progress made for a sink in the execution of a ``StreamingQuery``
/// during a trigger. See ``StreamingQueryProgress`` for more information.
public struct SinkProgress: Sendable, Codable {
  /// Description of the sink.
  public let description: String
  /// Number of rows written to the sink, `-1` if not available.
  public let numOutputRows: Int64
  /// Custom metrics of this sink.
  public let metrics: [String: String]

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.description = try container.decodeIfPresent(String.self, forKey: .description) ?? ""
    self.numOutputRows = try container.decodeIfPresent(Int64.self, forKey: .numOutputRows) ?? -1
    self.metrics = try container.decodeIfPresent([String: String].self, forKey: .metrics) ?? [:]
  }
}

/// Information about updates made to stateful operators in a ``StreamingQuery``
/// during a trigger. See ``StreamingQueryProgress`` for more information.
public struct StateOperatorProgress: Sendable, Codable {
  public let operatorName: String
  public let numRowsTotal: Int64
  public let numRowsUpdated: Int64
  public let allUpdatesTimeMs: Int64
  public let numRowsRemoved: Int64
  public let allRemovalsTimeMs: Int64
  public let commitTimeMs: Int64
  public let memoryUsedBytes: Int64
  public let numRowsDroppedByWatermark: Int64
  public let numShufflePartitions: Int64
  public let numStateStoreInstances: Int64
  public let customMetrics: [String: Int64]

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.operatorName = try container.decodeIfPresent(String.self, forKey: .operatorName) ?? ""
    self.numRowsTotal = try container.decodeIfPresent(Int64.self, forKey: .numRowsTotal) ?? 0
    self.numRowsUpdated = try container.decodeIfPresent(Int64.self, forKey: .numRowsUpdated) ?? 0
    self.allUpdatesTimeMs =
      try container.decodeIfPresent(Int64.self, forKey: .allUpdatesTimeMs) ?? 0
    self.numRowsRemoved = try container.decodeIfPresent(Int64.self, forKey: .numRowsRemoved) ?? 0
    self.allRemovalsTimeMs =
      try container.decodeIfPresent(Int64.self, forKey: .allRemovalsTimeMs) ?? 0
    self.commitTimeMs = try container.decodeIfPresent(Int64.self, forKey: .commitTimeMs) ?? 0
    self.memoryUsedBytes = try container.decodeIfPresent(Int64.self, forKey: .memoryUsedBytes) ?? 0
    self.numRowsDroppedByWatermark =
      try container.decodeIfPresent(Int64.self, forKey: .numRowsDroppedByWatermark) ?? 0
    self.numShufflePartitions =
      try container.decodeIfPresent(Int64.self, forKey: .numShufflePartitions) ?? 0
    self.numStateStoreInstances =
      try container.decodeIfPresent(Int64.self, forKey: .numStateStoreInstances) ?? 0
    self.customMetrics =
      try container.decodeIfPresent([String: Int64].self, forKey: .customMetrics) ?? [:]
  }
}

/// Information about progress made in the execution of a ``StreamingQuery``
/// during a trigger. Each event relates to processing done for a single trigger of
/// the streaming query. Events are emitted even when no new data is available to be processed.
public struct StreamingQueryProgress: Sendable, Codable {
  /// A unique query id that persists across restarts. See ``StreamingQuery/id``.
  public let id: UUID
  /// A query id that is unique for every start/restart. See ``StreamingQuery/runId``.
  public let runId: UUID
  /// User-specified name of the query, `nil` if not specified.
  public let name: String?
  /// Beginning time of the trigger in ISO8601 format, i.e. UTC timestamps.
  public let timestamp: String
  /// A unique id for the current batch of data being processed.
  public let batchId: Int64
  /// The process duration of each batch.
  public let batchDuration: Int64
  /// The amount of time taken to perform various operations in milliseconds.
  public let durationMs: [String: Int64]
  /// Statistics of event time seen in this batch, e.g. `max`/`min`/`avg`/`watermark`.
  public let eventTime: [String: String]
  /// Information about operators in the query that store state.
  public let stateOperators: [StateOperatorProgress]
  /// Detailed statistics on data being read from each of the streaming sources.
  public let sources: [SourceProgress]
  /// Information about progress made for the sink.
  public let sink: SinkProgress
  /// Observed metrics of this query, keyed by the observation name with the metric row
  /// preserved as a JSON string.
  public let observedMetrics: [String: String]
  /// The original JSON representation of this progress received from the server.
  public internal(set) var json: String = ""

  /// The aggregate (across all sources) number of records processed in a trigger.
  public var numInputRows: Int64 {
    return sources.map { $0.numInputRows }.reduce(0, +)
  }

  /// The aggregate (across all sources) rate of data arriving.
  public var inputRowsPerSecond: Double {
    return sources.map { $0.inputRowsPerSecond }.reduce(0, +)
  }

  /// The aggregate (across all sources) rate at which Spark is processing data.
  public var processedRowsPerSecond: Double {
    return sources.map { $0.processedRowsPerSecond }.reduce(0, +)
  }

  enum CodingKeys: String, CodingKey {
    case id, runId, name, timestamp, batchId, batchDuration, durationMs, eventTime
    case stateOperators, sources, sink, observedMetrics
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.id = try container.decode(UUID.self, forKey: .id)
    self.runId = try container.decode(UUID.self, forKey: .runId)
    self.name = try container.decodeIfPresent(String.self, forKey: .name)
    self.timestamp = try container.decode(String.self, forKey: .timestamp)
    self.batchId = try container.decode(Int64.self, forKey: .batchId)
    self.batchDuration = try container.decodeIfPresent(Int64.self, forKey: .batchDuration) ?? 0
    self.durationMs =
      try container.decodeIfPresent([String: Int64].self, forKey: .durationMs) ?? [:]
    self.eventTime =
      try container.decodeIfPresent([String: String].self, forKey: .eventTime) ?? [:]
    self.stateOperators =
      try container.decodeIfPresent([StateOperatorProgress].self, forKey: .stateOperators) ?? []
    self.sources = try container.decodeIfPresent([SourceProgress].self, forKey: .sources) ?? []
    self.sink = try container.decode(SinkProgress.self, forKey: .sink)
    self.observedMetrics =
      try container.decodeIfPresent([String: JSONValue].self, forKey: .observedMetrics)?
      .mapValues { $0.compactString } ?? [:]
  }

  /// Creates a ``StreamingQueryProgress`` from the given JSON string.
  /// Unknown fields are ignored and most of the missing fields fall back to default values.
  /// - Parameter json: A JSON string emitted by a Spark Connect server.
  /// - Returns: A ``StreamingQueryProgress``.
  public static func fromJson(_ json: String) throws -> StreamingQueryProgress {
    var progress = try JSONDecoder().decode(StreamingQueryProgress.self, from: Data(json.utf8))
    progress.json = json
    return progress
  }
}
