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
import Testing

@testable import SparkConnect

/// A test suite for ``StreamingQueryProgress`` JSON decoding
struct StreamingQueryProgressTests {

  @Test
  func fromJson() throws {
    let json = """
      {
        "id": "33bd4ba3-79b2-4f0e-8d1c-2ccb44e29d4e",
        "runId": "68ea3dd5-8b8e-4290-8d78-4b0a3f0e4a52",
        "name": "myQuery",
        "timestamp": "2026-08-07T00:00:00.000Z",
        "batchId": 2,
        "batchDuration": 1000,
        "durationMs": {"addBatch": 100, "triggerExecution": 245},
        "eventTime": {"watermark": "2026-08-07T00:00:00.000Z"},
        "stateOperators": [{
          "operatorName": "stateStoreSave",
          "numRowsTotal": 3,
          "numRowsUpdated": 1,
          "allUpdatesTimeMs": 10,
          "numRowsRemoved": 0,
          "allRemovalsTimeMs": 0,
          "commitTimeMs": 5,
          "memoryUsedBytes": 100,
          "numRowsDroppedByWatermark": 0,
          "numShufflePartitions": 5,
          "numStateStoreInstances": 5,
          "customMetrics": {"loadedMapCacheHitCount": 2}
        }],
        "sources": [{
          "description": "FileStreamSource[file:/tmp/input]",
          "startOffset": "0",
          "endOffset": "1",
          "latestOffset": "1",
          "numInputRows": 10,
          "inputRowsPerSecond": 12.5,
          "processedRowsPerSecond": 25.0,
          "metrics": {"avgOffsetsBehindLatest": "0.0"}
        }],
        "sink": {"description": "FileSink[file:/tmp/output]", "numOutputRows": 10},
        "observedMetrics": {"event": {"c1": 1, "c2": 2}}
      }
      """
    let progress = try StreamingQueryProgress.fromJson(json)
    #expect(progress.id == UUID(uuidString: "33bd4ba3-79b2-4f0e-8d1c-2ccb44e29d4e"))
    #expect(progress.runId == UUID(uuidString: "68ea3dd5-8b8e-4290-8d78-4b0a3f0e4a52"))
    #expect(progress.name == "myQuery")
    #expect(progress.timestamp == "2026-08-07T00:00:00.000Z")
    #expect(progress.batchId == 2)
    #expect(progress.batchDuration == 1000)
    #expect(progress.durationMs == ["addBatch": 100, "triggerExecution": 245])
    #expect(progress.eventTime == ["watermark": "2026-08-07T00:00:00.000Z"])
    #expect(progress.stateOperators.count == 1)
    #expect(progress.stateOperators[0].operatorName == "stateStoreSave")
    #expect(progress.stateOperators[0].numRowsTotal == 3)
    #expect(progress.stateOperators[0].customMetrics == ["loadedMapCacheHitCount": 2])
    #expect(progress.sources.count == 1)
    #expect(progress.sources[0].description == "FileStreamSource[file:/tmp/input]")
    #expect(progress.sources[0].startOffset == "0")
    #expect(progress.sources[0].endOffset == "1")
    #expect(progress.sources[0].numInputRows == 10)
    #expect(progress.sources[0].inputRowsPerSecond == 12.5)
    #expect(progress.sources[0].metrics == ["avgOffsetsBehindLatest": "0.0"])
    #expect(progress.sink.description == "FileSink[file:/tmp/output]")
    #expect(progress.sink.numOutputRows == 10)
    #expect(progress.observedMetrics == ["event": "{\"c1\":1,\"c2\":2}"])
    #expect(progress.numInputRows == 10)
    #expect(progress.inputRowsPerSecond == 12.5)
    #expect(progress.processedRowsPerSecond == 25.0)
    #expect(progress.json == json)
  }

  @Test
  func fromJsonWithMissingFields() throws {
    let json = """
      {
        "id": "33bd4ba3-79b2-4f0e-8d1c-2ccb44e29d4e",
        "runId": "68ea3dd5-8b8e-4290-8d78-4b0a3f0e4a52",
        "name": null,
        "timestamp": "2026-08-07T00:00:00.000Z",
        "batchId": 0,
        "unknownField": {"ignored": true},
        "sink": {"description": "ForeachBatchSink"}
      }
      """
    let progress = try StreamingQueryProgress.fromJson(json)
    #expect(progress.name == nil)
    #expect(progress.batchDuration == 0)
    #expect(progress.durationMs.isEmpty)
    #expect(progress.eventTime.isEmpty)
    #expect(progress.stateOperators.isEmpty)
    #expect(progress.sources.isEmpty)
    #expect(progress.sink.description == "ForeachBatchSink")
    #expect(progress.sink.numOutputRows == -1)
    #expect(progress.sink.metrics.isEmpty)
    #expect(progress.observedMetrics.isEmpty)
    #expect(progress.numInputRows == 0)
  }

  @Test
  func fromJsonWithObjectOffsetsAndNaN() throws {
    let json = """
      {
        "id": "33bd4ba3-79b2-4f0e-8d1c-2ccb44e29d4e",
        "runId": "68ea3dd5-8b8e-4290-8d78-4b0a3f0e4a52",
        "timestamp": "2026-08-07T00:00:00.000Z",
        "batchId": 0,
        "sources": [{
          "description": "KafkaV2[Subscribe[topic-0]]",
          "startOffset": {"topic-0": {"0": 1}},
          "endOffset": {"topic-0": {"0": 2}},
          "latestOffset": null,
          "numInputRows": 1,
          "inputRowsPerSecond": "NaN"
        }],
        "sink": {"description": "MemorySink"}
      }
      """
    let progress = try StreamingQueryProgress.fromJson(json)
    #expect(progress.sources[0].startOffset == "{\"topic-0\":{\"0\":1}}")
    #expect(progress.sources[0].endOffset == "{\"topic-0\":{\"0\":2}}")
    #expect(progress.sources[0].latestOffset == "null")
    #expect(progress.sources[0].inputRowsPerSecond.isNaN)
    #expect(progress.sources[0].processedRowsPerSecond.isNaN)
    #expect(progress.inputRowsPerSecond.isNaN)
  }

  @Test
  func fromJsonWithInvalidJson() throws {
    #expect(throws: Error.self) {
      try StreamingQueryProgress.fromJson("invalid")
    }
  }
}
