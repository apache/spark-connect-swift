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
import FlatBuffers
import Testing

@testable import SparkConnect

/// A test suite for `RecordBatch` which does not require a Spark Connect server
@Suite(.serialized)
struct RecordBatchTests {

  @Test
  func withoutColumns() throws {
    let schema = ArrowSchema.Builder().finish()
    #expect(RecordBatch(schema, columns: []).length == 0)
    #expect(RecordBatch(schema, columns: [], length: 3).length == 3)
    #expect(try RecordBatch.Builder().finish().get().length == 0)
  }

  @Test
  func readWithoutColumns() throws {
    // Like the result of `df.select()`, a record batch without columns still has rows.
    let schema = ArrowSchema.Builder().finish()
    let writer = ArrowWriter()
    let schemaMessage = try writer.toMessage(schema).get()
    let batchMessage = try writer.toMessage(RecordBatch(schema, columns: [], length: 3)).get()

    let reader = ArrowReader()
    let result = ArrowReader.makeArrowReaderResult()
    try reader.fromMessage(schemaMessage, dataBody: Data(), result: result).get()
    try reader.fromMessage(batchMessage[0], dataBody: batchMessage[1], result: result).get()
    #expect(result.batches.count == 1)
    #expect(result.batches[0].columnCount == 0)
    #expect(result.batches[0].length == 3)
  }

  @Test
  func readCompressed() throws {
    // Spark 4.1+ compresses the buffers if `spark.sql.execution.arrow.compression.codec` is set.
    let schemaMessage = try ArrowWriter().toMessage(ArrowSchema.Builder().finish()).get()
    for codec in [org_apache_arrow_flatbuf_CompressionType.lz4Frame, .zstd] {
      var fbb = FlatBufferBuilder()
      let compression = org_apache_arrow_flatbuf_BodyCompression.createBodyCompression(
        &fbb, codec: codec)
      let recordBatch = org_apache_arrow_flatbuf_RecordBatch.createRecordBatch(
        &fbb, length: 3, compressionOffset: compression)
      let message = org_apache_arrow_flatbuf_Message.createMessage(
        &fbb, version: .max, headerType: .recordbatch, headerOffset: recordBatch)
      fbb.finish(offset: message)

      let reader = ArrowReader()
      let result = ArrowReader.makeArrowReaderResult()
      try reader.fromMessage(schemaMessage, dataBody: Data(), result: result).get()
      #expect(throws: ArrowError.self) {
        try reader.fromMessage(fbb.data, dataBody: Data(), result: result).get()
      }
      #expect(result.batches.isEmpty)
    }
  }
}
