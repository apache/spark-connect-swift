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
import GRPCCore
import GRPCNIOTransportHTTP2

/// Extension providing artifact operations on ``SparkConnectClient``.
extension SparkConnectClient {
  /// The chunk size used when splitting an artifact into multiple `AddArtifactsRequest` messages,
  /// following the midpoint recommendation of 32KiB for gRPC payloads.
  static let artifactChunkSize = 32 * 1024

  func addArtifact(_ url: URL) async throws {
    guard url.lastPathComponent.hasSuffix(".jar") else {
      throw SparkConnectError.InvalidArgument
    }

    let JAR_PREFIX = "jars"
    let name = "\(JAR_PREFIX)/" + url.lastPathComponent

    try await withGPRC { client in
      let service = SparkConnectService.Client(wrapping: client)

      var chunk = Spark_Connect_AddArtifactsRequest.ArtifactChunk()
      chunk.data = try Data(contentsOf: url)
      chunk.crc = Int64(CRC32.checksum(data: chunk.data))

      var singleChunk = Spark_Connect_AddArtifactsRequest.SingleChunkArtifact()
      singleChunk.name = name
      singleChunk.data = chunk
      var batch = Spark_Connect_AddArtifactsRequest.Batch()
      batch.artifacts.append(singleChunk)

      var addArtifactsRequest = Spark_Connect_AddArtifactsRequest()
      addArtifactsRequest.sessionID = self.sessionID!
      addArtifactsRequest.userContext = self.userContext
      addArtifactsRequest.clientType = self.clientType
      addArtifactsRequest.batch = batch
      let request = addArtifactsRequest
      _ = try await service.addArtifacts(
        request: StreamingClientRequest<Spark_Connect_AddArtifactsRequest> { x in
          try await x.write(contentsOf: [request])
        })
    }
  }

  /// Check whether an artifact with the given name exists in the server-side session.
  /// - Parameter name: An artifact name, e.g. `cache/abc123`.
  /// - Returns: True if the artifact exists.
  func artifactExists(_ name: String) async throws -> Bool {
    try await withGPRC { client in
      let service = SparkConnectService.Client(wrapping: client)
      var request = Spark_Connect_ArtifactStatusesRequest()
      request.sessionID = self.sessionID!
      request.userContext = self.userContext
      request.clientType = self.clientType
      request.names = [name]
      let response = try await service.artifactStatus(request)
      return response.statuses[name]?.exists ?? false
    }
  }

  /// Cache the given data as a `cache/` artifact in the server-side session and return its
  /// SHA-256 hash. The upload is skipped if an artifact with the same hash already exists.
  /// Data larger than ``artifactChunkSize`` is uploaded as a chunked artifact stream.
  /// - Parameter data: The data to cache.
  /// - Returns: A SHA-256 hash of the data as a lowercase hex string.
  func cacheArtifact(_ data: Data) async throws -> String {
    let hash = SHA256.hexString(data: data)
    let name = "cache/\(hash)"
    if try await artifactExists(name) {
      return hash
    }

    try await withGPRC { client in
      let service = SparkConnectService.Client(wrapping: client)
      let chunkSize = Self.artifactChunkSize
      var requests: [Spark_Connect_AddArtifactsRequest] = []
      if data.count <= chunkSize {
        var chunk = Spark_Connect_AddArtifactsRequest.ArtifactChunk()
        chunk.data = data
        chunk.crc = Int64(CRC32.checksum(data: chunk.data))

        var singleChunk = Spark_Connect_AddArtifactsRequest.SingleChunkArtifact()
        singleChunk.name = name
        singleChunk.data = chunk
        var batch = Spark_Connect_AddArtifactsRequest.Batch()
        batch.artifacts.append(singleChunk)

        var request = Spark_Connect_AddArtifactsRequest()
        request.sessionID = self.sessionID!
        request.userContext = self.userContext
        request.clientType = self.clientType
        request.batch = batch
        requests.append(request)
      } else {
        let numChunks = (data.count + chunkSize - 1) / chunkSize
        for i in 0..<numChunks {
          let range = (i * chunkSize)..<min((i + 1) * chunkSize, data.count)
          var chunk = Spark_Connect_AddArtifactsRequest.ArtifactChunk()
          chunk.data = data.subdata(in: range)
          chunk.crc = Int64(CRC32.checksum(data: chunk.data))

          var request = Spark_Connect_AddArtifactsRequest()
          request.sessionID = self.sessionID!
          request.userContext = self.userContext
          request.clientType = self.clientType
          if i == 0 {
            var beginChunk = Spark_Connect_AddArtifactsRequest.BeginChunkedArtifact()
            beginChunk.name = name
            beginChunk.totalBytes = Int64(data.count)
            beginChunk.numChunks = Int64(numChunks)
            beginChunk.initialChunk = chunk
            request.beginChunk = beginChunk
          } else {
            request.chunk = chunk
          }
          requests.append(request)
        }
      }
      let allRequests = requests
      _ = try await service.addArtifacts(
        request: StreamingClientRequest<Spark_Connect_AddArtifactsRequest> { x in
          try await x.write(contentsOf: allRequests)
        })
    }
    return hash
  }
}
