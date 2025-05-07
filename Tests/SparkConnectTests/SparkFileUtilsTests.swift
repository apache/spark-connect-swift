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

import Foundation
import Testing

@testable import SparkConnect

/// A test suite for `SparkFileUtils`
@Suite(.serialized)
struct SparkFileUtilsTests {
  let fm = FileManager.default

  @Test
  func resolveURI() async throws {
    let fileNameURL = SparkFileUtils.resolveURL("jar1")
    #expect(fileNameURL!.absoluteString == "file://\(fm.currentDirectoryPath)/jar1")

    let homeUrl = SparkFileUtils.resolveURL("~/jar1")
    #expect(homeUrl!.absoluteString == "\(fm.homeDirectoryForCurrentUser.absoluteString)jar1")

    let absolutePath = SparkFileUtils.resolveURL("file:/jar1")
    #expect(absolutePath!.absoluteString == "file:/jar1")

    let hdfsPath = SparkFileUtils.resolveURL("hdfs:/root/spark.jar")
    #expect(hdfsPath!.absoluteString == "hdfs:/root/spark.jar")

    let s3aPath = SparkFileUtils.resolveURL("s3a:/bucket/spark.jar")
    #expect(s3aPath!.absoluteString == "s3a:/bucket/spark.jar")
  }

  @Test
  func directory() async throws {
    // This tests three functions.
    // createTempDir -> createDirectory(root: String, namePrefix: String = "spark")
    // -> createDirectory(at: URL)
    let dir = SparkFileUtils.createTempDir()

    var isDir: ObjCBool = false
    let exists = fm.fileExists(atPath: dir.path(), isDirectory: &isDir)
    #expect(exists && isDir.boolValue)

    #expect(SparkFileUtils.recursiveList(directory: dir).isEmpty)

    let emptyData = Data()
    try emptyData.write(to: URL(string: dir.absoluteString + "/1")!)

    #expect(SparkFileUtils.recursiveList(directory: dir).count == 1)

    try SparkFileUtils.deleteRecursively(dir)

    #expect(!fm.fileExists(atPath: dir.path(), isDirectory: &isDir))
  }
}
