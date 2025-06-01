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
import SparkConnect
import Testing

struct CRC32Tests {
  @Test
  func testChecksumWithEmptyData() async throws {
    #expect(CRC32.checksum(data: Data()) == 0)
    #expect(CRC32.checksum(string: "") == 0)
    #expect(CRC32.checksum(bytes: []) == 0)
  }

  @Test
  func testChecksum() async throws {
    let str = "Apache Spark Connect Client for Swift"
    #expect(CRC32.checksum(string: str, encoding: .ascii) == 2_736_908_745)
    #expect(CRC32.checksum(data: str.data(using: .ascii)!) == 2_736_908_745)
    #expect(CRC32.checksum(bytes: [UInt8](str.data(using: .ascii)!)) == 2_736_908_745)
  }

  @Test
  func testLongChecksum() async throws {
    let str = String(repeating: "Apache Spark Connect Client for Swift", count: 1000)
    #expect(CRC32.checksum(string: str, encoding: .ascii) == 1_985_943_888)
    #expect(CRC32.checksum(data: str.data(using: .ascii)!) == 1_985_943_888)
    #expect(CRC32.checksum(bytes: [UInt8](str.data(using: .ascii)!)) == 1_985_943_888)
  }
}
