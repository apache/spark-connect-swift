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
import SparkConnect
import Testing

struct SHA256Tests {
  @Test
  func testEmptyData() async throws {
    #expect(
      SHA256.hexString(data: Data())
        == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
  }

  @Test
  func testShortData() async throws {
    #expect(
      SHA256.hexString(data: "abc".data(using: .ascii)!)
        == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
  }

  @Test
  func testTwoBlockData() async throws {
    let str = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
    #expect(
      SHA256.hexString(data: str.data(using: .ascii)!)
        == "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1")
  }

  @Test
  func testLongData() async throws {
    let data = Data(repeating: UInt8(ascii: "a"), count: 1_000_000)
    #expect(
      SHA256.hexString(data: data)
        == "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0")
  }

  @Test
  func testDigest() async throws {
    let digest = SHA256.digest(data: "abc".data(using: .ascii)!)
    #expect(digest.count == 32)
    #expect(digest[0] == 0xba)
    #expect(digest[31] == 0xad)
  }
}
