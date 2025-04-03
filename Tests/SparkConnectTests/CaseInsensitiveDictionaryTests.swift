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

/// A test suite for `CaseInsensitiveDictionary`
struct CaseInsensitiveDictionaryTests {
  @Test
  func empty() async throws {
    let dict = CaseInsensitiveDictionary([:])
    #expect(dict.count == 0)
  }

  @Test
  func originalDictionary() async throws {
    let dict = CaseInsensitiveDictionary([
      "key1": "value1",
      "KEY1": "VALUE1",
    ])
    #expect(dict.count == 1)
    #expect(dict.originalDictionary.count == 2)
  }

  @Test
  func toDictionary() async throws {
    let dict = CaseInsensitiveDictionary([
      "key1": "value1",
      "KEY1": "VALUE1",
    ])
    #expect(dict.toDictionary().count == 2)
  }

  @Test
  func `subscript`() async throws {
    var dict = CaseInsensitiveDictionary([:])
    #expect(dict.count == 0)

    dict["KEY1"] = "value1"
    #expect(dict.count == 1)
    #expect(dict["key1"] as! String == "value1")
    #expect(dict["KEY1"] as! String == "value1")
    #expect(dict["KeY1"] as! String == "value1")

    dict["key2"] = false
    #expect(dict.count == 2)
    #expect(dict["kEy2"] as! Bool == false)

    dict["key3"] = 2025
    #expect(dict.count == 3)
    #expect(dict["key3"] as! Int == 2025)
  }

  @Test
  func updatedOriginalDictionary() async throws {
    var dict = CaseInsensitiveDictionary([
      "key1": "value1",
      "KEY1": "VALUE1",
    ])
    #expect(dict.count == 1)
    #expect(dict.originalDictionary.count == 2)

    dict["KEY1"] = "Swift"
    #expect(dict["KEY1"] as! String == "Swift")
    #expect(dict.count == 1)
    #expect(dict.originalDictionary.count == 1)
    #expect(dict.toDictionary().count == 1)
  }
}
