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

/// A  dictionary in which keys are case insensitive. The input dictionary can be
/// accessed for cases where case-sensitive information is required.
public struct CaseInsensitiveDictionary: Sendable {
  public var originalDictionary: [String: Sendable]
  private var keyLowerCasedDictionary: [String: Sendable] = [:]

  init(_ originalDictionary: [String: Sendable] = [:]) {
    self.originalDictionary = originalDictionary
    for (key, value) in originalDictionary {
      keyLowerCasedDictionary[key.lowercased()] = value
    }
  }

  subscript(key: String) -> Sendable? {
    get {
      return keyLowerCasedDictionary[key.lowercased()]
    }
    set {
      let lowerKey = key.lowercased()
      if let newValue = newValue {
        keyLowerCasedDictionary[lowerKey] = newValue
      } else {
        keyLowerCasedDictionary.removeValue(forKey: lowerKey)
      }
      originalDictionary = originalDictionary.filter { $0.key.lowercased() != lowerKey }
      if let newValue = newValue {
        originalDictionary[key] = newValue
      }
    }
  }

  public func toDictionary() -> [String: Sendable] {
    return originalDictionary
  }

  public func toStringDictionary() -> [String: String] {
    return originalDictionary.mapValues { String(describing: $0) }
  }

  public var count: Int {
    return keyLowerCasedDictionary.count
  }
}
