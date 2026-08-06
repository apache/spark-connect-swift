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

/// A helper class to observe metrics of a ``DataFrame`` while an action is performed on it.
///
/// ```swift
/// let observation = Observation("my_metrics")
/// let observedDf = try await df.observe(observation, count(col("*")), max(col("id")))
/// try await observedDf.count()
/// let metrics = try await observation.get
/// ```
public actor Observation {
  /// The name of the observation.
  public nonisolated let name: String

  private var values: [String: Sendable]? = nil

  /// Create an `Observation` instance with a random name.
  public init() {
    self.name = UUID().uuidString
  }

  /// Create an `Observation` instance with the given name.
  /// - Parameter name: The name of the observation.
  public init(_ name: String) {
    self.name = name
  }

  /// The observed metrics keyed by the metric column names. Metrics whose observed values are
  /// null are not included.
  /// - Throws: `SparkConnectError.invalidState` if no action has been performed on the observed
  /// ``DataFrame`` yet.
  public var get: [String: Sendable] {
    get throws {
      guard let values else {
        throw SparkConnectError.invalidState(
          SparkConnectError.Details(
            message: "No metrics are observed yet. Perform an action on the observed DataFrame."))
      }
      return values
    }
  }

  /// Update the observed metric values delivered by an `ExecutePlanResponse`.
  func setValues(_ values: [String: Sendable]) {
    self.values = values
  }
}
