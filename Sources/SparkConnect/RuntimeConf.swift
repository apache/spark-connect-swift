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

/// User-facing configuration API, accessible through `SparkSession.conf`.
public actor RuntimeConf {
  private let client: SparkConnectClient

  /// Create a `RuntimeConf` instance with the given client.
  /// - Parameter client: A client to talk to the Spark Connect server.
  init(_ client: SparkConnectClient) {
    self.client = client
  }

  /// Set a new configuration.
  /// - Parameters:
  ///   - key: A string for the configuration key.
  ///   - value: A string for the configuration value.
  public func set(_ key: String, _ value: String) async throws {
    try await client.setConf(map: [key: value])
  }

  /// Reset a configuration.
  /// - Parameters:
  ///   - key: A string for the configuration key.
  public func unset(_ key: String) async throws {
    try await client.unsetConf(keys: [key])
  }

  /// Get a configuration.
  /// - Parameter key: A string for the configuration look-up.
  /// - Returns: A string for the configuration.
  public func get(_ key: String) async throws -> String {
    return try await client.getConf(key)
  }

  /// Get all configurations.
  /// - Returns: A map of configuration key-values.
  public func getAll() async throws -> [String: String] {
    return try await client.getConfAll()
  }
}
