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

  /// Set a new configuration.
  /// - Parameters:
  ///   - key: A string for the configuration key.
  ///   - value: A boolean value for the configuration.
  public func set(_ key: String, _ value: Bool) async throws {
    try await client.setConf(map: [key: String(value)])
  }

  /// Set a new configuration.
  /// - Parameters:
  ///   - key: A string for the configuration key.
  ///   - value: A Int64 value for the configuration.
  public func set(_ key: String, _ value: Int64) async throws {
    try await client.setConf(map: [key: String(value)])
  }

  /// Reset a configuration.
  /// - Parameters:
  ///   - key: A string for the configuration key.
  public func unset(_ key: String) async throws {
    try await client.unsetConf(keys: [key])
  }

  /// Returns the value of Spark runtime configuration property for the given key. If the key is
  /// not set yet, return its default value if possible, otherwise `NoSuchElementException` will be
  /// thrown.
  /// - Parameter key: A string for the configuration look-up.
  /// - Returns: A string for the configuration.
  public func get(_ key: String) async throws -> String {
    return try await client.getConf(key)
  }

  /// Returns the value of Spark runtime configuration property for the given key. If the key is
  /// not set yet, return the user given `value`. This is useful when its default value defined
  /// by Apache Spark is not the desired one.
  /// - Parameters:
  ///   - key: A string for the configuration key.
  ///   - value: A default string value for the configuration.
  public func get(_ key: String, _ value: String) async throws -> String {
    return try await client.getConfWithDefault(key, value)
  }

  /// Get all configurations.
  /// - Returns: A map of configuration key-values.
  public func getAll() async throws -> [String: String] {
    return try await client.getConfAll()
  }

  /// Returns the value of Spark runtime configuration property for the given key. If the key is
  /// not set yet, return its default value if possible, otherwise `nil` will be returned.
  /// - Parameter key: A string for the configuration look-up.
  /// - Returns: A string for the configuration or nil.
  public func getOption(_ key: String) async throws -> String? {
    return try await client.getConfOption(key)
  }

  /// Indicates whether the configuration property with the given key is modifiable in the current
  /// session.
  /// - Parameter key: A string for the configuration look-up.
  /// - Returns: `true` if the configuration property is modifiable. For static SQL, Spark Core, invalid
  /// (not existing) and other non-modifiable configuration properties, the returned value is
  /// `false`.
  public func isModifiable(_ key: String) async throws -> Bool {
    return try await client.isModifiable(key)
  }
}
