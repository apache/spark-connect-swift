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

/// An actor to load a streaming `Dataset` from external storage systems
/// (e.g. file systems, key-value stores, etc). Use `SparkSession.readStream` to access this.
public actor DataStreamReader: Sendable {
  var source: String = ""

  var paths: [String] = []

  var extraOptions: CaseInsensitiveDictionary = CaseInsensitiveDictionary([:])

  var userSpecifiedSchemaDDL: String? = nil

  let sparkSession: SparkSession

  init(sparkSession: SparkSession) {
    self.sparkSession = sparkSession
  }

  /// Specifies the input data source format.
  /// - Parameter source: A string.
  /// - Returns: A ``DataStreamReader``.
  public func format(_ source: String) -> DataStreamReader {
    self.source = source
    return self
  }

  /// Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
  /// automatically from data. By specifying the schema here, the underlying data source can skip
  /// the schema inference step, and thus speed up data loading.
  /// - Parameter schema: A DDL schema string.
  /// - Returns: A `DataStreamReader`.
  @discardableResult
  public func schema(_ schema: String) async throws -> DataStreamReader {
    // Validate by parsing.
    do {
      try await sparkSession.client.ddlParse(schema)
    } catch {
      throw SparkConnectError.InvalidTypeException
    }
    self.userSpecifiedSchemaDDL = schema
    return self
  }

  /// Adds an input option for the underlying data source.
  /// - Parameters:
  ///   - key: A key string.
  ///   - value: A value string.
  /// - Returns: A `DataStreamReader`.
  public func option(_ key: String, _ value: String) -> DataStreamReader {
    self.extraOptions[key] = value
    return self
  }

  /// Adds an input option for the underlying data source.
  /// - Parameters:
  ///   - key: A key string.
  ///   - value: A `Bool` value.
  /// - Returns: A `DataStreamReader`.
  public func option(_ key: String, _ value: Bool) -> DataStreamReader {
    self.extraOptions[key] = String(value)
    return self
  }

  /// Adds an input option for the underlying data source.
  /// - Parameters:
  ///   - key: A key string.
  ///   - value: A `Int64` value.
  /// - Returns: A `DataStreamReader`.
  public func option(_ key: String, _ value: Int64) -> DataStreamReader {
    self.extraOptions[key] = String(value)
    return self
  }

  /// Adds an input option for the underlying data source.
  /// - Parameters:
  ///   - key: A key string.
  ///   - value: A `Double` value.
  /// - Returns: A `DataStreamReader`.
  public func option(_ key: String, _ value: Double) -> DataStreamReader {
    self.extraOptions[key] = String(value)
    return self
  }

  /// Adds input options for the underlying data source.
  /// - Parameter options: A string-string dictionary.
  /// - Returns: A `DataStreamReader`.
  public func options(_ options: [String: String]) -> DataStreamReader {
    for (key, value) in options {
      self.extraOptions[key] = value
    }
    return self
  }

  /// Loads input data stream in as a `DataFrame`, for data streams that don't require a path
  /// (e.g. external key-value stores).
  /// - Returns: A `DataFrame`.
  public func load() -> DataFrame {
    return load([])
  }

  /// Loads input data stream in as a `DataFrame`, for data streams that require a path
  /// (e.g. data backed by a local or distributed file system).
  /// - Parameter path: A path string.
  /// - Returns: A `DataFrame`.
  public func load(_ path: String) -> DataFrame {
    return load([path])
  }

  func load(_ paths: [String]) -> DataFrame {
    self.paths = paths

    var dataSource = DataSource()
    dataSource.format = self.source
    dataSource.paths = self.paths
    dataSource.options = self.extraOptions.toStringDictionary()
    if let userSpecifiedSchemaDDL = self.userSpecifiedSchemaDDL {
      dataSource.schema = userSpecifiedSchemaDDL
    }

    var read = Read()
    read.dataSource = dataSource
    read.isStreaming = true

    var relation = Relation()
    relation.read = read

    var plan = Plan()
    plan.opType = .root(relation)

    return DataFrame(spark: sparkSession, plan: plan)
  }

  /// Define a Streaming DataFrame on a Table. The DataSource corresponding to the table should
  /// support streaming mode.
  /// - Parameter tableName: The name of the table.
  /// - Returns: A ``DataFrame``.
  public func table(_ tableName: String) -> DataFrame {
    var namedTable = NamedTable()
    namedTable.unparsedIdentifier = tableName
    namedTable.options = self.extraOptions.toStringDictionary()

    var read = Read()
    read.namedTable = namedTable
    read.isStreaming = true

    var relation = Relation()
    relation.read = read

    var plan = Plan()
    plan.opType = .root(relation)

    return DataFrame(spark: sparkSession, plan: plan)
  }

  /// Loads a text file stream and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func text(_ path: String) -> DataFrame {
    self.source = "text"
    return load(path)
  }

  /// Loads a CSV file stream and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func csv(_ path: String) -> DataFrame {
    self.source = "csv"
    return load(path)
  }

  /// Loads a JSON file stream and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func json(_ path: String) -> DataFrame {
    self.source = "json"
    return load(path)
  }

  /// Loads an XML file stream and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func xml(_ path: String) -> DataFrame {
    self.source = "xml"
    return load(path)
  }

  /// Loads an ORC file stream and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func orc(_ path: String) -> DataFrame {
    self.source = "orc"
    return load(path)
  }

  /// Loads a Parquet file stream and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func parquet(_ path: String) -> DataFrame {
    self.source = "parquet"
    return load(path)
  }
}
