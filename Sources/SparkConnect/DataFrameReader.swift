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
import Atomics
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import GRPCProtobuf
import NIOCore
import SwiftyTextTable
import Synchronization

/// An interface used to load a `DataFrame` from external storage systems
/// (e.g. file systems, key-value stores, etc). Use `SparkSession.read` to access this.
public actor DataFrameReader: Sendable {
  var source: String = ""

  var paths: [String] = []

  // TODO: Case-insensitive Map
  var extraOptions: [String: String] = [:]

  let sparkSession: SparkSession

  init(sparkSession: SparkSession) {
    self.sparkSession = sparkSession
  }

  /// Specifies the input data source format.
  /// - Parameter source: A string.
  /// - Returns: A `DataFrameReader`.
  public func format(_ source: String) -> DataFrameReader {
    self.source = source
    return self
  }

  /// Adds an input option for the underlying data source.
  /// - Parameters:
  ///   - key: A key string.
  ///   - value: A value string.
  /// - Returns: A `DataFrameReader`.
  public func option(_ key: String, _ value: String) -> DataFrameReader {
    self.extraOptions[key] = value
    return self
  }

  /// Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
  /// key-value stores).
  /// - Returns: A `DataFrame`.
  public func load() -> DataFrame {
    return load([])
  }

  /// Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by a
  /// local or distributed file system).
  /// - Parameter path: A path string.
  /// - Returns: A `DataFrame`.
  public func load(_ path: String) -> DataFrame {
    return load([path])
  }

  /// Loads input in as a `DataFrame`, for data sources that support multiple paths. Only works if
  /// the source is a HadoopFsRelationProvider.
  /// - Parameter paths: An array of path strings.
  /// - Returns: A `DataFrame`.
  public func load(_ paths: [String]) -> DataFrame {
    self.paths = paths

    var dataSource = DataSource()
    dataSource.format = self.source
    dataSource.paths = self.paths
    dataSource.options = self.extraOptions

    var read = Read()
    read.dataSource = dataSource

    var relation = Relation()
    relation.read = read

    var plan = Plan()
    plan.opType = .root(relation)

    return DataFrame(spark: sparkSession, plan: plan)
  }

  /// Loads an Avro file and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func avro(_ path: String) -> DataFrame {
    self.source = "avro"
    return load(path)
  }

  /// Loads Avro files and returns the result as a `DataFrame`.
  /// - Parameter paths: Path strings
  /// - Returns: A `DataFrame`.
  public func avro(_ paths: String...) -> DataFrame {
    self.source = "avro"
    return load(paths)
  }

  /// Loads a CSV file and returns the result as a `DataFrame`. See the documentation on the other
  /// overloaded `csv()` method for more details.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func csv(_ path: String) -> DataFrame {
    self.source = "csv"
    return load(path)
  }

  /// Loads CSV files and returns the result as a `DataFrame`.
  /// This function will go through the input once to determine the input schema if `inferSchema`
  /// is enabled. To avoid going through the entire data once, disable `inferSchema` option or
  /// specify the schema explicitly using `schema`.
  /// - Parameter paths: Path strings.
  /// - Returns: A `DataFrame`.
  public func csv(_ paths: String...) -> DataFrame {
    self.source = "csv"
    return load(paths)
  }

  /// Loads a JSON file and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func json(_ path: String) -> DataFrame {
    self.source = "json"
    return load(path)
  }

  /// Loads JSON files and returns the result as a `DataFrame`.
  /// - Parameter paths: Path strings
  /// - Returns: A `DataFrame`.
  public func json(_ paths: String...) -> DataFrame {
    self.source = "json"
    return load(paths)
  }

  /// Loads an ORC file and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func orc(_ path: String) -> DataFrame {
    self.source = "orc"
    return load(path)
  }

  /// Loads ORC files and returns the result as a `DataFrame`.
  /// - Parameter paths: Path strings
  /// - Returns: A `DataFrame`.
  public func orc(_ paths: String...) -> DataFrame {
    self.source = "orc"
    return load(paths)
  }

  /// Loads a Parquet file and returns the result as a `DataFrame`.
  /// - Parameter path: A path string
  /// - Returns: A `DataFrame`.
  public func parquet(_ path: String) -> DataFrame {
    self.source = "parquet"
    return load(path)
  }

  /// Loads Parquet files, returning the result as a `DataFrame`.
  /// - Parameter paths: Path strings
  /// - Returns: A `DataFrame`.
  public func parquet(_ paths: String...) -> DataFrame {
    self.source = "parquet"
    return load(paths)
  }
}
