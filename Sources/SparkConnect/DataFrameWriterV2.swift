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

/// Interface used to write a ``DataFrame`` to external storage using the v2 API.
public actor DataFrameWriterV2: Sendable {

  let tableName: String

  let df: DataFrame

  var provider: String? = nil

  var extraOptions: CaseInsensitiveDictionary = CaseInsensitiveDictionary()

  var tableProperties: CaseInsensitiveDictionary = CaseInsensitiveDictionary()

  var partitioningColumns: [Spark_Connect_Expression] = []

  var clusteringColumns: [String]? = nil

  init(_ table: String, _ df: DataFrame) {
    self.tableName = table
    self.df = df
  }

  /// Specifies a provider for the underlying output data source. Spark's default catalog supports
  /// "orc", "json", etc.
  /// - Parameter provider: <#provider description#>
  public func using(_ provider: String) -> DataFrameWriterV2 {
    self.provider = provider
    return self
  }

  /// Adds an output option for the underlying data source.
  /// - Parameters:
  ///   - key: A key string.
  ///   - value: A value string.
  /// - Returns: A `DataFrameWriter`.
  public func option(_ key: String, _ value: String) -> DataFrameWriterV2 {
    self.extraOptions[key] = value
    return self
  }

  /// Add a table property.
  /// - Parameters:
  ///   - property: A property name.
  ///   - value: A property value.
  public func tableProperty(property: String, value: String) -> DataFrameWriterV2 {
    self.tableProperties[property] = value
    return self
  }

  /// Partition the output table created by `create`, `createOrReplace`, or `replace` using the
  /// given columns or transforms.
  /// - Parameter columns: Columns to partition
  /// - Returns: A ``DataFrameWriterV2``.
  public func partitionBy(_ columns: String...) -> DataFrameWriterV2 {
    self.partitioningColumns = columns.map {
      var expr = Spark_Connect_Expression()
      expr.expressionString = $0.toExpressionString
      return expr
    }
    return self
  }

  /// Clusters the output by the given columns on the storage. The rows with matching values in the
  /// specified clustering columns will be consolidated within the same group.
  /// - Parameter columns: Columns to cluster
  /// - Returns: A ``DataFrameWriterV2``.
  public func clusterBy(_ columns: String...) -> DataFrameWriterV2 {
    self.clusteringColumns = columns
    return self
  }

  /// Create a new table from the contents of the data frame.
  public func create() async throws {
    try await executeWriteOperation(.create)
  }

  /// Replace an existing table with the contents of the data frame.
  public func replace() async throws {
    try await executeWriteOperation(.replace)
  }

  /// Create a new table or replace an existing table with the contents of the data frame.
  public func createOrReplace() async throws {
    try await executeWriteOperation(.createOrReplace)
  }

  /// Append the contents of the data frame to the output table.
  public func append() async throws {
    try await executeWriteOperation(.append)
  }

  /// Overwrite rows matching the given filter condition with the contents of the ``DataFrame`` in the
  /// output table.
  /// - Parameter condition: A filter condition.
  public func overwrite(condition: String) async throws {
    try await executeWriteOperation(.overwrite)
  }

  /// Overwrite all partition for which the ``DataFrame`` contains at least one row with the contents
  /// of the data frame in the output table.
  /// This operation is equivalent to Hive's `INSERT OVERWRITE ... PARTITION`, which replaces
  /// partitions dynamically depending on the contents of the ``DataFrame``.
  public func overwritePartitions() async throws {
    try await executeWriteOperation(.overwritePartitions)
  }

  private func executeWriteOperation(_ mode: WriteOperationV2.Mode) async throws {
    var write = WriteOperationV2()

    let plan = await self.df.getPlan() as! Plan
    write.input = plan.root
    write.tableName = self.tableName
    if let provider = self.provider {
      write.provider = provider
    }
    write.partitioningColumns = self.partitioningColumns
    if let clusteringColumns = self.clusteringColumns {
      write.clusteringColumns = clusteringColumns
    }
    for option in self.extraOptions.toStringDictionary() {
      write.options[option.key] = option.value
    }
    for property in self.tableProperties.toStringDictionary() {
      write.tableProperties[property.key] = property.value
    }
    write.mode = mode

    var command = Spark_Connect_Command()
    command.writeOperationV2 = write
    _ = try await df.spark.client.execute(df.spark.sessionID, command)
  }
}
