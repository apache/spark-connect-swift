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

public enum Trigger {
  case OneTime
  case AvailableNow
  case ProcessingTime(_ intervalMs: Int64)
  case Continuous(_ intervalMs: Int64)
}

/// An actor used to write a streaming `DataFrame` to external storage systems
/// (e.g. file systems, key-value stores, etc). Use `DataFrame.writeStream` to access this.
public actor DataStreamWriter: Sendable {
  var queryName: String? = nil

  var source: String? = nil

  var trigger: Trigger = Trigger.ProcessingTime(0)

  var path: String? = nil

  var tableName: String? = nil

  var outputMode: String? = nil

  var extraOptions: CaseInsensitiveDictionary = CaseInsensitiveDictionary()

  var partitioningColumns: [String]? = nil

  var clusteringColumns: [String]? = nil

  let df: DataFrame

  init(df: DataFrame) {
    self.df = df
  }

  /// Specifies the name of the ``StreamingQuery`` that can be
  /// started with `start()`. This name must be unique among all the currently active queries in
  /// the associated SparkSession.
  /// - Parameter queryName: A string name.
  /// - Returns: A ``DataStreamWriter``.
  public func queryName(_ queryName: String) -> DataStreamWriter {
    self.queryName = queryName
    return self
  }

  /// Specifies the underlying output data source.
  /// - Parameter source: A string.
  /// - Returns: A `DataStreamWriter`.
  public func format(_ source: String) -> DataStreamWriter {
    self.source = source
    return self
  }

  /// Specifies how data of a streaming ``DataFrame`` is written to a streaming sink.
  ///
  /// - `append`: only the new rows in the streaming ``DataFrame`` will be written to the sink.
  /// - `complete`: all the rows in the streaming ``DataFrame`` will be written to the sink
  /// every time there are some updates.
  /// - `update`: only the rows that were updated in the streaming ``DataFrame`` will be
  /// written to the sink every time there are some updates. If the query doesn't contain
  /// aggregations, it will be equivalent to `append` mode.
  ///
  /// - Parameter outputMode: A string for outputMode.
  /// - Returns: A ``DataStreamWriter``.
  public func outputMode(_ outputMode: String) -> DataStreamWriter {
    self.outputMode = outputMode
    return self
  }

  /// Adds an output option for the underlying data source.
  /// - Parameters:
  ///   - key: A key string.
  ///   - value: A value string.
  /// - Returns: A `DataStreamWriter`.
  public func option(_ key: String, _ value: String) -> DataStreamWriter {
    self.extraOptions[key] = value
    return self
  }

  /// Partitions the output by the given columns on the file system. If specified, the output is
  /// laid out on the file system similar to Hive's partitioning scheme.
  /// - Parameter colNames: Column names to partition.
  /// - Returns: A ``DataStreamWriter``.
  public func partitionBy(_ colNames: String...) -> DataStreamWriter {
    self.partitioningColumns = colNames
    return self
  }

  /// Clusters the output by the given columns. If specified, the output is laid out such that
  /// records with similar values on the clustering column are grouped together in the same file.
  /// - Parameter colNames: Column names to cluster.
  /// - Returns: A ``DataStreamWriter``.
  public func clusterBy(_ colNames: String...) -> DataStreamWriter {
    self.clusteringColumns = colNames
    return self
  }

  /// Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
  /// key-value stores).
  public func trigger(_ trigger: Trigger) async throws -> DataStreamWriter {
    self.trigger = trigger
    return self
  }

  /// Starts the execution of the streaming query, which will continually output results to the
  /// given path as new data arrives. The returned ``StreamingQuery`` object can be used to interact
  /// with the stream.
  /// - Parameter path: A path to write.
  /// - Returns: A ``StreamingQuery``.
  public func start(_ path: String) async throws -> StreamingQuery {
    self.path = path
    return try await start()
  }

  /// Starts the execution of the streaming query, which will continually output results to the
  /// given path as new data arrives. The returned ``StreamingQuery`` object can be used to interact
  /// with the stream. Throws exceptions if the following conditions are met:
  /// - Another run of the same streaming query, that is a streaming query sharing the same
  ///   checkpoint location, is already active on the same Spark Driver
  /// - The SQL configuration `spark.sql.streaming.stopActiveRunOnRestart` is enabled
  /// - The active run cannot be stopped within the timeout controlled by the SQL configuration `spark.sql.streaming.stopTimeout`
  ///
  /// - Returns: A ``StreamingQuery``.
  public func start() async throws -> StreamingQuery {
    var writeStreamOperationStart = WriteStreamOperationStart()
    writeStreamOperationStart.input = (await df.getPlan() as! Plan).root
    if let source = self.source {
      writeStreamOperationStart.format = source
    }
    writeStreamOperationStart.options = self.extraOptions.toStringDictionary()
    if let partitioningColumns = self.partitioningColumns {
      writeStreamOperationStart.partitioningColumnNames = partitioningColumns
    }
    if let clusteringColumns = self.clusteringColumns {
      writeStreamOperationStart.clusteringColumnNames = clusteringColumns
    }
    writeStreamOperationStart.trigger =
      switch self.trigger {
      case .ProcessingTime(let intervalMs):
        .processingTimeInterval("INTERVAL \(intervalMs) MILLISECOND")
      case .OneTime:
        .once(true)
      case .AvailableNow:
        .availableNow(true)
      case .Continuous(let intervalMs):
        .continuousCheckpointInterval("INTERVAL \(intervalMs) MILLISECOND")
      }
    if let outputMode = self.outputMode {
      writeStreamOperationStart.outputMode = outputMode
    }
    if let queryName = self.queryName {
      writeStreamOperationStart.queryName = queryName
    }
    if let path = self.path {
      writeStreamOperationStart.sinkDestination = .path(path)
    }
    if let tableName = self.tableName {
      writeStreamOperationStart.sinkDestination = .tableName(tableName)
    }

    var command = Spark_Connect_Command()
    command.writeStreamOperationStart = writeStreamOperationStart

    let response = try await df.spark.client.execute(df.spark.sessionID, command)
    let result = response.first!.writeStreamOperationStartResult
    if result.hasQueryStartedEventJson {
      // TODO: post
    }

    let query = try await StreamingQuery(
      UUID(uuidString: result.queryID.id)!,
      UUID(uuidString: result.queryID.runID)!,
      result.name,
      self.df.sparkSession
    )

    return query
  }

  /// Starts the execution of the streaming query, which will continually output results to the
  /// given table as new data arrives. The returned ``StreamingQuery`` object can be used to interact
  /// with the stream.
  /// - Parameter tableName: A table name.
  /// - Returns: A ``StreamingQuery``.
  public func toTable(tableName: String) async throws -> StreamingQuery {
    self.tableName = tableName
    return try await start()
  }
}
