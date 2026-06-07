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

/// Statistic functions for ``DataFrame``s.
///
/// Use ``DataFrame/stat`` to access this. It mirrors PySpark's `DataFrameStatFunctions`
/// (`df.stat.cov`, `df.stat.corr`).
public actor DataFrameStatFunctions: Sendable {
  let df: DataFrame

  init(df: DataFrame) {
    self.df = df
  }

  /// Calculates the sample covariance of two numerical columns of a ``DataFrame``.
  /// - Parameters:
  ///   - col1: The name of the first column.
  ///   - col2: The name of the second column.
  /// - Returns: The sample covariance of the two columns.
  public func cov(_ col1: String, _ col2: String) async throws -> Double {
    return try await collectDouble { SparkConnectClient.getStatCov($0, col1, col2) }
  }

  /// Calculates the correlation of two columns of a ``DataFrame``. Currently only supports the
  /// Pearson Correlation Coefficient.
  /// - Parameters:
  ///   - col1: The name of the first column.
  ///   - col2: The name of the second column.
  ///   - method: The correlation method. Currently only `pearson` is supported.
  /// - Returns: The Pearson Correlation Coefficient of the two columns.
  public func corr(
    _ col1: String, _ col2: String, method: String = "pearson"
  ) async throws -> Double {
    return try await collectDouble { SparkConnectClient.getStatCorr($0, col1, col2, method) }
  }

  // MARK: - Helpers

  /// Builds a single-value ``DataFrame`` from this ``DataFrame``'s plan using the given plan
  /// builder, executes it, and returns the resulting `Double`.
  private func collectDouble(_ f: (Relation) -> Plan) async throws -> Double {
    let plan = await df.getPlan() as! Plan
    let result = DataFrame(spark: await df.spark, plan: f(plan.root))
    return try await result.collect()[0].get(0) as! Double
  }
}

extension DataFrame {
  /// Returns a ``DataFrameStatFunctions`` for working with statistic functions.
  public var stat: DataFrameStatFunctions {
    DataFrameStatFunctions(df: self)
  }
}
