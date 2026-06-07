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
/// Use ``DataFrame/stat`` to access this. It mirrors PySpark's `DataFrameStatFunctions`.
public actor DataFrameStatFunctions: Sendable {
  let df: DataFrame

  init(df: DataFrame) {
    self.df = df
  }

  /// Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
  /// The number of distinct values for each column should be less than `1e4`. At most `1e6` non-zero
  /// pair frequencies will be returned. The first column of each row will be the distinct values of
  /// `col1` and the column names will be the distinct values of `col2`. The name of the first column
  /// will be `<col1>_<col2>`. Counts will be returned as `Long`s. Pairs that have no occurrences will
  /// have zero as their counts.
  /// - Parameters:
  ///   - col1: The name of the first column. Distinct items will make the first item of each row.
  ///   - col2: The name of the second column. Distinct items will make the column names of the ``DataFrame``.
  /// - Returns: A ``DataFrame`` containing the contingency table.
  public func crosstab(_ col1: String, _ col2: String) async throws -> DataFrame {
    let plan = await df.getPlan() as! Plan
    return DataFrame(
      spark: await df.spark, plan: SparkConnectClient.getStatCrosstab(plan.root, col1, col2))
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

  /// Calculates the approximate quantiles of a numerical column of a ``DataFrame``.
  /// - Parameters:
  ///   - col: The name of the numerical column.
  ///   - probabilities: A list of quantile probabilities. Each number must belong to `[0, 1]`.
  ///     For example, 0 is the minimum, 0.5 is the median, 1 is the maximum.
  ///   - relativeError: The relative target precision to achieve (greater than or equal to 0).
  ///     If set to zero, the exact quantiles are computed, which could be very expensive. Note that
  ///     values greater than 1 are accepted but give the same result as 1.
  /// - Returns: The approximate quantiles at the given probabilities.
  public func approxQuantile(
    _ col: String, _ probabilities: [Double], _ relativeError: Double
  ) async throws -> [Double] {
    return try await approxQuantile([col], probabilities, relativeError)[0]
  }

  /// Calculates the approximate quantiles of numerical columns of a ``DataFrame``.
  /// - Parameters:
  ///   - cols: The names of the numerical columns.
  ///   - probabilities: A list of quantile probabilities. Each number must belong to `[0, 1]`.
  ///     For example, 0 is the minimum, 0.5 is the median, 1 is the maximum.
  ///   - relativeError: The relative target precision to achieve (greater than or equal to 0).
  ///     If set to zero, the exact quantiles are computed, which could be very expensive. Note that
  ///     values greater than 1 are accepted but give the same result as 1.
  /// - Returns: The approximate quantiles at the given probabilities of each column.
  public func approxQuantile(
    _ cols: [String], _ probabilities: [Double], _ relativeError: Double
  ) async throws -> [[Double]] {
    let plan = await df.getPlan() as! Plan
    let result = DataFrame(
      spark: await df.spark,
      plan: SparkConnectClient.getStatApproxQuantile(plan.root, cols, probabilities, relativeError))
    let quantilesPerColumn = try await result.collect()[0].get(0) as! [any Sendable]
    return quantilesPerColumn.map { ($0 as! [any Sendable]).map { $0 as! Double } }
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
