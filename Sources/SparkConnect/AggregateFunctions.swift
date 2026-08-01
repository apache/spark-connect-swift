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

/// Returns some value of the column for a group of rows.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func any_value(_ col: Column) -> Column {
  return fn("any_value", col)
}

/// Returns some value of the column for a group of rows.
/// If `ignoreNulls` is true, returns only non-null values.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - ignoreNulls: A ``Column`` that evaluates to a boolean. Must be a constant.
/// - Returns: A ``Column``.
public func any_value(_ col: Column, _ ignoreNulls: Column) -> Column {
  return fn("any_value", col, ignoreNulls)
}

/// Returns the approximate number of distinct items in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func approx_count_distinct(_ col: Column) -> Column {
  return fn("approx_count_distinct", col)
}

/// Returns the approximate number of distinct items in a group.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - rsd: The maximum relative standard deviation allowed (default = 0.05).
/// - Returns: A ``Column``.
public func approx_count_distinct(_ col: Column, _ rsd: Double) -> Column {
  return fn("approx_count_distinct", col, lit(rsd))
}

/// Returns true if all values of the column are true.
/// - Parameter col: A ``Column`` that evaluates to a boolean.
/// - Returns: A ``Column``.
public func bool_and(_ col: Column) -> Column {
  return fn("bool_and", col)
}

/// Returns true if at least one value of the column is true.
/// - Parameter col: A ``Column`` that evaluates to a boolean.
/// - Returns: A ``Column``.
public func bool_or(_ col: Column) -> Column {
  return fn("bool_or", col)
}

/// Returns a list of objects with duplicates.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func collect_list(_ col: Column) -> Column {
  return fn("collect_list", col)
}

/// Returns a set of objects with duplicate elements eliminated.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func collect_set(_ col: Column) -> Column {
  return fn("collect_set", col)
}

/// Returns the Pearson Correlation Coefficient for two columns.
/// - Parameters:
///   - column1: A ``Column``.
///   - column2: A ``Column``.
/// - Returns: A ``Column``.
public func corr(_ column1: Column, _ column2: Column) -> Column {
  return fn("corr", column1, column2)
}

/// Returns the number of distinct items in a group.
/// This is an alias of ``count_distinct(_:_:)``.
/// - Parameters:
///   - expr: A ``Column`` to count.
///   - exprs: Additional ``Column``s to count.
/// - Returns: A ``Column``.
public func countDistinct(_ expr: Column, _ exprs: Column...) -> Column {
  return fn("count", [expr] + exprs, isDistinct: true)
}

/// Returns the number of distinct items in a group.
/// - Parameters:
///   - expr: A ``Column`` to count.
///   - exprs: Additional ``Column``s to count.
/// - Returns: A ``Column``.
public func count_distinct(_ expr: Column, _ exprs: Column...) -> Column {
  return fn("count", [expr] + exprs, isDistinct: true)
}

/// Returns the number of `TRUE` values for the expression.
/// - Parameter col: A ``Column`` that evaluates to a boolean.
/// - Returns: A ``Column``.
public func count_if(_ col: Column) -> Column {
  return fn("count_if", col)
}

/// Returns the population covariance for two columns.
/// - Parameters:
///   - column1: A ``Column``.
///   - column2: A ``Column``.
/// - Returns: A ``Column``.
public func covar_pop(_ column1: Column, _ column2: Column) -> Column {
  return fn("covar_pop", column1, column2)
}

/// Returns the sample covariance for two columns.
/// - Parameters:
///   - column1: A ``Column``.
///   - column2: A ``Column``.
/// - Returns: A ``Column``.
public func covar_samp(_ column1: Column, _ column2: Column) -> Column {
  return fn("covar_samp", column1, column2)
}

/// Returns the first value in a group.
///
/// The function by default returns the first values it sees. It will return the first non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func first(_ col: Column) -> Column {
  return first(col, false)
}

/// Returns the first value in a group.
///
/// The function by default returns the first values it sees. It will return the first non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - ignoreNulls: Whether to skip null values.
/// - Returns: A ``Column``.
public func first(_ col: Column, _ ignoreNulls: Bool) -> Column {
  return fn("first", col, lit(ignoreNulls))
}

/// Indicates whether a specified column in a GROUP BY list is aggregated or not,
/// returns 1 for aggregated or 0 for not aggregated in the result set.
/// - Parameter col: A ``Column`` to check.
/// - Returns: A ``Column``.
public func grouping(_ col: Column) -> Column {
  return fn("grouping", col)
}

/// Returns the level of grouping.
/// - Parameter cols: ``Column``s to check.
/// - Returns: A ``Column``.
public func grouping_id(_ cols: Column...) -> Column {
  return fn("grouping_id", cols)
}

/// Returns the kurtosis of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func kurtosis(_ col: Column) -> Column {
  return fn("kurtosis", col)
}

/// Returns the last value in a group.
///
/// The function by default returns the last values it sees. It will return the last non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func last(_ col: Column) -> Column {
  return last(col, false)
}

/// Returns the last value in a group.
///
/// The function by default returns the last values it sees. It will return the last non-null
/// value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - ignoreNulls: Whether to skip null values.
/// - Returns: A ``Column``.
public func last(_ col: Column, _ ignoreNulls: Bool) -> Column {
  return fn("last", col, lit(ignoreNulls))
}

/// Returns the value associated with the maximum value of `ord`.
/// - Parameters:
///   - col: A ``Column`` to return the value from.
///   - ord: A ``Column`` to be maximized.
/// - Returns: A ``Column``.
public func max_by(_ col: Column, _ ord: Column) -> Column {
  return fn("max_by", col, ord)
}

/// Returns the median of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func median(_ col: Column) -> Column {
  return fn("median", col)
}

/// Returns the value associated with the minimum value of `ord`.
/// - Parameters:
///   - col: A ``Column`` to return the value from.
///   - ord: A ``Column`` to be minimized.
/// - Returns: A ``Column``.
public func min_by(_ col: Column, _ ord: Column) -> Column {
  return fn("min_by", col, ord)
}

/// Returns the most frequent value in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func mode(_ col: Column) -> Column {
  return fn("mode", col)
}

/// Returns the most frequent value in a group.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - deterministic: If there are multiple equally-frequent results,
///     whether to return the lowest (defined by min-hash) one.
/// - Returns: A ``Column``.
public func mode(_ col: Column, _ deterministic: Bool) -> Column {
  return fn("mode", col, lit(deterministic))
}

/// Returns the approximate `percentile` of the numeric column `col` which is the smallest value
/// in the ordered `col` values (sorted from least to greatest) such that no more than `percentage`
/// of `col` values is less than the value or equal to that value.
/// - Parameters:
///   - col: A ``Column`` to aggregate.
///   - percentage: A percentage ``Column``. Each value must be between 0.0 and 1.0.
///   - accuracy: A positive numeric literal ``Column`` that controls approximation accuracy
///     at the cost of memory.
/// - Returns: A ``Column``.
public func percentile_approx(_ col: Column, _ percentage: Column, _ accuracy: Column) -> Column {
  return fn("percentile_approx", col, percentage, accuracy)
}

/// Returns the skewness of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func skewness(_ col: Column) -> Column {
  return fn("skewness", col)
}

/// Returns the sample standard deviation of the expression in a group.
/// This is an alias of ``stddev_samp(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func stddev(_ col: Column) -> Column {
  return fn("stddev", col)
}

/// Returns the population standard deviation of the expression in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func stddev_pop(_ col: Column) -> Column {
  return fn("stddev_pop", col)
}

/// Returns the sample standard deviation of the expression in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func stddev_samp(_ col: Column) -> Column {
  return fn("stddev_samp", col)
}

/// Returns the sum of distinct values in the expression.
/// This is an alias of ``sum_distinct(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func sumDistinct(_ col: Column) -> Column {
  return sum_distinct(col)
}

/// Returns the sum of distinct values in the expression.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func sum_distinct(_ col: Column) -> Column {
  return fn("sum", [col], isDistinct: true)
}

/// Returns the population variance of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func var_pop(_ col: Column) -> Column {
  return fn("var_pop", col)
}

/// Returns the unbiased variance of the values in a group.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func var_samp(_ col: Column) -> Column {
  return fn("var_samp", col)
}

/// Returns the unbiased variance of the values in a group.
/// This is an alias of ``var_samp(_:)``.
/// - Parameter col: A ``Column`` to aggregate.
/// - Returns: A ``Column``.
public func variance(_ col: Column) -> Column {
  return fn("variance", col)
}
