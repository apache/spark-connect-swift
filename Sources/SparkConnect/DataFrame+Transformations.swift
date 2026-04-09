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

/// Extension providing transformation operations on ``DataFrame``.
///
/// Transformations are lazy operations that return a new ``DataFrame``
/// without triggering computation. They include column operations,
/// filtering, sorting, joins, set operations, partitioning, and more.
extension DataFrame {

  // MARK: - Column Operations

  /// Selects a subset of existing columns using column names.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func select(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getProject(self.plan.root, cols))
  }

  /// Selects a subset of existing columns using column names.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func toDF(_ cols: String...) -> DataFrame {
    let df =
      if cols.isEmpty {
        DataFrame(spark: self.spark, plan: self.plan)
      } else {
        DataFrame(spark: self.spark, plan: SparkConnectClient.getProject(self.plan.root, cols))
      }
    return df
  }

  /// Returns a new DataFrame where each row is reconciled to match the specified schema.
  /// - Parameter schema: The given schema.
  /// - Returns: A ``DataFrame`` with the given schema.
  public func to(_ schema: String) async throws -> DataFrame {
    let dataType = try await sparkSession.client.ddlParse(schema)
    return DataFrame(
      spark: self.spark, plan: SparkConnectClient.getToSchema(self.plan.root, dataType))
  }

  /// Returns the content of the Dataset as a Dataset of JSON strings.
  /// - Returns: A ``DataFrame`` with a single string column whose content is JSON.
  public func toJSON() -> DataFrame {
    return selectExpr("to_json(struct(*))")
  }

  /// Projects a set of expressions and returns a new ``DataFrame``.
  /// - Parameter exprs: Expression strings
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func selectExpr(_ exprs: String...) -> DataFrame {
    return DataFrame(
      spark: self.spark, plan: SparkConnectClient.getProjectExprs(self.plan.root, exprs))
  }

  /// Returns a new Dataset with a column dropped. This is a no-op if schema doesn't contain column name.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame`` with subset of columns.
  public func drop(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getDrop(self.plan.root, cols))
  }

  /// Returns a new ``DataFrame`` that contains only the unique rows from this ``DataFrame``.
  /// This is an alias for `distinct`. If column names are given, Spark considers only those columns.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame``.
  public func dropDuplicates(_ cols: String...) -> DataFrame {
    let plan = SparkConnectClient.getDropDuplicates(self.plan.root, cols, withinWatermark: false)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new Dataset with duplicates rows removed, within watermark.
  /// If column names are given, Spark considers only those columns.
  /// - Parameter cols: Column names
  /// - Returns: A ``DataFrame``.
  public func dropDuplicatesWithinWatermark(_ cols: String...) -> DataFrame {
    let plan = SparkConnectClient.getDropDuplicates(self.plan.root, cols, withinWatermark: true)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Computes basic statistics for numeric and string columns, including count, mean, stddev, min,
  /// and max. If no columns are given, this function computes statistics for all numerical or
  /// string columns.
  /// - Parameter cols: Column names.
  /// - Returns: A ``DataFrame`` containing basic statistics.
  public func describe(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getDescribe(self.plan.root, cols))
  }

  /// Computes specified statistics for numeric and string columns. Available statistics are:
  /// count, mean, stddev, min, max, arbitrary approximate percentiles specified as a percentage (e.g. 75%)
  /// count_distinct, approx_count_distinct . If no statistics are given, this function computes count, mean,
  /// stddev, min, approximate quartiles (percentiles at 25%, 50%, and 75%), and max.
  /// - Parameter statistics: Statistics names.
  /// - Returns: A ``DataFrame`` containing specified statistics.
  public func summary(_ statistics: String...) -> DataFrame {
    return DataFrame(
      spark: self.spark, plan: SparkConnectClient.getSummary(self.plan.root, statistics))
  }

  /// Returns a new Dataset with a column renamed. This is a no-op if schema doesn't contain existingName.
  /// - Parameters:
  ///   - existingName: A existing column name to be renamed.
  ///   - newName: A new column name.
  /// - Returns: A ``DataFrame`` with the renamed column.
  public func withColumnRenamed(_ existingName: String, _ newName: String) -> DataFrame {
    return withColumnRenamed([existingName: newName])
  }

  /// Returns a new Dataset with columns renamed. This is a no-op if schema doesn't contain existingName.
  /// - Parameters:
  ///   - colNames: A list of existing colum names to be renamed.
  ///   - newColNames: A list of new column names.
  /// - Returns: A ``DataFrame`` with the renamed columns.
  public func withColumnRenamed(_ colNames: [String], _ newColNames: [String]) -> DataFrame {
    let dic = Dictionary(uniqueKeysWithValues: zip(colNames, newColNames))
    return DataFrame(
      spark: self.spark, plan: SparkConnectClient.getWithColumnRenamed(self.plan.root, dic))
  }

  /// Returns a new Dataset with columns renamed. This is a no-op if schema doesn't contain existingName.
  /// - Parameter colsMap: A dictionary of existing column name and new column name.
  /// - Returns: A ``DataFrame`` with the renamed columns.
  public func withColumnRenamed(_ colsMap: [String: String]) -> DataFrame {
    return DataFrame(
      spark: self.spark, plan: SparkConnectClient.getWithColumnRenamed(self.plan.root, colsMap))
  }

  // MARK: - Filtering and Sorting

  /// Filters rows using the given condition.
  ///
  /// The condition should be a SQL expression that evaluates to a boolean value.
  ///
  /// ```swift
  /// // Filter with simple condition
  /// let adults = df.filter("age >= 18")
  ///
  /// // Filter with complex condition
  /// let qualifiedUsers = df.filter("age >= 21 AND department = 'Engineering'")
  ///
  /// // Filter with SQL functions
  /// let recent = df.filter("date_diff(current_date(), join_date) < 30")
  /// ```
  ///
  /// - Parameter conditionExpr: A SQL expression string for filtering
  /// - Returns: A new DataFrame containing only rows that match the condition
  public func filter(_ conditionExpr: String) -> DataFrame {
    return DataFrame(
      spark: self.spark, plan: SparkConnectClient.getFilter(self.plan.root, conditionExpr))
  }

  /// Filters rows using the given condition (alias for filter).
  ///
  /// This method is an alias for ``filter(_:)`` and behaves identically.
  ///
  /// ```swift
  /// let highSalary = df.where("salary > 100000")
  /// ```
  ///
  /// - Parameter conditionExpr: A SQL expression string for filtering
  /// - Returns: A new DataFrame containing only rows that match the condition
  public func `where`(_ conditionExpr: String) -> DataFrame {
    return filter(conditionExpr)
  }

  /// Returns a new DataFrame sorted by the specified columns.
  ///
  /// By default, sorts in ascending order. Use `desc("column")` for descending order.
  ///
  /// ```swift
  /// // Sort by single column (ascending)
  /// let sorted = df.sort("age")
  ///
  /// // Sort by multiple columns
  /// let multiSort = df.sort("department", "salary")
  ///
  /// // Sort with mixed order
  /// let mixedSort = df.sort("department", "desc(salary)")
  /// ```
  ///
  /// - Parameter cols: Column names or expressions to sort by
  /// - Returns: A new DataFrame sorted by the specified columns
  public func sort(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getSort(self.plan.root, cols))
  }

  /// Return a new ``DataFrame`` sorted by the specified column(s).
  /// - Parameter cols: Column names.
  /// - Returns: A sorted ``DataFrame``
  public func orderBy(_ cols: String...) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getSort(self.plan.root, cols))
  }

  /// Limits the result count to the number specified.
  ///
  /// This transformation is often used for:
  /// - Previewing data
  /// - Reducing data size for testing
  /// - Implementing pagination
  ///
  /// ```swift
  /// // Get top 10 records
  /// let top10 = df.limit(10)
  ///
  /// // Preview data
  /// let preview = df.filter("status = 'active'").limit(100)
  /// ```
  ///
  /// - Parameter n: Maximum number of rows to return
  /// - Returns: A new DataFrame with at most n rows
  public func limit(_ n: Int32) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getLimit(self.plan.root, n))
  }

  /// Returns a new Dataset by skipping the first `n` rows.
  /// - Parameter n: Number of rows to skip.
  /// - Returns: A subset of the rows
  public func offset(_ n: Int32) -> DataFrame {
    return DataFrame(spark: self.spark, plan: SparkConnectClient.getOffset(self.plan.root, n))
  }

  /// Returns a new ``Dataset`` by sampling a fraction of rows, using a user-supplied seed.
  /// - Parameters:
  ///   - withReplacement: Sample with replacement or not.
  ///   - fraction: Fraction of rows to generate, range [0.0, 1.0].
  ///   - seed: Seed for sampling.
  /// - Returns: A subset of the records.
  public func sample(_ withReplacement: Bool, _ fraction: Double, _ seed: Int64) -> DataFrame {
    return DataFrame(
      spark: self.spark,
      plan: SparkConnectClient.getSample(self.plan.root, withReplacement, fraction, seed))
  }

  /// Returns a new ``Dataset`` by sampling a fraction of rows, using a random seed.
  /// - Parameters:
  ///   - withReplacement: Sample with replacement or not.
  ///   - fraction: Fraction of rows to generate, range [0.0, 1.0].
  /// - Returns: A subset of the records.
  public func sample(_ withReplacement: Bool, _ fraction: Double) -> DataFrame {
    return sample(withReplacement, fraction, Int64.random(in: Int64.min...Int64.max))
  }

  /// Returns a new ``Dataset`` by sampling a fraction of rows (without replacement), using a
  /// user-supplied seed.
  /// - Parameters:
  ///   - fraction: Fraction of rows to generate, range [0.0, 1.0].
  ///   - seed: Seed for sampling.
  /// - Returns: A subset of the records.
  public func sample(_ fraction: Double, _ seed: Int64) -> DataFrame {
    return sample(false, fraction, seed)
  }

  /// Returns a new ``Dataset`` by sampling a fraction of rows (without replacement), using a
  /// random seed.
  /// - Parameters:
  ///   - fraction: Fraction of rows to generate, range [0.0, 1.0].
  /// - Returns: A subset of the records.
  public func sample(_ fraction: Double) -> DataFrame {
    return sample(false, fraction)
  }

  // MARK: - Join Operations

  /// Join with another DataFrame.
  ///
  /// This performs an inner join and requires a subsequent join predicate.
  /// For other join types, use the overloaded methods with join type parameter.
  ///
  /// ```swift
  /// // Basic join (requires join condition)
  /// let joined = df1.join(df2)
  ///     .where("df1.id = df2.user_id")
  ///
  /// // Join with condition
  /// let result = users.join(orders, "id")
  /// ```
  ///
  /// - Parameter right: The DataFrame to join with
  /// - Returns: A new DataFrame representing the join result
  public func join(_ right: DataFrame) async -> DataFrame {
    let right = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getJoin(self.plan.root, right, JoinType.inner)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Equi-join with another DataFrame using the given column.
  ///
  /// This method performs an equi-join on a single column that exists in both DataFrames.
  ///
  /// ```swift
  /// // Inner join on a single column
  /// let joined = users.join(orders, "user_id")
  ///
  /// // Left outer join
  /// let leftJoined = users.join(orders, "user_id", "left")
  ///
  /// // Other join types: "inner", "outer", "left", "right", "semi", "anti"
  /// ```
  ///
  /// - Parameters:
  ///   - right: The DataFrame to join with
  ///   - usingColumn: Column name that exists in both DataFrames
  ///   - joinType: Type of join (default: "inner")
  /// - Returns: A new DataFrame with the join result
  public func join(_ right: DataFrame, _ usingColumn: String, _ joinType: String = "inner") async
    -> DataFrame
  {
    await join(right, [usingColumn], joinType)
  }

  /// Inner equi-join with another `DataFrame` using the given columns.
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - usingColumn: Names of the columns to join on. These columns must exist on both sides.
  ///   - joinType: A join type name.
  /// - Returns: A `DataFrame`.
  public func join(_ other: DataFrame, _ usingColumns: [String], _ joinType: String = "inner") async
    -> DataFrame
  {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getJoin(
      self.plan.root,
      right,
      joinType.toJoinType,
      usingColumns: usingColumns
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Inner equi-join with another `DataFrame` using the given columns.
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinExprs:A join expression string.
  /// - Returns: A `DataFrame`.
  public func join(_ right: DataFrame, joinExprs: String) async -> DataFrame {
    return await join(right, joinExprs: joinExprs, joinType: "inner")
  }

  /// Inner equi-join with another `DataFrame` using the given columns.
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinExprs:A join expression string.
  ///   - joinType: A join type name.
  /// - Returns: A `DataFrame`.
  public func join(_ right: DataFrame, joinExprs: String, joinType: String) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getJoin(
      self.plan.root,
      rightPlan,
      joinType.toJoinType,
      joinCondition: joinExprs
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Explicit cartesian join with another `DataFrame`.
  /// - Parameter right: Right side of the join operation.
  /// - Returns: A `DataFrame`.
  public func crossJoin(_ right: DataFrame) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getJoin(self.plan.root, rightPlan, JoinType.cross)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Lateral join with another ``DataFrame``.
  ///
  /// Behaves as an JOIN LATERAL.
  ///
  /// - Parameters:
  ///   - right: Right side of the join operation.
  /// - Returns: A ``DataFrame``.
  public func lateralJoin(_ right: DataFrame) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getLateralJoin(
      self.plan.root,
      rightPlan,
      JoinType.inner
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Lateral join with another ``DataFrame``.
  ///
  /// Behaves as an JOIN LATERAL.
  ///
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinType: One of `inner` (default), `cross`, `left`, `leftouter`, `left_outer`.
  /// - Returns: A ``DataFrame``.
  public func lateralJoin(_ right: DataFrame, joinType: String) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getLateralJoin(
      self.plan.root,
      rightPlan,
      joinType.toJoinType
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Lateral join with another ``DataFrame``.
  ///
  /// Behaves as an JOIN LATERAL.
  ///
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinExprs: A join expression string.
  /// - Returns: A ``DataFrame``.
  public func lateralJoin(_ right: DataFrame, joinExprs: String) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getLateralJoin(
      self.plan.root,
      rightPlan,
      JoinType.inner,
      joinCondition: joinExprs
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Lateral join with another ``DataFrame``.
  ///
  /// Behaves as an JOIN LATERAL.
  ///
  /// - Parameters:
  ///   - right: Right side of the join operation.
  ///   - joinType: One of `inner` (default), `cross`, `left`, `leftouter`, `left_outer`.
  ///   - joinExprs: A join expression string.
  /// - Returns: A ``DataFrame``.
  public func lateralJoin(
    _ right: DataFrame, joinExprs: String, joinType: String = "inner"
  ) async -> DataFrame {
    let rightPlan = await (right.getPlan() as! Plan).root
    let plan = SparkConnectClient.getLateralJoin(
      self.plan.root,
      rightPlan,
      joinType.toJoinType,
      joinCondition: joinExprs
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  // MARK: - Set Operations

  /// Returns a new `DataFrame` containing rows in this `DataFrame` but not in another `DataFrame`.
  /// This is equivalent to `EXCEPT DISTINCT` in SQL.
  /// - Parameter other: A `DataFrame` to exclude.
  /// - Returns: A `DataFrame`.
  public func except(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(self.plan.root, right, SetOpType.except)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing rows in this `DataFrame` but not in another `DataFrame` while
  /// preserving the duplicates. This is equivalent to `EXCEPT ALL` in SQL.
  /// - Parameter other: A `DataFrame` to exclude.
  /// - Returns: A `DataFrame`.
  public func exceptAll(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(
      self.plan.root, right, SetOpType.except, isAll: true)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing rows only in both this `DataFrame` and another `DataFrame`.
  /// This is equivalent to `INTERSECT` in SQL.
  /// - Parameter other: A `DataFrame` to intersect with.
  /// - Returns: A `DataFrame`.
  public func intersect(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(self.plan.root, right, SetOpType.intersect)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing rows only in both this `DataFrame` and another `DataFrame` while
  /// preserving the duplicates. This is equivalent to `INTERSECT ALL` in SQL.
  /// - Parameter other: A `DataFrame` to intersect with.
  /// - Returns: A `DataFrame`.
  public func intersectAll(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(
      self.plan.root, right, SetOpType.intersect, isAll: true)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing union of rows in this `DataFrame` and another `DataFrame`.
  /// This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union (that does
  /// deduplication of elements), use this function followed by a [[distinct]].
  /// Also as standard in SQL, this function resolves columns by position (not by name)
  /// - Parameter other: A `DataFrame` to union with.
  /// - Returns: A `DataFrame`.
  public func union(_ other: DataFrame) async -> DataFrame {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(
      self.plan.root, right, SetOpType.union, isAll: true)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new `DataFrame` containing union of rows in this `DataFrame` and another `DataFrame`.
  /// This is an alias of `union`.
  /// - Parameter other: A `DataFrame` to union with.
  /// - Returns: A `DataFrame`.
  public func unionAll(_ other: DataFrame) async -> DataFrame {
    return await union(other)
  }

  /// Returns a new `DataFrame` containing union of rows in this `DataFrame` and another `DataFrame`.
  /// The difference between this function and [[union]] is that this function resolves columns by
  /// name (not by position).
  /// When the parameter `allowMissingColumns` is `true`, the set of column names in this and other
  /// `DataFrame` can differ; missing columns will be filled with null. Further, the missing columns
  /// of this `DataFrame` will be added at the end in the schema of the union result
  /// - Parameter other: A `DataFrame` to union with.
  /// - Returns: A `DataFrame`.
  public func unionByName(_ other: DataFrame, _ allowMissingColumns: Bool = false) async
    -> DataFrame
  {
    let right = await (other.getPlan() as! Plan).root
    let plan = SparkConnectClient.getSetOperation(
      self.plan.root,
      right,
      SetOpType.union,
      isAll: true,
      byName: true,
      allowMissingColumns: allowMissingColumns
    )
    return DataFrame(spark: self.spark, plan: plan)
  }

  // MARK: - Partitioning

  private func buildRepartition(numPartitions: Int32, shuffle: Bool) -> DataFrame {
    let plan = SparkConnectClient.getRepartition(self.plan.root, numPartitions, shuffle)
    return DataFrame(spark: self.spark, plan: plan)
  }

  private func buildRepartitionByExpression(numPartitions: Int32?, partitionExprs: [String])
    -> DataFrame
  {
    let plan = SparkConnectClient.getRepartitionByExpression(
      self.plan.root, partitionExprs, numPartitions)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Returns a new ``DataFrame`` that has exactly `numPartitions` partitions.
  /// - Parameter numPartitions: The number of partitions.
  /// - Returns: A `DataFrame`.
  public func repartition(_ numPartitions: Int32) -> DataFrame {
    return buildRepartition(numPartitions: numPartitions, shuffle: true)
  }

  /// Returns a new ``DataFrame`` partitioned by the given partitioning expressions, using
  /// `spark.sql.shuffle.partitions` as number of partitions. The resulting Dataset is hash
  /// partitioned.
  /// - Parameter partitionExprs: The partition expression strings.
  /// - Returns: A `DataFrame`.
  public func repartition(_ partitionExprs: String...) -> DataFrame {
    return buildRepartitionByExpression(numPartitions: nil, partitionExprs: partitionExprs)
  }

  /// Returns a new ``DataFrame`` partitioned by the given partitioning expressions, using
  /// `spark.sql.shuffle.partitions` as number of partitions. The resulting Dataset is hash
  /// partitioned.
  /// - Parameters:
  ///   - numPartitions: The number of partitions.
  ///   - partitionExprs: The partition expression strings.
  /// - Returns: A `DataFrame`.
  public func repartition(_ numPartitions: Int32, _ partitionExprs: String...) -> DataFrame {
    return buildRepartitionByExpression(
      numPartitions: numPartitions, partitionExprs: partitionExprs)
  }

  /// Returns a new ``DataFrame`` partitioned by the given partitioning expressions, using
  /// `spark.sql.shuffle.partitions` as number of partitions. The resulting Dataset is hash
  /// partitioned.
  /// - Parameter partitionExprs: The partition expression strings.
  /// - Returns: A `DataFrame`.
  public func repartitionByExpression(_ numPartitions: Int32?, _ partitionExprs: String...)
    -> DataFrame
  {
    return buildRepartitionByExpression(
      numPartitions: numPartitions, partitionExprs: partitionExprs)
  }

  /// Returns a new ``DataFrame`` that has exactly `numPartitions` partitions, when the fewer partitions
  /// are requested. If a larger number of partitions is requested, it will stay at the current
  /// number of partitions. Similar to coalesce defined on an `RDD`, this operation results in a
  /// narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a
  /// shuffle, instead each of the 100 new partitions will claim 10 of the current partitions.
  /// - Parameter numPartitions: The number of partitions.
  /// - Returns: A `DataFrame`.
  public func coalesce(_ numPartitions: Int32) -> DataFrame {
    return buildRepartition(numPartitions: numPartitions, shuffle: false)
  }

  // MARK: - Data Shape

  /// Returns a new ``Dataset`` that contains only the unique rows from this ``Dataset``.
  /// This is an alias for `dropDuplicates`.
  /// - Returns: A `DataFrame`.
  public func distinct() -> DataFrame {
    return dropDuplicates()
  }

  /// Transposes a DataFrame, switching rows to columns. This function transforms the DataFrame
  /// such that the values in the first column become the new columns of the DataFrame.
  /// - Returns: A transposed ``DataFrame``.
  public func transpose() -> DataFrame {
    return buildTranspose([])
  }

  /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
  /// set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
  /// which cannot be reversed. This is an alias for `unpivot`.
  /// - Parameters:
  ///   - ids: ID column names
  ///   - values: Value column names to unpivot
  ///   - variableColumnName: Name of the variable column
  ///   - valueColumnName: Name of the value column
  /// - Returns: A ``DataFrame``.
  public func melt(
    _ ids: [String],
    _ values: [String],
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    return unpivot(ids, values, variableColumnName, valueColumnName)
  }

  /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
  /// set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
  /// which cannot be reversed. This is an alias for `unpivot`.
  /// - Parameters:
  ///   - ids: ID column names
  ///   - variableColumnName: Name of the variable column
  ///   - valueColumnName: Name of the value column
  /// - Returns: A ``DataFrame``.
  public func melt(
    _ ids: [String],
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    return unpivot(ids, variableColumnName, valueColumnName)
  }

  /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
  /// set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
  /// which cannot be reversed.
  /// - Parameters:
  ///   - ids: ID column names
  ///   - values: Value column names to unpivot
  ///   - variableColumnName: Name of the variable column
  ///   - valueColumnName: Name of the value column
  /// - Returns: A ``DataFrame``.
  public func unpivot(
    _ ids: [String],
    _ values: [String],
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    return buildUnpivot(ids, values, variableColumnName, valueColumnName)
  }

  /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns
  /// set. This is the reverse to `groupBy(...).pivot(...).agg(...)`, except for the aggregation,
  /// which cannot be reversed.
  /// - Parameters:
  ///   - ids: ID column names
  ///   - variableColumnName: Name of the variable column
  ///   - valueColumnName: Name of the value column
  /// - Returns: A ``DataFrame``.
  public func unpivot(
    _ ids: [String],
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    return buildUnpivot(ids, nil, variableColumnName, valueColumnName)
  }

  func buildUnpivot(
    _ ids: [String],
    _ values: [String]?,
    _ variableColumnName: String,
    _ valueColumnName: String
  ) -> DataFrame {
    let plan = SparkConnectClient.getUnpivot(
      self.plan.root, ids, values, variableColumnName, valueColumnName)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Transposes a ``DataFrame`` such that the values in the specified index column become the new
  /// columns of the ``DataFrame``.
  /// - Parameter indexColumn: The single column that will be treated as the index for the transpose operation.
  /// This column will be used to pivot the data, transforming the DataFrame such that the values of
  /// the indexColumn become the new columns in the transposed DataFrame.
  /// - Returns: A transposed ``DataFrame``.
  public func transpose(_ indexColumn: String) -> DataFrame {
    return buildTranspose([indexColumn])
  }

  func buildTranspose(_ indexColumn: [String]) -> DataFrame {
    let plan = SparkConnectClient.getTranspose(self.plan.root, indexColumn)
    return DataFrame(spark: self.spark, plan: plan)
  }

  // MARK: - Grouping

  /// Groups the DataFrame using the specified columns.
  ///
  /// This method is used to perform aggregations on groups of data.
  /// After grouping, you can apply aggregation functions like count(), sum(), avg(), etc.
  ///
  /// ```swift
  /// // Group by single column
  /// let byDept = df.groupBy("department")
  ///     .agg(count("*").alias("employee_count"))
  ///
  /// // Group by multiple columns
  /// let byDeptAndLocation = df.groupBy("department", "location")
  ///     .agg(
  ///         avg("salary").alias("avg_salary"),
  ///         max("salary").alias("max_salary")
  ///     )
  /// ```
  ///
  /// - Parameter cols: Column names to group by
  /// - Returns: A ``GroupedData`` object for aggregation operations
  public func groupBy(_ cols: String...) -> GroupedData {
    return GroupedData(self, GroupType.groupby, cols)
  }

  /// Create a multi-dimensional rollup for the current ``DataFrame`` using the specified columns, so we
  /// can run aggregation on them.
  /// - Parameter cols: Grouping column names.
  /// - Returns: A ``GroupedData``.
  public func rollup(_ cols: String...) -> GroupedData {
    return GroupedData(self, GroupType.rollup, cols)
  }

  /// Create a multi-dimensional cube for the current ``DataFrame`` using the specified columns, so we
  /// can run aggregation on them.
  /// - Parameter cols: Grouping column names.
  /// - Returns: A ``GroupedData``.
  public func cube(_ cols: String...) -> GroupedData {
    return GroupedData(self, GroupType.cube, cols)
  }

  // MARK: - Hints and Watermark

  /// Specifies some hint on the current Dataset.
  /// - Parameters:
  ///   - name: The hint name.
  ///   - parameters: The parameters of the hint
  /// - Returns: A ``DataFrame``.
  @discardableResult
  public func hint(_ name: String, _ parameters: Sendable...) -> DataFrame {
    let plan = SparkConnectClient.getHint(self.plan.root, name, parameters)
    return DataFrame(spark: self.spark, plan: plan)
  }

  /// Defines an event time watermark for this ``DataFrame``.  A watermark tracks a point in time
  /// before which we assume no more late data is going to arrive.
  /// - Parameters:
  ///   - eventTime: the name of the column that contains the event time of the row.
  ///   - delayThreshold: the minimum delay to wait to data to arrive late, relative to
  ///   the latest record that has been processed in the form of an interval (e.g. "1 minute" or "5 hours").
  ///   NOTE: This should not be negative.
  /// - Returns: A ``DataFrame`` instance.
  public func withWatermark(_ eventTime: String, _ delayThreshold: String) -> DataFrame {
    let plan = SparkConnectClient.getWithWatermark(self.plan.root, eventTime, delayThreshold)
    return DataFrame(spark: self.spark, plan: plan)
  }
}
