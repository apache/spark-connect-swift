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

/// A struct for defining actions to be taken when matching rows in a ``DataFrame``
/// during a merge operation.
public struct WhenMatched: Sendable {
  let mergeIntoWriter: MergeIntoWriter
  let condition: String?

  init(_ mergeIntoWriter: MergeIntoWriter, _ condition: String? = nil) {
    self.mergeIntoWriter = mergeIntoWriter
    self.condition = condition
  }

  /// Specifies an action to update all matched rows in the ``DataFrame``.
  /// - Returns: The ``MergeIntoWriter`` instance with the update all action configured.
  public func updateAll() async -> MergeIntoWriter {
    await mergeIntoWriter.updateAll(condition, false)
  }

  /// Specifies an action to update matched rows in the ``DataFrame`` with the provided column
  /// assignments.
  /// - Parameter map: A dictionary from column names to expressions representing the updates to be applied.
  /// - Returns: The ``MergeIntoWriter`` instance with the update action configured.
  public func update(map: [String: String]) async -> MergeIntoWriter {
    await mergeIntoWriter.update(condition, map, false)
  }

  /// Specifies an action to delete matched rows from the DataFrame.
  /// - Returns: The ``MergeIntoWriter`` instance with the delete action configured.
  public func delete() async -> MergeIntoWriter {
    await mergeIntoWriter.delete(condition, false)
  }
}

/// A struct for defining actions to be taken when no matching rows are found in a ``DataFrame``
/// during a merge operation.
public struct WhenNotMatched: Sendable {
  let mergeIntoWriter: MergeIntoWriter
  let condition: String?

  init(_ mergeIntoWriter: MergeIntoWriter, _ condition: String? = nil) {
    self.mergeIntoWriter = mergeIntoWriter
    self.condition = condition
  }

  /// Specifies an action to insert all non-matched rows into the ``DataFrame``.
  /// - Returns: The`` MergeIntoWriter`` instance with the insert all action configured.
  public func insertAll() async -> MergeIntoWriter {
    await mergeIntoWriter.insertAll(condition)
  }

  /// Specifies an action to insert non-matched rows into the ``DataFrame``
  /// with the provided column assignments.
  /// - Parameter map: A dictionary of column names to expressions representing the values to be inserted.
  /// - Returns: The ``MergeIntoWriter`` instance with the insert action configured.
  public func insert(_ map: [String: String]) async -> MergeIntoWriter {
    await mergeIntoWriter.insert(condition, map)
  }
}

/// A struct for defining actions to be performed when there is no match by source during a merge
/// operation in a ``MergeIntoWriter``.
public struct WhenNotMatchedBySource: Sendable {
  let mergeIntoWriter: MergeIntoWriter
  let condition: String?

  init(_ mergeIntoWriter: MergeIntoWriter, _ condition: String? = nil) {
    self.mergeIntoWriter = mergeIntoWriter
    self.condition = condition
  }

  /// Specifies an action to update all non-matched rows in the target ``DataFrame``
  /// when not matched by the source.
  /// - Returns: A ``MergeIntoWriter`` instance.
  public func updateAll() async -> MergeIntoWriter {
    await mergeIntoWriter.updateAll(condition, true)
  }

  /// Specifies an action to update non-matched rows in the target ``DataFrame``
  /// with the provided column assignments when not matched by the source.
  /// - Parameter map: A dictionary from column names to expressions representing the updates to be applied
  /// - Returns: A ``MergeIntoWriter`` instance.
  public func update(map: [String: String]) async -> MergeIntoWriter {
    await mergeIntoWriter.update(condition, map, true)
  }

  /// Specifies an action to delete non-matched rows from the target ``DataFrame``
  /// when not matched by the source.
  /// - Returns: A ``MergeIntoWriter`` instance.
  public func delete() async -> MergeIntoWriter {
    await mergeIntoWriter.delete(condition, true)
  }
}

/// `MergeIntoWriter` provides methods to define and execute merge actions based on specified
/// conditions.
public actor MergeIntoWriter {
  var schemaEvolution: Bool = false

  let table: String

  let df: DataFrame

  let condition: String

  var mergeIntoTableCommand = MergeIntoTableCommand()

  init(_ table: String, _ df: DataFrame, _ condition: String) {
    self.table = table
    self.df = df
    self.condition = condition

    self.mergeIntoTableCommand.targetTableName = table
    self.mergeIntoTableCommand.mergeCondition.expressionString = condition.toExpressionString
  }

  public var schemaEvolutionEnabled: Bool {
    schemaEvolution
  }

  /// Enable automatic schema evolution for this merge operation.
  /// - Returns: ``MergeIntoWriter`` instance
  public func withSchemaEvolution() -> MergeIntoWriter {
    self.schemaEvolution = true
    return self
  }

  /// Initialize a `WhenMatched` action without any condition.
  /// - Returns: A `WhenMatched` instance.
  public func whenMatched() -> WhenMatched {
    WhenMatched(self)
  }

  /// Initialize a `WhenMatched` action with a condition.
  /// - Parameter condition: The condition to be evaluated for the action.
  /// - Returns: A `WhenMatched` instance configured with the specified condition.
  public func whenMatched(_ condition: String) -> WhenMatched {
    WhenMatched(self, condition)
  }

  /// Initialize a `WhenNotMatched` action without any condition.
  /// - Returns: A `WhenNotMatched` instance.
  public func whenNotMatched() -> WhenNotMatched {
    WhenNotMatched(self)
  }

  /// Initialize a `WhenNotMatched` action with a condition.
  /// - Parameter condition: The condition to be evaluated for the action.
  /// - Returns: A `WhenNotMatched` instance configured with the specified condition.
  public func whenNotMatched(_ condition: String) -> WhenNotMatched {
    WhenNotMatched(self, condition)
  }

  /// Initialize a `WhenNotMatchedBySource` action without any condition.
  /// - Returns: A `WhenNotMatchedBySource` instance.
  public func whenNotMatchedBySource() -> WhenNotMatchedBySource {
    WhenNotMatchedBySource(self)
  }

  /// Initialize a `WhenNotMatchedBySource` action with a condition
  /// - Parameter condition: The condition to be evaluated for the action.
  /// - Returns: A `WhenNotMatchedBySource` instance configured with the specified condition.
  public func whenNotMatchedBySource(_ condition: String) -> WhenNotMatchedBySource {
    WhenNotMatchedBySource(self, condition)
  }

  /// Executes the merge operation.
  public func merge() async throws {
    if self.mergeIntoTableCommand.matchActions.count == 0
      && self.mergeIntoTableCommand.notMatchedActions.count == 0
      && self.mergeIntoTableCommand.notMatchedBySourceActions.count == 0
    {
      throw SparkConnectError.InvalidArgumentException
    }
    self.mergeIntoTableCommand.sourceTablePlan = await (self.df.getPlan() as! Plan).root
    self.mergeIntoTableCommand.withSchemaEvolution = self.schemaEvolution

    var command = Spark_Connect_Command()
    command.mergeIntoTableCommand = self.mergeIntoTableCommand
    _ = try await df.spark.client.execute(df.spark.sessionID, command)
  }

  public func insertAll(_ condition: String?) -> MergeIntoWriter {
    let expression = buildMergeAction(ActionType.insertStar, condition)
    self.mergeIntoTableCommand.notMatchedActions.append(expression)
    return self
  }

  public func insert(_ condition: String?, _ map: [String: String]) -> MergeIntoWriter {
    let expression = buildMergeAction(ActionType.insert, condition, map)
    self.mergeIntoTableCommand.notMatchedActions.append(expression)
    return self
  }

  public func updateAll(_ condition: String?, _ notMatchedBySource: Bool) -> MergeIntoWriter {
    appendUpdateDeleteAction(buildMergeAction(ActionType.updateStar, condition), notMatchedBySource)
  }

  public func update(_ condition: String?, _ map: [String: String], _ notMatchedBySource: Bool)
    -> MergeIntoWriter
  {
    appendUpdateDeleteAction(buildMergeAction(ActionType.update, condition), notMatchedBySource)
  }

  public func delete(_ condition: String?, _ notMatchedBySource: Bool) -> MergeIntoWriter {
    appendUpdateDeleteAction(buildMergeAction(ActionType.delete, condition), notMatchedBySource)
  }

  private func appendUpdateDeleteAction(
    _ action: Spark_Connect_Expression,
    _ notMatchedBySource: Bool
  ) -> MergeIntoWriter {
    if notMatchedBySource {
      self.mergeIntoTableCommand.notMatchedBySourceActions.append(action)
    } else {
      self.mergeIntoTableCommand.matchActions.append(action)
    }
    return self
  }

  private func buildMergeAction(
    _ actionType: ActionType,
    _ condition: String?,
    _ assignments: [String: String] = [:]
  ) -> Spark_Connect_Expression {
    var mergeAction = Spark_Connect_MergeAction()
    mergeAction.actionType = actionType
    if let condition {
      var expression = Spark_Connect_Expression()
      expression.expressionString = condition.toExpressionString
      mergeAction.condition = expression
    }
    mergeAction.assignments = assignments.map { key, value in
      var keyExpr = Spark_Connect_Expression()
      var valueExpr = Spark_Connect_Expression()

      keyExpr.expressionString = key.toExpressionString
      valueExpr.expressionString = value.toExpressionString

      var assignment = MergeAction.Assignment()
      assignment.key = keyExpr
      assignment.value = valueExpr
      return assignment
    }
    var expression = Spark_Connect_Expression()
    expression.mergeAction = mergeAction
    return expression
  }
}
