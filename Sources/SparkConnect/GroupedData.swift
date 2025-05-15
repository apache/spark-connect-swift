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

public actor GroupedData {
  let df: DataFrame
  let groupType: GroupType
  let groupingCols: [String]

  init(_ df: DataFrame, _ groupType: GroupType, _ groupingCols: [String]) {
    self.df = df
    self.groupType = groupType
    self.groupingCols = groupingCols
  }

  public func agg(_ exprs: String...) async -> DataFrame {
    var aggregate = Aggregate()
    aggregate.input = await (self.df.getPlan() as! Plan).root
    aggregate.groupType = self.groupType
    aggregate.groupingExpressions = self.groupingCols.map { $0.toExpression }
    aggregate.aggregateExpressions = exprs.map { $0.toExpression }
    var relation = Relation()
    relation.aggregate = aggregate
    var plan = Plan()
    plan.opType = .root(relation)
    return await DataFrame(spark: df.spark, plan: plan)
  }
}
