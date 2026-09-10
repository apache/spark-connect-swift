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

import Testing

@testable import SparkConnect

/// A test suite for `DataFrame` internal APIs
@Suite(.serialized)
struct DataFrameInternalTests {

  @Test
  func showString() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(10).showString(2, 0, false).collect()
    #expect(rows.count == 1)
    #expect(rows[0].length == 1)
    #expect(
      try (rows[0].get(0) as! String).trimmingCharacters(in: .whitespacesAndNewlines) == """
        +---+
        |id |
        +---+
        |0  |
        |1  |
        +---+
        only showing top 2 rows
        """)
    await spark.stop()
  }

  @Test
  func showStringTruncate() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.sql("SELECT * FROM VALUES ('abc', 'def'), ('ghi', 'jkl')")
      .showString(2, 2, false).collect()
    #expect(rows.count == 1)
    #expect(rows[0].length == 1)
    print(try rows[0].get(0) as! String)
    #expect(
      try rows[0].get(0) as! String == """
        +----+----+
        |col1|col2|
        +----+----+
        |  ab|  de|
        |  gh|  jk|
        +----+----+

        """)
    await spark.stop()
  }

  @Test
  func showStringVertical() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(10).showString(2, 0, true).collect()
    #expect(rows.count == 1)
    #expect(rows[0].length == 1)
    print(try rows[0].get(0) as! String)
    #expect(
      try (rows[0].get(0) as! String).trimmingCharacters(in: .whitespacesAndNewlines) == """
        -RECORD 0--
         id  | 0   
        -RECORD 1--
         id  | 1   
        only showing top 2 rows
        """)
    await spark.stop()
  }

  @Test
  func groupingSetsPlan() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let range = try await spark.range(1)
    let df = await range.groupingSets([["id"], []], "id").agg("count(*)")
    let aggregate = await df.plan.root.aggregate
    #expect(aggregate.groupType == .groupingSets)
    #expect(aggregate.groupingExpressions.map { $0.expressionString.expression } == ["id"])
    #expect(aggregate.groupingSets.count == 2)
    #expect(aggregate.groupingSets[0].groupingSet.map { $0.expressionString.expression } == ["id"])
    #expect(aggregate.groupingSets[1].groupingSet.isEmpty)
    await spark.stop()
  }

  @Test
  func colRegexExpression() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let regex = df.colRegex("`a.*`").expr.unresolvedRegex
    #expect(regex.colName == "`a.*`")
    #expect(!regex.hasPlanID)
    await spark.stop()
  }

  @Test
  func planID() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df1 = try await spark.range(1)
    let df2 = try await spark.sql("SELECT 1")
    let df3 = await df1.select("id")
    let common1 = await df1.plan.root.common
    let common2 = await df2.plan.root.common
    let common3 = await df3.plan.root.common
    #expect(common1.hasPlanID && common2.hasPlanID && common3.hasPlanID)
    #expect(Set([common1.planID, common2.planID, common3.planID]).count == 3)
    #expect(await df3.plan.root.project.input.common.planID == common1.planID)
    #expect(await df1.toDF().plan.root.common.planID == common1.planID)
    await spark.stop()
  }

  @Test
  func colExpression() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let planID = await df.plan.root.common.planID
    let attribute = df["id"].expr.unresolvedAttribute
    #expect(attribute.unparsedIdentifier == "id")
    #expect(attribute.planID == planID)
    #expect(df.col("id").expr == df["id"].expr)
    let star = df["*"].expr.unresolvedStar
    #expect(!star.hasUnparsedTarget)
    #expect(star.planID == planID)
    let structStar = df["s.*"].expr.unresolvedStar
    #expect(structStar.unparsedTarget == "s.*")
    #expect(!structStar.hasPlanID)
    await spark.stop()
  }

  @Test
  func removeCachedRemoteRelation() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.0.0") {
      // Disable the server-side plan cache. Otherwise, `count()` keeps succeeding with the plan
      // cached for this `DataFrame`'s plan ID even after the cached relation is removed.
      try await spark.conf.set("spark.connect.session.planCache.enabled", "false")
      let df = try await spark.range(10).localCheckpoint()
      #expect(try await df.count() == 10)
      let cachedRemoteRelation = await df.plan.root.cachedRemoteRelation
      try await spark.client.removeCachedRemoteRelation(cachedRemoteRelation)
      // The `DataFrame` is no longer queryable after the server-side cached relation is removed.
      try await #require(throws: Error.self) {
        try await df.count()
      }
    }
    await spark.stop()
  }
}
