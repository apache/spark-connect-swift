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

#if !os(Linux)
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
#endif
}
