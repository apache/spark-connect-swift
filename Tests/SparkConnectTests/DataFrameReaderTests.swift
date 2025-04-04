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
import Testing

@testable import SparkConnect

/// A test suite for `DataFrameReader`
struct DataFrameReaderTests {

  @Test
  func csv() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let path = "../examples/src/main/resources/people.csv"
    #expect(try await spark.read.format("csv").load(path).count() == 3)
    #expect(try await spark.read.csv(path).count() == 3)
    #expect(try await spark.read.csv(path, path).count() == 6)
    await spark.stop()
  }

  @Test
  func json() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let path = "../examples/src/main/resources/people.json"
    #expect(try await spark.read.format("json").load(path).count() == 3)
    #expect(try await spark.read.json(path).count() == 3)
    #expect(try await spark.read.json(path, path).count() == 6)
    await spark.stop()
  }

  @Test
  func orc() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let path = "../examples/src/main/resources/users.orc"
    #expect(try await spark.read.format("orc").load(path).count() == 2)
    #expect(try await spark.read.orc(path).count() == 2)
    #expect(try await spark.read.orc(path, path).count() == 4)
    await spark.stop()
  }

  @Test
  func parquet() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let path = "../examples/src/main/resources/users.parquet"
    #expect(try await spark.read.format("parquet").load(path).count() == 2)
    #expect(try await spark.read.parquet(path).count() == 2)
    #expect(try await spark.read.parquet(path, path).count() == 4)
    await spark.stop()
  }
}
