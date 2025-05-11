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

import SparkConnect

let spark = try await SparkSession.builder.getOrCreate()

let n: Int64 = CommandLine.arguments.count > 1 ? Int64(CommandLine.arguments[1])! : 1_000_000

let count =
  try await spark
  .range(n)
  .selectExpr("(pow(rand() * 2 - 1, 2) + pow(rand() * 2 - 1, 2)) as v")
  .where("v <= 1")
  .count()

print("Pi is roughly \(4.0 * Double(count) / (Double(n) - 1))")

await spark.stop()
