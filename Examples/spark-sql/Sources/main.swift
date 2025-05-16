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
import SparkConnect

let statement = /([^;]*);/

let spark = try await SparkSession.builder.getOrCreate()
print("Connected to Apache Spark \(await spark.version) Server")

var isRunning = true
var lines = ""
while isRunning {
  if lines.isEmpty {
    print("spark-sql (\(try await spark.catalog.currentDatabase()))> ", terminator: "")
  }
  guard let input = readLine() else {
    isRunning = false
    break
  }
  lines += input + " "

  let matches = lines.matches(of: statement)
  for match in matches {
    lines = ""
    switch match.1 {
    case "exit":
      isRunning = false
      break
    default:
      do {
        try await spark.time({ try await spark.sql(String(match.1)).show(10000, false) })
      } catch {
        print("Error: \(error)")
      }
    }
  }
}

await spark.stop()
