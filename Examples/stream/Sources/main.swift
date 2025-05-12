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

let spark = try await SparkSession.builder.getOrCreate()
print("Connected to Apache Spark \(await spark.version) Server")

let host = ProcessInfo.processInfo.environment["TARGET_HOST"] ?? "localhost"

let lines =
  await spark
  .readStream
  .format("socket")
  .option("host", host)
  .option("port", 9999)
  .load()

let word =
  await lines
  .selectExpr("explode(split(value, ' ')) as word")

let wordCounts =
  await word
  .groupBy("word")
  .agg("count(*)")

let query =
  try await wordCounts
  .writeStream
  .outputMode("complete")
  .format("console")
  .start()

_ = try await query.awaitTermination()
