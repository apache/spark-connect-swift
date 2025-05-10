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
print("Connected to Apache Spark \(await spark.version) Server")

let statements = [
  "DROP TABLE IF EXISTS t",
  "CREATE TABLE IF NOT EXISTS t(a INT) USING ORC",
  "INSERT INTO t VALUES (1), (2), (3)",
]

for s in statements {
  print("EXECUTE: \(s)")
  _ = try await spark.sql(s).count()
}
print("SELECT * FROM t")
try await spark.sql("SELECT * FROM t").cache().show()

try await spark.range(10).filter("id % 2 == 0").write.mode("overwrite").orc("/tmp/orc")
try await spark.read.orc("/tmp/orc").show()

await spark.stop()
