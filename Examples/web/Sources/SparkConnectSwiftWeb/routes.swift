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
import Vapor

func routes(_ app: Application) throws {
  app.get { req async in
    "Welcome to the Swift world. Say hello!"
  }

  app.get("hello") { req async -> String in
    return await Task {
      do {
        let spark = try await SparkSession.builder.getOrCreate()
        let response = "Hi, this is powered by the Apache Spark \(await spark.version)."
        await spark.stop()
        return response
      } catch {
        return "Fail to connect: \(error)"
      }
    }.value
  }
}
