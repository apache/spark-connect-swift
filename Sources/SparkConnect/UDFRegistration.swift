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

/// Functions for registering user-defined functions. To access this, use ``SparkSession/udf``.
public actor UDFRegistration: Sendable {
  var spark: SparkSession

  init(spark: SparkSession) {
    self.spark = spark
  }

  /// Register a Java user-defined function as a SQL function.
  ///
  /// The class must directly implement one of the `UDF0` ... `UDF22` interfaces (an interface
  /// inherited from a superclass is not detected), have a public no-arg constructor, and be
  /// available in the server's classpath (e.g., via ``SparkSession/addArtifact(_:)-(URL)``).
  ///
  /// ```swift
  /// try await spark.udf.registerJava("sconcat", "com.example.StringConcat", .string)
  /// try await spark.sql("SELECT sconcat('a', 'b')").show()
  /// ```
  ///
  /// - Parameters:
  ///   - name: The name of the function.
  ///   - className: The fully qualified class name of the Java UDF.
  ///   - returnType: The return type of the UDF. If `nil`, the server infers it via reflection.
  public func registerJava(_ name: String, _ className: String, _ returnType: DataType? = nil)
    async throws
  {
    try await spark.client.registerJava(name, className, returnType)
  }
}
