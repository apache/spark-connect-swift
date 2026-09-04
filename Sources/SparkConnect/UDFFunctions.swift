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

// MARK: - UDF functions

/// Calls a SQL function.
/// - Parameters:
///   - funcName: A function name that follows the SQL identifier syntax
///   (can be quoted, can be qualified).
///   - cols: The expression parameters of the function.
/// - Returns: A ``Column``.
public func call_function(_ funcName: String, _ cols: Column...) -> Column {
  return callFunction(funcName, cols)
}

/// Calls a user-defined function registered by SQL `CREATE FUNCTION`.
/// This is an alias of ``call_function(_:_:)``.
/// - Parameters:
///   - udfName: A user-defined function name that follows the SQL identifier syntax
///   (can be quoted, can be qualified).
///   - cols: The expression parameters of the function.
/// - Returns: A ``Column``.
public func call_udf(_ udfName: String, _ cols: Column...) -> Column {
  return callFunction(udfName, cols)
}

private func callFunction(_ funcName: String, _ cols: [Column]) -> Column {
  var function = Spark_Connect_CallFunction()
  function.functionName = funcName
  function.arguments = cols.map { $0.expr }
  var expr = Spark_Connect_Expression()
  expr.callFunction = function
  return Column(expr)
}
