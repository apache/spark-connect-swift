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
import GRPCCore

struct BearerTokenInterceptor: ClientInterceptor {
  let token: String

  init(token: String) {
    self.token = token
  }

  func intercept<Input: Sendable, Output: Sendable>(
    request: StreamingClientRequest<Input>,
    context: ClientContext,
    next: (
      _ request: StreamingClientRequest<Input>,
      _ context: ClientContext
    ) async throws -> StreamingClientResponse<Output>
  ) async throws -> StreamingClientResponse<Output> {
    var request = request
    request.metadata.addString("Bearer \(self.token)", forKey: "Authorization")

    // Forward the request to the next interceptor.
    return try await next(request, context)
  }
}
