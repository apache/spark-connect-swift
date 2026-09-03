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

/// Returns whether `version` is greater than or equal to `target`.
///
/// The versions are compared component by component numerically, so this is correct where
/// the lexicographical `String` comparison is not, e.g. `"4.10" >= "4.2"` is `false` while
/// `isSparkVersionAtLeast("4.10", "4.2")` is `true`.
///
/// A missing component is treated as zero (`"4.2"` and `"4.2.0"` are equivalent) and a
/// non-numeric suffix is ignored (`"4.3.0-SNAPSHOT"` is treated as `"4.3.0"`).
func isSparkVersionAtLeast(_ version: String, _ target: String) -> Bool {
  let lhs = numericComponents(version)
  let rhs = numericComponents(target)
  for i in 0..<max(lhs.count, rhs.count) {
    let l = i < lhs.count ? lhs[i] : 0
    let r = i < rhs.count ? rhs[i] : 0
    if l != r {
      return l > r
    }
  }
  return true
}

/// Returns the leading numeric components of a version string, e.g. `[4, 3, 0]` for
/// `"4.3.0-SNAPSHOT"`.
private func numericComponents(_ version: String) -> [Int] {
  version
    .prefix(while: { $0.isNumber || $0 == "." })
    .split(separator: ".")
    .map { Int($0) ?? 0 }
}
