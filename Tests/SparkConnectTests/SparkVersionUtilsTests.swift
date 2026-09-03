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

import Testing

/// A test suite for `isSparkVersionAtLeast`. This does not require a Spark Connect server.
struct SparkVersionUtilsTests {
  @Test
  func equalVersions() {
    #expect(isSparkVersionAtLeast("4.2.0", "4.2.0"))
    #expect(isSparkVersionAtLeast("4.2", "4.2.0"))
    #expect(isSparkVersionAtLeast("4.2.0", "4.2"))
    #expect(isSparkVersionAtLeast("4", "4.0.0"))
  }

  @Test
  func compareVersions() {
    #expect(isSparkVersionAtLeast("4.2.0", "4.1.0"))
    #expect(isSparkVersionAtLeast("4.2.1", "4.2.0"))
    #expect(isSparkVersionAtLeast("5.0.0", "4.0.0"))
    #expect(!isSparkVersionAtLeast("4.1.0", "4.2.0"))
    #expect(!isSparkVersionAtLeast("4.2.0", "4.2.1"))
    #expect(!isSparkVersionAtLeast("3.5.0", "4.0.0"))
  }

  @Test
  func compareVersionsNumerically() {
    #expect(isSparkVersionAtLeast("4.10", "4.2"))
    #expect(isSparkVersionAtLeast("4.10.0", "4.2.0"))
    #expect(isSparkVersionAtLeast("4.2.10", "4.2.9"))
    #expect(isSparkVersionAtLeast("10.0.0", "9.0.0"))
    #expect(!isSparkVersionAtLeast("4.2", "4.10"))
    #expect(!isSparkVersionAtLeast("4.9", "4.10"))
  }

  @Test
  func ignoreNonNumericSuffix() {
    #expect(isSparkVersionAtLeast("4.3.0-SNAPSHOT", "4.3.0"))
    #expect(isSparkVersionAtLeast("4.3.0-SNAPSHOT", "4.2.0"))
    #expect(isSparkVersionAtLeast("4.10.0-SNAPSHOT", "4.2.0"))
    #expect(isSparkVersionAtLeast("4.1.0-preview1", "4.1.0"))
    #expect(!isSparkVersionAtLeast("4.1.0-SNAPSHOT", "4.2.0"))
  }

  @Test
  func emptyVersion() {
    #expect(!isSparkVersionAtLeast("", "4.0.0"))
    #expect(isSparkVersionAtLeast("", "0"))
    #expect(isSparkVersionAtLeast("4.0.0", ""))
  }
}
