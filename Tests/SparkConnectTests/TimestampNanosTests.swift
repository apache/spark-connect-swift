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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import SparkConnect
import Testing

/// A test suite for ``TimestampNanos``.
struct TimestampNanosTests {
  @Test
  func components() async throws {
    let timestamp = try #require(
      TimestampNanos(epochMicros: 1_767_225_600_123_456, nanosWithinMicro: 789))
    #expect(timestamp.epochMicros == 1_767_225_600_123_456)
    #expect(timestamp.nanosWithinMicro == 789)
    #expect(TimestampNanos(epochMicros: 1)?.nanosWithinMicro == 0)
  }

  @Test
  func componentBounds() async throws {
    #expect(TimestampNanos(epochMicros: 0, nanosWithinMicro: 0) != nil)
    #expect(TimestampNanos(epochMicros: 0, nanosWithinMicro: 999) != nil)
    #expect(TimestampNanos(epochMicros: Int64.min) != nil)
    #expect(TimestampNanos(epochMicros: Int64.max) != nil)
    #expect(TimestampNanos(epochMicros: 0, nanosWithinMicro: -1) == nil)
    #expect(TimestampNanos(epochMicros: 0, nanosWithinMicro: 1_000) == nil)
  }

  @Test
  func epochNanos() async throws {
    #expect(TimestampNanos(epochNanos: 0) == TimestampNanos(epochMicros: 0))
    #expect(
      TimestampNanos(epochNanos: 1_767_225_600_123_456_789)
        == TimestampNanos(epochMicros: 1_767_225_600_123_456, nanosWithinMicro: 789))
    // Timestamps before 1970 keep `nanosWithinMicro` in `0...999`.
    #expect(
      TimestampNanos(epochNanos: -1) == TimestampNanos(epochMicros: -1, nanosWithinMicro: 999))
    #expect(TimestampNanos(epochNanos: -1_000) == TimestampNanos(epochMicros: -1))
    #expect(
      TimestampNanos(epochNanos: -1_001) == TimestampNanos(epochMicros: -2, nanosWithinMicro: 999))
  }

  @Test
  func date() async throws {
    #expect(TimestampNanos(epochNanos: 0).date == Date(timeIntervalSince1970: 0))
    #expect(TimestampNanos(epochNanos: 1_500_000_000).date == Date(timeIntervalSince1970: 1.5))
    #expect(TimestampNanos(epochNanos: -1_500_000_000).date == Date(timeIntervalSince1970: -1.5))
    let date = TimestampNanos(epochNanos: 1_767_225_600_123_456_789).date
    #expect(abs(date.timeIntervalSince1970 - 1_767_225_600.123456789) < 1e-6)
  }

  @Test
  func description() async throws {
    #expect(TimestampNanos(epochNanos: 0).description == "1970-01-01 00:00:00")
    #expect(TimestampNanos(epochNanos: -1).description == "1969-12-31 23:59:59.999999999")
    #expect(
      TimestampNanos(epochNanos: 1_767_225_600_123_000_000).description
        == "2026-01-01 00:00:00.123")
    #expect(
      TimestampNanos(epochNanos: 1_767_225_600_123_456_000).description
        == "2026-01-01 00:00:00.123456")
    #expect(
      TimestampNanos(epochNanos: 1_767_225_600_123_456_789).description
        == "2026-01-01 00:00:00.123456789")
    #expect(
      TimestampNanos(epochNanos: 951_782_400_000_000_000).description == "2000-02-29 00:00:00")
    // Beyond the range of Arrow's 64-bit nanosecond timestamps (1677 ~ 2262).
    #expect(
      TimestampNanos(epochMicros: 32_503_680_000_000_000)?.description == "3000-01-01 00:00:00")
    #expect(
      TimestampNanos(epochMicros: -11_676_096_000_000_000)?.description == "1600-01-01 00:00:00")
    #expect(
      TimestampNanos(epochMicros: -62_135_596_800_000_000)?.description == "0001-01-01 00:00:00")
  }
}
