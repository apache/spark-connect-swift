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
import Testing

/// A test suite for ``LocalTime``.
struct LocalTimeTests {
  @Test
  func components() async throws {
    let time = try #require(LocalTime(hour: 12, minute: 34, second: 56, nanosecond: 123_456_789))
    #expect(time.hour == 12)
    #expect(time.minute == 34)
    #expect(time.second == 56)
    #expect(time.nanosecond == 123_456_789)
    #expect(time.nanoOfDay == 45_296_123_456_789)
  }

  @Test
  func componentBounds() async throws {
    #expect(LocalTime(hour: 0, minute: 0) != nil)
    #expect(LocalTime(hour: 23, minute: 59, second: 59, nanosecond: 999_999_999) != nil)
    #expect(LocalTime(hour: -1, minute: 0) == nil)
    #expect(LocalTime(hour: 24, minute: 0) == nil)
    #expect(LocalTime(hour: 0, minute: 60) == nil)
    #expect(LocalTime(hour: 0, minute: 0, second: 60) == nil)
    #expect(LocalTime(hour: 0, minute: 0, second: 0, nanosecond: 1_000_000_000) == nil)
  }

  @Test
  func nanoOfDay() async throws {
    #expect(LocalTime(nanoOfDay: 0) == LocalTime(hour: 0, minute: 0))
    #expect(LocalTime(nanoOfDay: 45_296_000_000_000) == LocalTime(hour: 12, minute: 34, second: 56))
    #expect(LocalTime(nanoOfDay: 86_399_999_999_999) != nil)
    #expect(LocalTime(nanoOfDay: -1) == nil)
    #expect(LocalTime(nanoOfDay: 86_400_000_000_000) == nil)
  }

  @Test
  func description() async throws {
    #expect(LocalTime(hour: 1, minute: 2, second: 3)?.description == "01:02:03")
    #expect(LocalTime(hour: 12, minute: 34, second: 56)?.description == "12:34:56")
    #expect(
      LocalTime(hour: 12, minute: 34, second: 56, nanosecond: 123_000_000)?.description
        == "12:34:56.123")
    #expect(
      LocalTime(hour: 12, minute: 34, second: 56, nanosecond: 123_456_000)?.description
        == "12:34:56.123456")
    #expect(
      LocalTime(hour: 12, minute: 34, second: 56, nanosecond: 123_456_789)?.description
        == "12:34:56.123456789")
  }
}
