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

/// A time of day without a date or a time zone, like `java.time.LocalTime`.
/// Values of Spark's `TIME` type are represented as `LocalTime` in ``Row``s.
public struct LocalTime: Sendable, Equatable, Hashable, CustomStringConvertible {
  static let nanosPerSecond: Int64 = 1_000_000_000
  static let nanosPerMinute: Int64 = 60 * nanosPerSecond
  static let nanosPerHour: Int64 = 60 * nanosPerMinute
  static let nanosPerDay: Int64 = 24 * nanosPerHour

  /// The number of nanoseconds since midnight, in the range `0..<86_400_000_000_000`.
  public let nanoOfDay: Int64

  /// Creates a `LocalTime` from a time of day.
  /// - Parameters:
  ///   - hour: An hour in the range `0...23`.
  ///   - minute: A minute in the range `0...59`.
  ///   - second: A second in the range `0...59`.
  ///   - nanosecond: A nanosecond in the range `0...999_999_999`.
  /// - Returns: `nil` if any component is out of range.
  public init?(hour: Int, minute: Int, second: Int = 0, nanosecond: Int = 0) {
    guard (0..<24).contains(hour), (0..<60).contains(minute), (0..<60).contains(second),
      (0..<1_000_000_000).contains(nanosecond)
    else {
      return nil
    }
    self.nanoOfDay =
      Int64(hour) * Self.nanosPerHour + Int64(minute) * Self.nanosPerMinute
      + Int64(second) * Self.nanosPerSecond + Int64(nanosecond)
  }

  /// Creates a `LocalTime` from the number of nanoseconds since midnight.
  /// - Parameter nanoOfDay: A nanosecond of the day in the range `0..<86_400_000_000_000`.
  /// - Returns: `nil` if `nanoOfDay` is out of range.
  public init?(nanoOfDay: Int64) {
    guard (0..<Self.nanosPerDay).contains(nanoOfDay) else {
      return nil
    }
    self.nanoOfDay = nanoOfDay
  }

  /// The hour of the day, in the range `0...23`.
  public var hour: Int { Int(nanoOfDay / Self.nanosPerHour) }

  /// The minute of the hour, in the range `0...59`.
  public var minute: Int { Int((nanoOfDay / Self.nanosPerMinute) % 60) }

  /// The second of the minute, in the range `0...59`.
  public var second: Int { Int((nanoOfDay / Self.nanosPerSecond) % 60) }

  /// The nanosecond of the second, in the range `0...999_999_999`.
  public var nanosecond: Int { Int(nanoOfDay % Self.nanosPerSecond) }

  /// A string like `12:34:56`, `12:34:56.123`, `12:34:56.123456`, or
  /// `12:34:56.123456789` depending on the smallest non-zero fractional unit.
  public var description: String {
    var result = String(format: "%02d:%02d:%02d", hour, minute, second)
    let nanosecond = self.nanosecond
    if nanosecond != 0 {
      if nanosecond % 1_000_000 == 0 {
        result += String(format: ".%03d", nanosecond / 1_000_000)
      } else if nanosecond % 1_000 == 0 {
        result += String(format: ".%06d", nanosecond / 1_000)
      } else {
        result += String(format: ".%09d", nanosecond)
      }
    }
    return result
  }
}
