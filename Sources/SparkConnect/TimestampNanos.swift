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

/// A timestamp with nanosecond precision, like `TimestampNanosVal` of Apache Spark.
/// Values of Spark's `TIMESTAMP_NTZ(p)` and `TIMESTAMP_LTZ(p)` types (`p` in `7...9`) are
/// represented as `TimestampNanos` in ``Row``s because `Date` cannot keep sub-microsecond digits.
/// Like `Date` for the microsecond timestamp types, a value is the elapsed time since the Unix
/// epoch: `TIMESTAMP_LTZ(p)` values are instants and `TIMESTAMP_NTZ(p)` values are local
/// date-times interpreted as UTC.
public struct TimestampNanos: Sendable, Equatable, Hashable, CustomStringConvertible {
  /// The number of microseconds since `1970-01-01 00:00:00` UTC, rounded toward negative infinity.
  public let epochMicros: Int64

  /// The nanosecond within the microsecond, in the range `0...999`.
  public let nanosWithinMicro: Int16

  /// Creates a `TimestampNanos` from the microseconds since the Unix epoch and the nanoseconds
  /// within that microsecond.
  /// - Parameters:
  ///   - epochMicros: The number of microseconds since `1970-01-01 00:00:00` UTC.
  ///   - nanosWithinMicro: A nanosecond within the microsecond in the range `0...999`.
  /// - Returns: `nil` if `nanosWithinMicro` is out of range.
  public init?(epochMicros: Int64, nanosWithinMicro: Int16 = 0) {
    guard (0...999).contains(nanosWithinMicro) else {
      return nil
    }
    self.epochMicros = epochMicros
    self.nanosWithinMicro = nanosWithinMicro
  }

  /// Creates a `TimestampNanos` from the number of nanoseconds since the Unix epoch.
  /// - Parameter epochNanos: The number of nanoseconds since `1970-01-01 00:00:00` UTC.
  public init(epochNanos: Int64) {
    let (micros, nanos) = Self.floorDivMod(epochNanos, 1_000)
    self.epochMicros = micros
    self.nanosWithinMicro = Int16(nanos)
  }

  /// The same point in time as a `Date`, rounded to the resolution of `Date`'s `Double` seconds.
  public var date: Date {
    Date(
      timeIntervalSince1970: TimeInterval(epochMicros) / 1_000_000
        + TimeInterval(nanosWithinMicro) / 1_000_000_000)
  }

  /// A UTC string like `2026-01-01 00:00:00`, `2026-01-01 00:00:00.123`,
  /// `2026-01-01 00:00:00.123456`, or `2026-01-01 00:00:00.123456789` depending on the
  /// smallest non-zero fractional unit.
  public var description: String {
    let (seconds, micros) = Self.floorDivMod(epochMicros, 1_000_000)
    let (days, secondOfDay) = Self.floorDivMod(seconds, 86_400)
    let nanoOfDay =
      secondOfDay * LocalTime.nanosPerSecond + micros * 1_000 + Int64(nanosWithinMicro)
    let (year, month, day) = Self.civilDate(daysSinceEpoch: days)
    return String(format: "%04d-%02d-%02d ", year, month, day)
      + LocalTime(nanoOfDay: nanoOfDay)!.description
  }

  private static func floorDivMod(_ x: Int64, _ y: Int64) -> (Int64, Int64) {
    let (quotient, remainder) = x.quotientAndRemainder(dividingBy: y)
    return remainder < 0 ? (quotient - 1, remainder + y) : (quotient, remainder)
  }

  /// Converts days since `1970-01-01` to a proleptic Gregorian date.
  /// See https://howardhinnant.github.io/date_algorithms.html#civil_from_days
  private static func civilDate(daysSinceEpoch: Int64) -> (Int, Int, Int) {
    let z = daysSinceEpoch + 719_468
    let (era, dayOfEra) = floorDivMod(z, 146_097)
    let yearOfEra = (dayOfEra - dayOfEra / 1_460 + dayOfEra / 36_524 - dayOfEra / 146_096) / 365
    let dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100)
    let shiftedMonth = (5 * dayOfYear + 2) / 153
    let day = dayOfYear - (153 * shiftedMonth + 2) / 5 + 1
    let month = shiftedMonth < 10 ? shiftedMonth + 3 : shiftedMonth - 9
    let year = yearOfEra + era * 400 + (month <= 2 ? 1 : 0)
    return (Int(year), Int(month), Int(day))
  }
}
