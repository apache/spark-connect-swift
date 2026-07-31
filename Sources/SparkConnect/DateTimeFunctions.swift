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

/// Returns the date that is `numMonths` after `startDate`.
/// - Parameters:
///   - startDate: A date ``Column``.
///   - numMonths: The number of months to add.
/// - Returns: A ``Column``.
public func add_months(_ startDate: Column, _ numMonths: Int32) -> Column {
  return add_months(startDate, lit(numMonths))
}

/// Returns the date that is `numMonths` after `startDate`.
/// - Parameters:
///   - startDate: A date ``Column``.
///   - numMonths: A ``Column`` for the number of months to add.
/// - Returns: A ``Column``.
public func add_months(_ startDate: Column, _ numMonths: Column) -> Column {
  return fn("add_months", startDate, numMonths)
}

/// Converts the timestamp without time zone `sourceTs` from the current time zone to `targetTz`.
/// - Parameters:
///   - targetTz: A ``Column`` for the time zone to which the input timestamp should be converted.
///   - sourceTs: A timestamp without time zone ``Column``.
/// - Returns: A ``Column``.
public func convert_timezone(_ targetTz: Column, _ sourceTs: Column) -> Column {
  return fn("convert_timezone", targetTz, sourceTs)
}

/// Converts the timestamp without time zone `sourceTs` from the `sourceTz` time zone to
/// `targetTz`.
/// - Parameters:
///   - sourceTz: A ``Column`` for the time zone of the input timestamp.
///   - targetTz: A ``Column`` for the time zone to which the input timestamp should be converted.
///   - sourceTs: A timestamp without time zone ``Column``.
/// - Returns: A ``Column``.
public func convert_timezone(_ sourceTz: Column, _ targetTz: Column, _ sourceTs: Column) -> Column
{
  return fn("convert_timezone", sourceTz, targetTz, sourceTs)
}

/// Returns the current date at the start of query evaluation.
/// - Returns: A ``Column``.
public func curdate() -> Column {
  return fn("curdate")
}

/// Returns the current date at the start of query evaluation.
/// - Returns: A ``Column``.
public func current_date() -> Column {
  return fn("current_date")
}

/// Returns the current time at the start of query evaluation. Note that the result will contain
/// 6 fractional digits of seconds.
/// - Returns: A ``Column``.
public func current_time() -> Column {
  return fn("current_time")
}

/// Returns the current time at the start of query evaluation.
/// - Parameter precision: The number of fractional digits of seconds in the range [0..6].
/// - Returns: A ``Column``.
public func current_time(_ precision: Int32) -> Column {
  return fn("current_time", lit(precision))
}

/// Returns the current timestamp at the start of query evaluation.
/// - Returns: A ``Column``.
public func current_timestamp() -> Column {
  return fn("current_timestamp")
}

/// Returns the current session local timezone.
/// - Returns: A ``Column``.
public func current_timezone() -> Column {
  return fn("current_timezone")
}

/// Returns the date that is `days` days after `start`.
/// - Parameters:
///   - start: A date ``Column``.
///   - days: The number of days to add.
/// - Returns: A ``Column``.
public func date_add(_ start: Column, _ days: Int32) -> Column {
  return date_add(start, lit(days))
}

/// Returns the date that is `days` days after `start`.
/// - Parameters:
///   - start: A date ``Column``.
///   - days: A ``Column`` for the number of days to add.
/// - Returns: A ``Column``.
public func date_add(_ start: Column, _ days: Column) -> Column {
  return fn("date_add", start, days)
}

/// Returns the number of days from `start` to `end`.
/// - Parameters:
///   - end: A date ``Column``.
///   - start: A date ``Column``.
/// - Returns: A ``Column``.
public func date_diff(_ end: Column, _ start: Column) -> Column {
  return fn("date_diff", end, start)
}

/// Converts a date/timestamp/string to a value of string in the format specified by the date
/// format given by the second argument.
/// - Parameters:
///   - dateExpr: A date/timestamp/string ``Column``.
///   - format: A pattern to format.
/// - Returns: A ``Column``.
public func date_format(_ dateExpr: Column, _ format: String) -> Column {
  return fn("date_format", dateExpr, lit(format))
}

/// Creates a date from the number of `days` since 1970-01-01.
/// - Parameter days: A ``Column`` for the number of days.
/// - Returns: A ``Column``.
public func date_from_unix_date(_ days: Column) -> Column {
  return fn("date_from_unix_date", days)
}

/// Extracts a part of the date/timestamp or interval source.
/// - Parameters:
///   - field: A ``Column`` for the field to extract, e.g. `lit("YEAR")`.
///   - source: A date/timestamp/interval ``Column``.
/// - Returns: A ``Column``.
public func date_part(_ field: Column, _ source: Column) -> Column {
  return fn("date_part", field, source)
}

/// Returns the date that is `days` days before `start`.
/// - Parameters:
///   - start: A date ``Column``.
///   - days: The number of days to subtract.
/// - Returns: A ``Column``.
public func date_sub(_ start: Column, _ days: Int32) -> Column {
  return date_sub(start, lit(days))
}

/// Returns the date that is `days` days before `start`.
/// - Parameters:
///   - start: A date ``Column``.
///   - days: A ``Column`` for the number of days to subtract.
/// - Returns: A ``Column``.
public func date_sub(_ start: Column, _ days: Column) -> Column {
  return fn("date_sub", start, days)
}

/// Returns timestamp truncated to the unit specified by the format.
/// - Parameters:
///   - format: A unit to truncate to, e.g. `year`, `month`, `day`, `hour`.
///   - timestamp: A timestamp ``Column``.
/// - Returns: A ``Column``.
public func date_trunc(_ format: String, _ timestamp: Column) -> Column {
  return fn("date_trunc", lit(format), timestamp)
}

/// Returns the date that is `days` days after `start`. This is an alias of ``date_add(_:_:)``.
/// - Parameters:
///   - start: A date ``Column``.
///   - days: A ``Column`` for the number of days to add.
/// - Returns: A ``Column``.
public func dateadd(_ start: Column, _ days: Column) -> Column {
  return fn("dateadd", start, days)
}

/// Returns the number of days from `start` to `end`.
/// - Parameters:
///   - end: A date ``Column``.
///   - start: A date ``Column``.
/// - Returns: A ``Column``.
public func datediff(_ end: Column, _ start: Column) -> Column {
  return fn("datediff", end, start)
}

/// Extracts a part of the date/timestamp or interval source. This is an alias of
/// ``date_part(_:_:)``.
/// - Parameters:
///   - field: A ``Column`` for the field to extract, e.g. `lit("YEAR")`.
///   - source: A date/timestamp/interval ``Column``.
/// - Returns: A ``Column``.
public func datepart(_ field: Column, _ source: Column) -> Column {
  return fn("datepart", field, source)
}

/// Extracts the day of the month as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func day(_ e: Column) -> Column {
  return fn("day", e)
}

/// Extracts the three-letter abbreviated day name from a given date/timestamp/string.
/// - Parameter timeExp: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func dayname(_ timeExp: Column) -> Column {
  return fn("dayname", timeExp)
}

/// Extracts the day of the month as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func dayofmonth(_ e: Column) -> Column {
  return fn("dayofmonth", e)
}

/// Extracts the day of the week as an integer from a given date/timestamp/string.
/// Ranges from 1 for a Sunday through to 7 for a Saturday.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func dayofweek(_ e: Column) -> Column {
  return fn("dayofweek", e)
}

/// Extracts the day of the year as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func dayofyear(_ e: Column) -> Column {
  return fn("dayofyear", e)
}

/// Extracts a part of the date/timestamp or interval source.
/// - Parameters:
///   - field: A ``Column`` for the field to extract, e.g. `lit("YEAR")`.
///   - source: A date/timestamp/interval ``Column``.
/// - Returns: A ``Column``.
public func extract(_ field: Column, _ source: Column) -> Column {
  return fn("extract", field, source)
}

/// Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
/// representing the timestamp of that moment in the current system time zone in the
/// `yyyy-MM-dd HH:mm:ss` format.
/// - Parameter ut: A number of seconds ``Column``.
/// - Returns: A ``Column``.
public func from_unixtime(_ ut: Column) -> Column {
  return fn("from_unixtime", ut)
}

/// Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
/// representing the timestamp of that moment in the current system time zone in the given format.
/// - Parameters:
///   - ut: A number of seconds ``Column``.
///   - f: A date time pattern.
/// - Returns: A ``Column``.
public func from_unixtime(_ ut: Column, _ f: String) -> Column {
  return fn("from_unixtime", ut, lit(f))
}

/// Takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in UTC, and
/// renders that timestamp as a timestamp in the given time zone.
/// - Parameters:
///   - ts: A timestamp ``Column``.
///   - tz: A time zone, e.g. `Asia/Seoul`.
/// - Returns: A ``Column``.
public func from_utc_timestamp(_ ts: Column, _ tz: String) -> Column {
  return from_utc_timestamp(ts, lit(tz))
}

/// Takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in UTC, and
/// renders that timestamp as a timestamp in the given time zone.
/// - Parameters:
///   - ts: A timestamp ``Column``.
///   - tz: A time zone ``Column``.
/// - Returns: A ``Column``.
public func from_utc_timestamp(_ ts: Column, _ tz: Column) -> Column {
  return fn("from_utc_timestamp", ts, tz)
}

/// Extracts the hours as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func hour(_ e: Column) -> Column {
  return fn("hour", e)
}

/// Returns the last day of the month which the given date belongs to.
/// - Parameter e: A date ``Column``.
/// - Returns: A ``Column``.
public func last_day(_ e: Column) -> Column {
  return fn("last_day", e)
}

/// Returns the current timestamp without time zone at the start of query evaluation.
/// - Returns: A ``Column``.
public func localtimestamp() -> Column {
  return fn("localtimestamp")
}

/// Creates a date from `year`, `month` and `day` fields.
/// - Parameters:
///   - year: A year ``Column``.
///   - month: A month ``Column``.
///   - day: A day ``Column``.
/// - Returns: A ``Column``.
public func make_date(_ year: Column, _ month: Column, _ day: Column) -> Column {
  return fn("make_date", year, month, day)
}

/// Creates a time from `hour`, `minute` and `second` fields.
/// - Parameters:
///   - hour: An hour ``Column``, from 0 to 23.
///   - minute: A minute ``Column``, from 0 to 59.
///   - second: A second ``Column``, from 0 to 59.999999.
/// - Returns: A ``Column``.
public func make_time(_ hour: Column, _ minute: Column, _ second: Column) -> Column {
  return fn("make_time", hour, minute, second)
}

/// Extracts the minutes as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func minute(_ e: Column) -> Column {
  return fn("minute", e)
}

/// Extracts the month as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func month(_ e: Column) -> Column {
  return fn("month", e)
}

/// Extracts the three-letter abbreviated month name from a given date/timestamp/string.
/// - Parameter timeExp: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func monthname(_ timeExp: Column) -> Column {
  return fn("monthname", timeExp)
}

/// Returns number of months between dates `start` and `end`.
/// - Parameters:
///   - end: A date ``Column``.
///   - start: A date ``Column``.
/// - Returns: A ``Column``.
public func months_between(_ end: Column, _ start: Column) -> Column {
  return fn("months_between", end, start)
}

/// Returns number of months between dates `end` and `start`. If `roundOff` is set to true, the
/// result is rounded off to 8 digits.
/// - Parameters:
///   - end: A date ``Column``.
///   - start: A date ``Column``.
///   - roundOff: Whether to round off the result to 8 digits.
/// - Returns: A ``Column``.
public func months_between(_ end: Column, _ start: Column, _ roundOff: Bool) -> Column {
  return fn("months_between", end, start, lit(roundOff))
}

/// Returns the first date which is later than the value of the `date` column that is on the
/// specified day of the week.
/// - Parameters:
///   - date: A date ``Column``.
///   - dayOfWeek: A day of the week, e.g. `Mon`, `Tue`.
/// - Returns: A ``Column``.
public func next_day(_ date: Column, _ dayOfWeek: String) -> Column {
  return next_day(date, lit(dayOfWeek))
}

/// Returns the first date which is later than the value of the `date` column that is on the
/// specified day of the week.
/// - Parameters:
///   - date: A date ``Column``.
///   - dayOfWeek: A day of the week ``Column``.
/// - Returns: A ``Column``.
public func next_day(_ date: Column, _ dayOfWeek: Column) -> Column {
  return fn("next_day", date, dayOfWeek)
}

/// Returns the current timestamp at the start of query evaluation.
/// - Returns: A ``Column``.
public func now() -> Column {
  return fn("now")
}

/// Extracts the quarter as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func quarter(_ e: Column) -> Column {
  return fn("quarter", e)
}

/// Extracts the seconds as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func second(_ e: Column) -> Column {
  return fn("second", e)
}

/// Generates session window given a timestamp specifying column.
/// - Parameters:
///   - timeColumn: A timestamp ``Column`` to use for windowing by time.
///   - gapDuration: A string specifying the timeout of the session, e.g. `10 minutes`.
/// - Returns: A ``Column``.
public func session_window(_ timeColumn: Column, _ gapDuration: String) -> Column {
  return session_window(timeColumn, lit(gapDuration))
}

/// Generates session window given a timestamp specifying column.
/// - Parameters:
///   - timeColumn: A timestamp ``Column`` to use for windowing by time.
///   - gapDuration: A ``Column`` specifying the timeout of the session.
/// - Returns: A ``Column``.
public func session_window(_ timeColumn: Column, _ gapDuration: Column) -> Column {
  return fn("session_window", timeColumn, gapDuration)
}

/// Assigns the given timestamp to a fixed-size bucket and returns the start time of the bucket.
/// - Parameters:
///   - bucketSize: A day-time interval ``Column`` for the width of each bucket.
///   - ts: A timestamp ``Column``.
/// - Returns: A ``Column``.
public func time_bucket(_ bucketSize: Column, _ ts: Column) -> Column {
  return fn("time_bucket", bucketSize, ts)
}

/// Assigns the given timestamp to a fixed-size bucket relative to `origin` and returns the start
/// time of the bucket.
/// - Parameters:
///   - bucketSize: A day-time interval ``Column`` for the width of each bucket.
///   - ts: A timestamp ``Column``.
///   - origin: A timestamp ``Column`` to which the buckets are aligned.
/// - Returns: A ``Column``.
public func time_bucket(_ bucketSize: Column, _ ts: Column, _ origin: Column) -> Column {
  return fn("time_bucket", bucketSize, ts, origin)
}

/// Returns the difference between two times, measured in specified units.
/// - Parameters:
///   - unit: A ``Column`` for the unit, e.g. `lit("HOUR")`.
///   - start: A start time ``Column``.
///   - end: An end time ``Column``.
/// - Returns: A ``Column``.
public func time_diff(_ unit: Column, _ start: Column, _ end: Column) -> Column {
  return fn("time_diff", unit, start, end)
}

/// Creates a TIME from the number of microseconds since midnight.
/// - Parameter e: A number of microseconds ``Column``.
/// - Returns: A ``Column``.
public func time_from_micros(_ e: Column) -> Column {
  return fn("time_from_micros", e)
}

/// Creates a TIME from the number of milliseconds since midnight.
/// - Parameter e: A number of milliseconds ``Column``.
/// - Returns: A ``Column``.
public func time_from_millis(_ e: Column) -> Column {
  return fn("time_from_millis", e)
}

/// Creates a TIME from the number of seconds since midnight.
/// - Parameter e: A number of seconds ``Column``.
/// - Returns: A ``Column``.
public func time_from_seconds(_ e: Column) -> Column {
  return fn("time_from_seconds", e)
}

/// Returns the number of microseconds since midnight from a TIME value.
/// - Parameter e: A time ``Column``.
/// - Returns: A ``Column``.
public func time_to_micros(_ e: Column) -> Column {
  return fn("time_to_micros", e)
}

/// Returns the number of milliseconds since midnight from a TIME value.
/// - Parameter e: A time ``Column``.
/// - Returns: A ``Column``.
public func time_to_millis(_ e: Column) -> Column {
  return fn("time_to_millis", e)
}

/// Returns the number of seconds since midnight from a TIME value.
/// - Parameter e: A time ``Column``.
/// - Returns: A ``Column``.
public func time_to_seconds(_ e: Column) -> Column {
  return fn("time_to_seconds", e)
}

/// Returns `time` truncated to the `unit`.
/// - Parameters:
///   - unit: A ``Column`` for the unit, e.g. `lit("HOUR")`.
///   - time: A time ``Column``.
/// - Returns: A ``Column``.
public func time_trunc(_ unit: Column, _ time: Column) -> Column {
  return fn("time_trunc", unit, time)
}

/// Adds the specified number of units to the given timestamp.
/// - Parameters:
///   - unit: A unit, e.g. `HOUR`, `DAY`.
///   - quantity: A ``Column`` for the number of units to add.
///   - ts: A timestamp ``Column``.
/// - Returns: A ``Column``.
public func timestamp_add(_ unit: String, _ quantity: Column, _ ts: Column) -> Column {
  return fn("timestampadd", lit(unit), quantity, ts)
}

/// Gets the difference between the timestamps in the specified units by truncating the fraction
/// part.
/// - Parameters:
///   - unit: A unit, e.g. `HOUR`, `DAY`.
///   - start: A start timestamp ``Column``.
///   - end: An end timestamp ``Column``.
/// - Returns: A ``Column``.
public func timestamp_diff(_ unit: String, _ start: Column, _ end: Column) -> Column {
  return fn("timestampdiff", lit(unit), start, end)
}

/// Creates timestamp from the number of microseconds since UTC epoch.
/// - Parameter e: A number of microseconds ``Column``.
/// - Returns: A ``Column``.
public func timestamp_micros(_ e: Column) -> Column {
  return fn("timestamp_micros", e)
}

/// Creates timestamp from the number of milliseconds since UTC epoch.
/// - Parameter e: A number of milliseconds ``Column``.
/// - Returns: A ``Column``.
public func timestamp_millis(_ e: Column) -> Column {
  return fn("timestamp_millis", e)
}

/// Creates timestamp from the number of nanoseconds since UTC epoch.
/// - Parameter e: A number of nanoseconds ``Column``.
/// - Returns: A ``Column``.
public func timestamp_nanos(_ e: Column) -> Column {
  return fn("timestamp_nanos", e)
}

/// Creates timestamp from the number of seconds (can be fractional) since UTC epoch.
/// - Parameter e: A number of seconds ``Column``.
/// - Returns: A ``Column``.
public func timestamp_seconds(_ e: Column) -> Column {
  return fn("timestamp_seconds", e)
}

/// Converts the column into `DateType` by casting rules to `DateType`.
/// - Parameter e: A ``Column``.
/// - Returns: A ``Column``.
public func to_date(_ e: Column) -> Column {
  return fn("to_date", e)
}

/// Converts the column into a `DateType` with a specified format.
/// - Parameters:
///   - e: A ``Column``.
///   - fmt: A date time pattern.
/// - Returns: A ``Column``.
public func to_date(_ e: Column, _ fmt: String) -> Column {
  return fn("to_date", e, lit(fmt))
}

/// Converts a string value to a time value.
/// - Parameter str: A string ``Column`` to be parsed to time.
/// - Returns: A ``Column``.
public func to_time(_ str: Column) -> Column {
  return fn("to_time", str)
}

/// Parses a string value to a time value with a specified format.
/// - Parameters:
///   - str: A string ``Column`` to be parsed to time.
///   - format: A time format pattern ``Column``.
/// - Returns: A ``Column``.
public func to_time(_ str: Column, _ format: Column) -> Column {
  return fn("to_time", str, format)
}

/// Converts to a timestamp by casting rules to `TimestampType`.
/// - Parameter s: A ``Column``.
/// - Returns: A ``Column``.
public func to_timestamp(_ s: Column) -> Column {
  return fn("to_timestamp", s)
}

/// Converts time string with the given pattern to timestamp.
/// - Parameters:
///   - s: A ``Column``.
///   - fmt: A date time pattern.
/// - Returns: A ``Column``.
public func to_timestamp(_ s: Column, _ fmt: String) -> Column {
  return fn("to_timestamp", s, lit(fmt))
}

/// Parses the `timestamp` expression to a timestamp with local time zone.
/// - Parameter timestamp: A ``Column``.
/// - Returns: A ``Column``.
public func to_timestamp_ltz(_ timestamp: Column) -> Column {
  return fn("to_timestamp_ltz", timestamp)
}

/// Parses the `timestamp` expression with the `format` expression to a timestamp with local time
/// zone.
/// - Parameters:
///   - timestamp: A ``Column``.
///   - format: A date time pattern ``Column``.
/// - Returns: A ``Column``.
public func to_timestamp_ltz(_ timestamp: Column, _ format: Column) -> Column {
  return fn("to_timestamp_ltz", timestamp, format)
}

/// Parses the `timestamp` expression to a timestamp without time zone.
/// - Parameter timestamp: A ``Column``.
/// - Returns: A ``Column``.
public func to_timestamp_ntz(_ timestamp: Column) -> Column {
  return fn("to_timestamp_ntz", timestamp)
}

/// Parses the `timestamp` expression with the `format` expression to a timestamp without time
/// zone.
/// - Parameters:
///   - timestamp: A ``Column``.
///   - format: A date time pattern ``Column``.
/// - Returns: A ``Column``.
public func to_timestamp_ntz(_ timestamp: Column, _ format: Column) -> Column {
  return fn("to_timestamp_ntz", timestamp, format)
}

/// Returns the UNIX timestamp of the given time.
/// - Parameter timeExp: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func to_unix_timestamp(_ timeExp: Column) -> Column {
  return fn("to_unix_timestamp", timeExp)
}

/// Returns the UNIX timestamp of the given time with a specified format.
/// - Parameters:
///   - timeExp: A date/timestamp/string ``Column``.
///   - format: A date time pattern ``Column``.
/// - Returns: A ``Column``.
public func to_unix_timestamp(_ timeExp: Column, _ format: Column) -> Column {
  return fn("to_unix_timestamp", timeExp, format)
}

/// Takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in the given
/// time zone, and renders that timestamp as a timestamp in UTC.
/// - Parameters:
///   - ts: A timestamp ``Column``.
///   - tz: A time zone, e.g. `Asia/Seoul`.
/// - Returns: A ``Column``.
public func to_utc_timestamp(_ ts: Column, _ tz: String) -> Column {
  return to_utc_timestamp(ts, lit(tz))
}

/// Takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in the given
/// time zone, and renders that timestamp as a timestamp in UTC.
/// - Parameters:
///   - ts: A timestamp ``Column``.
///   - tz: A time zone ``Column``.
/// - Returns: A ``Column``.
public func to_utc_timestamp(_ ts: Column, _ tz: Column) -> Column {
  return fn("to_utc_timestamp", ts, tz)
}

/// Returns date truncated to the unit specified by the format.
/// - Parameters:
///   - date: A date ``Column``.
///   - format: A unit to truncate to, e.g. `year`, `month`.
/// - Returns: A ``Column``.
public func trunc(_ date: Column, _ format: String) -> Column {
  return fn("trunc", date, lit(format))
}

/// Converts the column into `DateType` by casting rules to `DateType`. Returns null with invalid
/// input.
/// - Parameter e: A ``Column``.
/// - Returns: A ``Column``.
public func try_to_date(_ e: Column) -> Column {
  return fn("try_to_date", e)
}

/// Converts the column into a `DateType` with a specified format. Returns null with invalid
/// input.
/// - Parameters:
///   - e: A ``Column``.
///   - fmt: A date time pattern.
/// - Returns: A ``Column``.
public func try_to_date(_ e: Column, _ fmt: String) -> Column {
  return fn("try_to_date", e, lit(fmt))
}

/// Converts a string value to a time value. Returns null with invalid input.
/// - Parameter str: A string ``Column`` to be parsed to time.
/// - Returns: A ``Column``.
public func try_to_time(_ str: Column) -> Column {
  return fn("try_to_time", str)
}

/// Parses a string value to a time value with a specified format. Returns null with invalid
/// input.
/// - Parameters:
///   - str: A string ``Column`` to be parsed to time.
///   - format: A time format pattern ``Column``.
/// - Returns: A ``Column``.
public func try_to_time(_ str: Column, _ format: Column) -> Column {
  return fn("try_to_time", str, format)
}

/// Parses the `s` to a timestamp. The function always returns null on an invalid input.
/// - Parameter s: A ``Column``.
/// - Returns: A ``Column``.
public func try_to_timestamp(_ s: Column) -> Column {
  return fn("try_to_timestamp", s)
}

/// Parses the `s` with the `format` to a timestamp. The function always returns null on an
/// invalid input.
/// - Parameters:
///   - s: A ``Column``.
///   - format: A date time pattern ``Column``.
/// - Returns: A ``Column``.
public func try_to_timestamp(_ s: Column, _ format: Column) -> Column {
  return fn("try_to_timestamp", s, format)
}

/// Returns the number of days since 1970-01-01.
/// - Parameter e: A date ``Column``.
/// - Returns: A ``Column``.
public func unix_date(_ e: Column) -> Column {
  return fn("unix_date", e)
}

/// Returns the number of microseconds since 1970-01-01 00:00:00 UTC.
/// - Parameter e: A timestamp ``Column``.
/// - Returns: A ``Column``.
public func unix_micros(_ e: Column) -> Column {
  return fn("unix_micros", e)
}

/// Returns the number of milliseconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of
/// precision.
/// - Parameter e: A timestamp ``Column``.
/// - Returns: A ``Column``.
public func unix_millis(_ e: Column) -> Column {
  return fn("unix_millis", e)
}

/// Returns the number of nanoseconds since 1970-01-01 00:00:00 UTC.
/// - Parameter e: A timestamp ``Column``.
/// - Returns: A ``Column``.
public func unix_nanos(_ e: Column) -> Column {
  return fn("unix_nanos", e)
}

/// Returns the number of seconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of
/// precision.
/// - Parameter e: A timestamp ``Column``.
/// - Returns: A ``Column``.
public func unix_seconds(_ e: Column) -> Column {
  return fn("unix_seconds", e)
}

/// Returns the current Unix timestamp (in seconds) as a long.
/// - Returns: A ``Column``.
public func unix_timestamp() -> Column {
  return unix_timestamp(current_timestamp())
}

/// Converts time string in format `yyyy-MM-dd HH:mm:ss` to Unix timestamp (in seconds), using
/// the default timezone and the default locale.
/// - Parameter s: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func unix_timestamp(_ s: Column) -> Column {
  return fn("unix_timestamp", s)
}

/// Converts time string with given pattern to Unix timestamp (in seconds).
/// - Parameters:
///   - s: A date/timestamp/string ``Column``.
///   - p: A date time pattern.
/// - Returns: A ``Column``.
public func unix_timestamp(_ s: Column, _ p: String) -> Column {
  return fn("unix_timestamp", s, lit(p))
}

/// Extracts the day of the week as an integer from a given date/timestamp/string.
/// Ranges from 0 for a Monday through to 6 for a Sunday.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func weekday(_ e: Column) -> Column {
  return fn("weekday", e)
}

/// Extracts the week number as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func weekofyear(_ e: Column) -> Column {
  return fn("weekofyear", e)
}

/// Generates tumbling time windows given a timestamp specifying column.
/// - Parameters:
///   - timeColumn: A timestamp ``Column`` to use for windowing by time.
///   - windowDuration: A string specifying the width of the window, e.g. `10 minutes`.
/// - Returns: A ``Column``.
public func window(_ timeColumn: Column, _ windowDuration: String) -> Column {
  return window(timeColumn, windowDuration, windowDuration, "0 second")
}

/// Bucketizes rows into one or more time windows given a timestamp specifying column.
/// - Parameters:
///   - timeColumn: A timestamp ``Column`` to use for windowing by time.
///   - windowDuration: A string specifying the width of the window, e.g. `10 minutes`.
///   - slideDuration: A string specifying the sliding interval of the window, e.g. `1 minute`.
/// - Returns: A ``Column``.
public func window(_ timeColumn: Column, _ windowDuration: String, _ slideDuration: String)
  -> Column
{
  return window(timeColumn, windowDuration, slideDuration, "0 second")
}

/// Bucketizes rows into one or more time windows given a timestamp specifying column.
/// - Parameters:
///   - timeColumn: A timestamp ``Column`` to use for windowing by time.
///   - windowDuration: A string specifying the width of the window, e.g. `10 minutes`.
///   - slideDuration: A string specifying the sliding interval of the window, e.g. `1 minute`.
///   - startTime: The offset with respect to 1970-01-01 00:00:00 UTC with which to start window
///     intervals, e.g. `15 minutes`.
/// - Returns: A ``Column``.
public func window(
  _ timeColumn: Column, _ windowDuration: String, _ slideDuration: String, _ startTime: String
) -> Column {
  return fn("window", timeColumn, lit(windowDuration), lit(slideDuration), lit(startTime))
}

/// Extracts the event time from the window column.
/// - Parameter windowColumn: A window ``Column``, e.g. produced by ``window(_:_:)``.
/// - Returns: A ``Column``.
public func window_time(_ windowColumn: Column) -> Column {
  return fn("window_time", windowColumn)
}

/// Extracts the year as an integer from a given date/timestamp/string.
/// - Parameter e: A date/timestamp/string ``Column``.
/// - Returns: A ``Column``.
public func year(_ e: Column) -> Column {
  return fn("year", e)
}
