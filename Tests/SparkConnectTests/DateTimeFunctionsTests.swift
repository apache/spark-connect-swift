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

@testable import SparkConnect

/// A test suite for `DateTimeFunctions`
@Suite(.serialized)
struct DateTimeFunctionsTests {

  @Test
  func dateTimeFunctions() throws {
    for (column, name) in [
      (date_from_unix_date(col("a")), "date_from_unix_date"),
      (day(col("a")), "day"),
      (dayname(col("a")), "dayname"),
      (dayofmonth(col("a")), "dayofmonth"),
      (dayofweek(col("a")), "dayofweek"),
      (dayofyear(col("a")), "dayofyear"),
      (from_unixtime(col("a")), "from_unixtime"),
      (hour(col("a")), "hour"),
      (last_day(col("a")), "last_day"),
      (minute(col("a")), "minute"),
      (month(col("a")), "month"),
      (monthname(col("a")), "monthname"),
      (quarter(col("a")), "quarter"),
      (second(col("a")), "second"),
      (time_from_micros(col("a")), "time_from_micros"),
      (time_from_millis(col("a")), "time_from_millis"),
      (time_from_seconds(col("a")), "time_from_seconds"),
      (time_to_micros(col("a")), "time_to_micros"),
      (time_to_millis(col("a")), "time_to_millis"),
      (time_to_seconds(col("a")), "time_to_seconds"),
      (timestamp_micros(col("a")), "timestamp_micros"),
      (timestamp_millis(col("a")), "timestamp_millis"),
      (timestamp_nanos(col("a")), "timestamp_nanos"),
      (timestamp_seconds(col("a")), "timestamp_seconds"),
      (to_date(col("a")), "to_date"),
      (to_time(col("a")), "to_time"),
      (to_timestamp(col("a")), "to_timestamp"),
      (to_timestamp_ltz(col("a")), "to_timestamp_ltz"),
      (to_timestamp_ntz(col("a")), "to_timestamp_ntz"),
      (to_unix_timestamp(col("a")), "to_unix_timestamp"),
      (try_to_date(col("a")), "try_to_date"),
      (try_to_time(col("a")), "try_to_time"),
      (try_to_timestamp(col("a")), "try_to_timestamp"),
      (unix_date(col("a")), "unix_date"),
      (unix_micros(col("a")), "unix_micros"),
      (unix_millis(col("a")), "unix_millis"),
      (unix_nanos(col("a")), "unix_nanos"),
      (unix_seconds(col("a")), "unix_seconds"),
      (unix_timestamp(col("a")), "unix_timestamp"),
      (weekday(col("a")), "weekday"),
      (weekofyear(col("a")), "weekofyear"),
      (window_time(col("a")), "window_time"),
      (year(col("a")), "year"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func dateTimeFunctionArguments() throws {
    for (column, name) in [
      (add_months(col("a"), col("b")), "add_months"),
      (convert_timezone(col("a"), col("b")), "convert_timezone"),
      (date_add(col("a"), col("b")), "date_add"),
      (date_diff(col("a"), col("b")), "date_diff"),
      (date_part(col("a"), col("b")), "date_part"),
      (date_sub(col("a"), col("b")), "date_sub"),
      (dateadd(col("a"), col("b")), "dateadd"),
      (datediff(col("a"), col("b")), "datediff"),
      (datepart(col("a"), col("b")), "datepart"),
      (extract(col("a"), col("b")), "extract"),
      (from_utc_timestamp(col("a"), col("b")), "from_utc_timestamp"),
      (months_between(col("a"), col("b")), "months_between"),
      (next_day(col("a"), col("b")), "next_day"),
      (session_window(col("a"), col("b")), "session_window"),
      (time_bucket(col("a"), col("b")), "time_bucket"),
      (time_trunc(col("a"), col("b")), "time_trunc"),
      (to_time(col("a"), col("b")), "to_time"),
      (to_timestamp_ltz(col("a"), col("b")), "to_timestamp_ltz"),
      (to_timestamp_ntz(col("a"), col("b")), "to_timestamp_ntz"),
      (to_unix_timestamp(col("a"), col("b")), "to_unix_timestamp"),
      (to_utc_timestamp(col("a"), col("b")), "to_utc_timestamp"),
      (try_to_time(col("a"), col("b")), "try_to_time"),
      (try_to_timestamp(col("a"), col("b")), "try_to_timestamp"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
    }

    for (column, name) in [
      (convert_timezone(col("a"), col("b"), col("c")), "convert_timezone"),
      (make_date(col("a"), col("b"), col("c")), "make_date"),
      (make_time(col("a"), col("b"), col("c")), "make_time"),
      (time_bucket(col("a"), col("b"), col("c")), "time_bucket"),
      (time_diff(col("a"), col("b"), col("c")), "time_diff"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 3)
    }

    for (column, name, value) in [
      (add_months(col("a"), 2), "add_months", Int32(2)),
      (date_add(col("a"), 10), "date_add", Int32(10)),
      (date_sub(col("a"), 10), "date_sub", Int32(10)),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[1].literal.integer == value)
    }

    for (column, name, value) in [
      (date_format(col("a"), "yyyy"), "date_format", "yyyy"),
      (from_unixtime(col("a"), "yyyy"), "from_unixtime", "yyyy"),
      (from_utc_timestamp(col("a"), "UTC"), "from_utc_timestamp", "UTC"),
      (next_day(col("a"), "Mon"), "next_day", "Mon"),
      (session_window(col("a"), "5 minutes"), "session_window", "5 minutes"),
      (to_date(col("a"), "yyyy-MM-dd"), "to_date", "yyyy-MM-dd"),
      (to_timestamp(col("a"), "yyyy-MM-dd"), "to_timestamp", "yyyy-MM-dd"),
      (to_utc_timestamp(col("a"), "UTC"), "to_utc_timestamp", "UTC"),
      (trunc(col("a"), "year"), "trunc", "year"),
      (try_to_date(col("a"), "yyyy-MM-dd"), "try_to_date", "yyyy-MM-dd"),
      (unix_timestamp(col("a"), "yyyy-MM-dd"), "unix_timestamp", "yyyy-MM-dd"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[1].literal.string == value)
    }

    for column in [curdate(), current_date(), current_time(), current_timestamp(),
      current_timezone(), localtimestamp(), now()]
    {
      #expect(column.expr.unresolvedFunction.arguments.isEmpty)
    }

    let preciseTime = current_time(6).expr
    #expect(preciseTime.unresolvedFunction.functionName == "current_time")
    #expect(preciseTime.unresolvedFunction.arguments[0].literal.integer == 6)

    let dateTruncated = date_trunc("hour", col("a")).expr
    #expect(dateTruncated.unresolvedFunction.functionName == "date_trunc")
    #expect(dateTruncated.unresolvedFunction.arguments[0].literal.string == "hour")

    let rounded = months_between(col("a"), col("b"), false).expr
    #expect(rounded.unresolvedFunction.functionName == "months_between")
    #expect(rounded.unresolvedFunction.arguments[2].literal.boolean == false)

    let added = timestamp_add("HOUR", col("a"), col("b")).expr
    #expect(added.unresolvedFunction.functionName == "timestampadd")
    #expect(added.unresolvedFunction.arguments[0].literal.string == "HOUR")

    let diffed = timestamp_diff("HOUR", col("a"), col("b")).expr
    #expect(diffed.unresolvedFunction.functionName == "timestampdiff")
    #expect(diffed.unresolvedFunction.arguments[0].literal.string == "HOUR")

    #expect(unix_timestamp().expr.unresolvedFunction.arguments.count == 1)

    for (column, count) in [
      (window(col("a"), "10 minutes"), 4),
      (window(col("a"), "10 minutes", "5 minutes"), 4),
      (window(col("a"), "10 minutes", "5 minutes", "1 minute"), 4),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == "window")
      #expect(expr.unresolvedFunction.arguments.count == count)
    }
  }

  @Test
  func selectCurrentDateTimeFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let rows = try await spark.range(1).select(
      curdate(), current_date(), current_timestamp(), current_timezone(), localtimestamp(), now(),
      unix_timestamp()
    ).collect()
    #expect(rows.count == 1)
    #expect(rows[0].length == 7)
    #expect(try await spark.range(1).select(year(current_date()) >= 2026).collect() == [Row(true)])
    await spark.stop()
  }

  @Test
  func selectDateFieldFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT DATE'2025-05-01' AS d, TIMESTAMP'2025-05-01 12:34:56' AS t")
    let fields = try await df.select(
      year(col("d")), quarter(col("d")), month(col("d")), day(col("d")), dayofmonth(col("d"))
    ).collect()
    #expect(fields == [Row(2025, 2, 5, 1, 1)])

    let days = try await df.select(
      dayofweek(col("d")), dayofyear(col("d")), weekday(col("d")), weekofyear(col("d")),
      dayname(col("d")), monthname(col("d"))
    ).collect()
    #expect(days == [Row(5, 121, 3, 18, "Thu", "May")])

    let times = try await df.select(
      hour(col("t")), minute(col("t")), second(col("t"))
    ).collect()
    #expect(times == [Row(12, 34, 56)])

    let extracted = try await df.select(
      extract(lit("YEAR"), col("d")), date_part(lit("MONTH"), col("d")),
      datepart(lit("DAY"), col("d"))
    ).collect()
    #expect(extracted == [Row(2025, 5, 1)])
    await spark.stop()
  }

  @Test
  func selectDateArithmeticFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT DATE'2025-05-01' AS d, TIMESTAMP'2025-05-01 12:34:56' AS t")
    let added = try await df.select(
      date_add(col("d"), 10).cast("string"), date_sub(col("d"), 1).cast("string"),
      dateadd(col("d"), lit(Int32(10))).cast("string"), add_months(col("d"), 2).cast("string")
    ).collect()
    #expect(added == [Row("2025-05-11", "2025-04-30", "2025-05-11", "2025-07-01")])

    let diffs = try await df.select(
      datediff(date_add(col("d"), 10), col("d")), date_diff(date_add(col("d"), 10), col("d")),
      months_between(add_months(col("d"), 2), col("d")),
      months_between(add_months(col("d"), 2), col("d"), false)
    ).collect()
    #expect(diffs == [Row(10, 10, 2.0, 2.0)])

    let boundaries = try await df.select(
      last_day(col("d")).cast("string"), next_day(col("d"), "Mon").cast("string"),
      next_day(col("d"), lit("Mon")).cast("string"), trunc(col("d"), "year").cast("string"),
      date_trunc("hour", col("t")).cast("string")
    ).collect()
    #expect(
      boundaries == [
        Row("2025-05-31", "2025-05-05", "2025-05-05", "2025-01-01", "2025-05-01 12:00:00")
      ])

    let timestamps = try await df.select(
      timestamp_add("DAY", lit(1), col("t")).cast("string"),
      timestamp_diff("HOUR", col("t"), timestamp_add("HOUR", lit(5), col("t")))
    ).collect()
    #expect(timestamps == [Row("2025-05-02 12:34:56", 5)])
    await spark.stop()
  }

  @Test
  func selectConversionFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT '2025-05-01' AS s, TIMESTAMP'2025-05-01 12:34:56' AS t")
    let dates = try await df.select(
      to_date(col("s")).cast("string"), to_date(col("s"), "yyyy-MM-dd").cast("string"),
      make_date(lit(2025), lit(5), lit(1)).cast("string")
    ).collect()
    #expect(dates == [Row("2025-05-01", "2025-05-01", "2025-05-01")])

    if await spark.version >= "4.1" {
      let tryDates = try await df.select(
        try_to_date(col("s")).cast("string"), try_to_date(col("s"), "yyyy-MM-dd").cast("string"),
        try_to_date(lit("abc"))
      ).collect()
      #expect(tryDates == [Row("2025-05-01", "2025-05-01", nil)])
    }

    let timestamps = try await df.select(
      to_timestamp(col("t")).cast("string"), to_timestamp(lit("2025-05-01"), "yyyy-MM-dd").cast("string"),
      try_to_timestamp(col("t")).cast("string"), try_to_timestamp(lit("abc")),
      to_timestamp_ltz(col("t")).cast("string"), to_timestamp_ntz(col("t")).cast("string")
    ).collect()
    #expect(
      timestamps == [
        Row(
          "2025-05-01 12:34:56", "2025-05-01 00:00:00", "2025-05-01 12:34:56", nil,
          "2025-05-01 12:34:56", "2025-05-01 12:34:56")
      ])

    let formatted = try await df.select(
      date_format(col("t"), "yyyy-MM-dd"),
      from_unixtime(to_unix_timestamp(col("s"), lit("yyyy-MM-dd"))),
      unix_timestamp(from_unixtime(lit(1234567890)))
    ).collect()
    #expect(formatted == [Row("2025-05-01", "2025-05-01 00:00:00", 1234567890)])

    let epochs = try await df.select(
      unix_date(make_date(lit(1970), lit(1), lit(2))), date_from_unix_date(lit(1)).cast("string"),
      unix_seconds(timestamp_seconds(lit(1234567890))),
      unix_millis(timestamp_millis(lit(1234567890123))),
      unix_micros(timestamp_micros(lit(1234567890123456)))
    ).collect()
    #expect(epochs == [Row(1, "1970-01-02", 1234567890, 1234567890123, 1234567890123456)])

    let timezones = try await df.select(
      from_utc_timestamp(col("t"), "Asia/Seoul").cast("string"),
      from_utc_timestamp(col("t"), lit("UTC")).cast("string"),
      to_utc_timestamp(col("t"), "Asia/Seoul").cast("string"),
      to_utc_timestamp(col("t"), lit("UTC")).cast("string"),
      convert_timezone(lit("UTC"), lit("Asia/Seoul"), to_timestamp_ntz(col("t"))).cast("string")
    ).collect()
    #expect(
      timezones == [
        Row(
          "2025-05-01 21:34:56", "2025-05-01 12:34:56", "2025-05-01 03:34:56",
          "2025-05-01 12:34:56", "2025-05-01 21:34:56")
      ])
    await spark.stop()
  }

  @Test
  func selectTimeFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    try await spark.conf.set("spark.sql.timeType.enabled", "true")
    if await spark.version >= "4.1" {
      let df = try await spark.sql("SELECT TIME'12:34:56' AS tm")
      let times = try await df.select(
        make_time(lit(12), lit(34), lit(56)).cast("string"), to_time(lit("12:34:56")).cast("string"),
        to_time(lit("12:34:56"), lit("HH:mm:ss")).cast("string"),
        try_to_time(lit("12:34:56")).cast("string"), try_to_time(lit("abc")),
        time_trunc(lit("HOUR"), col("tm")).cast("string"),
        time_diff(lit("HOUR"), make_time(lit(10), lit(0), lit(0)), col("tm"))
      ).collect()
      #expect(times == [Row("12:34:56", "12:34:56", "12:34:56", "12:34:56", nil, "12:00:00", 2)])

      let rows = try await df.select(current_time(), current_time(6)).collect()
      #expect(rows.count == 1)
      #expect(rows[0].length == 2)
    }
    if await spark.version >= "4.2" {
      let df = try await spark.sql("SELECT TIMESTAMP'2025-05-01 12:34:56' AS t")
      let rows = try await df.select(
        time_from_seconds(lit(3661)).cast("string"), time_from_millis(lit(1000)).cast("string"),
        time_from_micros(lit(1000000)).cast("string"),
        time_to_seconds(make_time(lit(1), lit(1), lit(1))).cast("long"),
        time_to_millis(make_time(lit(1), lit(1), lit(1))).cast("long"),
        time_to_micros(make_time(lit(1), lit(1), lit(1))).cast("long"),
        time_bucket(lit("10").cast("interval minute"), col("t")).cast("string")
      ).collect()
      #expect(
        rows == [
          Row("01:01:01", "00:00:01", "00:00:01", 3661, 3661000, 3661000000, "2025-05-01 12:30:00")
        ])
    }
    try await spark.conf.unset("spark.sql.timeType.enabled")
    await spark.stop()
  }
}
