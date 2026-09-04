# Date and Time Functions

Build, parse, extract, shift, and bucket dates, times, and timestamps.

## Overview

```swift
try await df.select(current_date(), date_add(col("d"), 7),
                    date_format(col("ts"), "yyyy-MM-dd HH:mm"))
  .show()
```

## Topics

### Current Date and Time

- ``curdate()``
- ``current_date()``
- ``current_time()``
- ``current_time(_:)``
- ``current_timestamp()``
- ``current_timezone()``
- ``localtimestamp()``
- ``now()``

### Field Extraction

- ``date_part(_:_:)``
- ``datepart(_:_:)``
- ``day(_:)``
- ``dayname(_:)``
- ``dayofmonth(_:)``
- ``dayofweek(_:)``
- ``dayofyear(_:)``
- ``extract(_:_:)``
- ``hour(_:)``
- ``last_day(_:)``
- ``minute(_:)``
- ``month(_:)``
- ``monthname(_:)``
- ``next_day(_:_:)-(_,Column)``
- ``next_day(_:_:)-(_,String)``
- ``quarter(_:)``
- ``second(_:)``
- ``weekday(_:)``
- ``weekofyear(_:)``
- ``year(_:)``

### Arithmetic

- ``add_months(_:_:)-(_,Column)``
- ``add_months(_:_:)-(_,Int32)``
- ``date_add(_:_:)-(_,Column)``
- ``date_add(_:_:)-(_,Int32)``
- ``date_diff(_:_:)``
- ``date_sub(_:_:)-(_,Column)``
- ``date_sub(_:_:)-(_,Int32)``
- ``date_trunc(_:_:)``
- ``dateadd(_:_:)``
- ``datediff(_:_:)``
- ``months_between(_:_:)``
- ``months_between(_:_:_:)``
- ``time_diff(_:_:_:)``
- ``time_trunc(_:_:)``
- ``timestamp_add(_:_:_:)``
- ``timestamp_diff(_:_:_:)``
- ``trunc(_:_:)``

### Construction

- ``make_date(_:_:_:)``
- ``make_dt_interval(days:hours:mins:secs:)``
- ``make_interval(years:months:weeks:days:hours:mins:secs:)``
- ``make_time(_:_:_:)``
- ``make_timestamp(_:_:_:_:_:_:)``
- ``make_timestamp(_:_:_:_:_:_:_:)``
- ``make_timestamp_ltz(_:_:_:_:_:_:)``
- ``make_timestamp_ltz(_:_:_:_:_:_:_:)``
- ``make_timestamp_ntz(_:_:_:_:_:_:)``
- ``make_ym_interval(years:months:)``
- ``try_make_interval(years:months:weeks:days:hours:mins:secs:)``
- ``try_make_timestamp(_:_:_:_:_:_:)``
- ``try_make_timestamp(_:_:_:_:_:_:_:)``
- ``try_make_timestamp_ltz(_:_:_:_:_:_:)``
- ``try_make_timestamp_ltz(_:_:_:_:_:_:_:)``
- ``try_make_timestamp_ntz(_:_:_:_:_:_:)``

### Parsing and Formatting

- ``date_format(_:_:)``
- ``from_unixtime(_:)``
- ``from_unixtime(_:_:)``
- ``to_date(_:)``
- ``to_date(_:_:)``
- ``to_time(_:)``
- ``to_time(_:_:)``
- ``to_timestamp(_:)``
- ``to_timestamp(_:_:)``
- ``to_timestamp_ltz(_:)``
- ``to_timestamp_ltz(_:_:)``
- ``to_timestamp_ntz(_:)``
- ``to_timestamp_ntz(_:_:)``
- ``to_unix_timestamp(_:)``
- ``to_unix_timestamp(_:_:)``
- ``try_to_date(_:)``
- ``try_to_date(_:_:)``
- ``try_to_time(_:)``
- ``try_to_time(_:_:)``
- ``try_to_timestamp(_:)``
- ``try_to_timestamp(_:_:)``
- ``unix_timestamp()``
- ``unix_timestamp(_:)``
- ``unix_timestamp(_:_:)``

### Epoch Conversion

- ``date_from_unix_date(_:)``
- ``time_from_micros(_:)``
- ``time_from_millis(_:)``
- ``time_from_seconds(_:)``
- ``time_to_micros(_:)``
- ``time_to_millis(_:)``
- ``time_to_seconds(_:)``
- ``timestamp_micros(_:)``
- ``timestamp_millis(_:)``
- ``timestamp_nanos(_:)``
- ``timestamp_seconds(_:)``
- ``unix_date(_:)``
- ``unix_micros(_:)``
- ``unix_millis(_:)``
- ``unix_nanos(_:)``
- ``unix_seconds(_:)``

### Time Zones

- ``convert_timezone(_:_:)``
- ``convert_timezone(_:_:_:)``
- ``from_utc_timestamp(_:_:)-(_,Column)``
- ``from_utc_timestamp(_:_:)-(_,String)``
- ``to_utc_timestamp(_:_:)-(_,Column)``
- ``to_utc_timestamp(_:_:)-(_,String)``

### Windowing

- ``session_window(_:_:)-(_,Column)``
- ``session_window(_:_:)-(_,String)``
- ``time_bucket(_:_:)``
- ``time_bucket(_:_:_:)``
- ``window(_:_:)``
- ``window(_:_:_:)``
- ``window(_:_:_:_:)``
- ``window_time(_:)``
