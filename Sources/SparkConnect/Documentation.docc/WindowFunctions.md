# Window Functions

Rank rows and reach across rows inside a window frame.

## Overview

Window functions are used with ``Column/over(_:)`` and a ``WindowSpec``
built by ``Window``.

```swift
let w = Window.partitionBy("dept").orderBy("salary")
try await df.select(col("name"), rank().over(w), lag(col("salary"), 1).over(w))
  .show()
```

Ordinary aggregate functions can also be used over a window; see
<doc:AggregateFunctions>.

## Topics

### Ranking

- ``cume_dist()``
- ``dense_rank()``
- ``ntile(_:)``
- ``percent_rank()``
- ``rank()``
- ``row_number()``

### Value Access

- ``lag(_:_:)``
- ``lag(_:_:_:)``
- ``lead(_:_:)``
- ``lead(_:_:_:)``
- ``nth_value(_:_:)``

### Counters

- ``counter_diff(_:)``
- ``counter_diff(_:_:)``
