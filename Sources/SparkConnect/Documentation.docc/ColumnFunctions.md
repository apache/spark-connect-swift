# Column Functions

Build column references, literals, and sort orders.

## Overview

Every SQL function in this library takes and returns a ``Column``. These
functions create the columns you start from: a reference to a named column, a
literal value, an arbitrary SQL expression, or a sort order for
``DataFrame/orderBy(_:)``.

```swift
let df = try await spark.range(10)
try await df.select(col("id"), lit("swift").alias("lang"))
  .orderBy(desc("id"))
  .show()
```

## Topics

### Column References

- ``col(_:)``
- ``column(_:)``
- ``expr(_:)``

### Literals

- ``lit(_:)-(Bool)``
- ``lit(_:)-(Double)``
- ``lit(_:)-(Float)``
- ``lit(_:)-(Int)``
- ``lit(_:)-(Int16)``
- ``lit(_:)-(Int32)``
- ``lit(_:)-(Int64)``
- ``lit(_:)-(Int8)``
- ``lit(_:)-(LocalTime)``
- ``lit(_:)-(String)``
- ``lit(_:)-(TimestampNanos)``

### Sort Orders

- ``asc(_:)``
- ``asc_nulls_first(_:)``
- ``asc_nulls_last(_:)``
- ``desc(_:)``
- ``desc_nulls_first(_:)``
- ``desc_nulls_last(_:)``

### Join Hints

- ``broadcast(_:)``
