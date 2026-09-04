# Conditional and Predicate Functions

Branch on conditions, handle nulls, and match patterns.

## Overview

```swift
try await df.select(when(col("age") < 18, "minor").otherwise("adult"),
                    coalesce(col("nickname"), col("name")),
                    col("email").rlike("@example\\.com$"))
  .show()
```

## Topics

### Conditional Expressions

- ``coalesce(_:)``
- ``ifnull(_:_:)``
- ``nanvl(_:_:)``
- ``nullif(_:_:)``
- ``nullifzero(_:)``
- ``nvl(_:_:)``
- ``nvl2(_:_:_:)``
- ``when(_:_:)-(_,Column)``
- ``when(_:_:)-(_,SparkLiteral)``
- ``zeroifnull(_:)``

### Null and NaN Predicates

- ``equal_null(_:_:)``
- ``isnan(_:)``
- ``isnotnull(_:)``
- ``isnull(_:)``

### Pattern Matching

- ``ilike(_:_:)``
- ``ilike(_:_:_:)``
- ``like(_:_:)``
- ``like(_:_:_:)``
- ``regexp(_:_:)``
- ``regexp_like(_:_:)``
- ``rlike(_:_:)``
