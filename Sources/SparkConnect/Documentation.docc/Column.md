# ``SparkConnect/Column``

A column expression: the value that ``DataFrame`` operations select, filter,
and aggregate on.

## Overview

Create a `Column` with ``col(_:)``, `lit`, or ``expr(_:)``, then compose
it with operators and the methods below. The complete set of SQL functions that
produce columns is listed in <doc:Functions>.

```swift
let df = try await spark.sql("SELECT * FROM people")

try await df
  .select(col("name"), (col("age") + 1).alias("next_age"))
  .filter(col("age").between(20, 29) && col("name").startsWith("A"))
  .orderBy(col("age").desc())
  .show()
```

Swift's operators are overloaded for `Column`, so `col("a") + 1`,
`col("a") == "x"`, and `col("a") && col("b")` all build expressions rather than
evaluating locally.

## Topics

### Creating a Column

- ``Column/init(_:)``

### Arithmetic Operators

- ``Column/+(_:_:)-(Column,Column)``
- ``Column/+(_:_:)-(_,SparkLiteral)``
- ``Column/+(_:_:)-(SparkLiteral,_)``
- ``Column/-(_:)``
- ``Column/-(_:_:)-5jg0c``
- ``Column/-(_:_:)-3xuar``
- ``Column/-(_:_:)-6p3n1``
- ``Column/*(_:_:)-(Column,Column)``
- ``Column/*(_:_:)-(_,SparkLiteral)``
- ``Column/*(_:_:)-(SparkLiteral,_)``
- ``Column//(_:_:)-(Column,Column)``
- ``Column//(_:_:)-(_,SparkLiteral)``
- ``Column//(_:_:)-(SparkLiteral,_)``
- ``Column/%(_:_:)-(Column,Column)``
- ``Column/%(_:_:)-(_,SparkLiteral)``
- ``Column/%(_:_:)-(SparkLiteral,_)``

### Comparison Operators

- ``Column/==(_:_:)-(Column,Column)``
- ``Column/==(_:_:)-(_,SparkLiteral)``
- ``Column/==(_:_:)-(SparkLiteral,_)``
- ``Column/!=(_:_:)-(Column,Column)``
- ``Column/!=(_:_:)-(_,SparkLiteral)``
- ``Column/!=(_:_:)-(SparkLiteral,_)``
- ``Column/<(_:_:)-(Column,Column)``
- ``Column/<(_:_:)-(_,SparkLiteral)``
- ``Column/<(_:_:)-(SparkLiteral,_)``
- ``Column/<=(_:_:)-(Column,Column)``
- ``Column/<=(_:_:)-(_,SparkLiteral)``
- ``Column/<=(_:_:)-(SparkLiteral,_)``
- ``Column/>(_:_:)-(Column,Column)``
- ``Column/>(_:_:)-(_,SparkLiteral)``
- ``Column/>(_:_:)-(SparkLiteral,_)``
- ``Column/>=(_:_:)-(Column,Column)``
- ``Column/>=(_:_:)-(_,SparkLiteral)``
- ``Column/>=(_:_:)-(SparkLiteral,_)``
- ``Column/eqNullSafe(_:)-(Column)``
- ``Column/eqNullSafe(_:)-(SparkLiteral)``
- ``Column/between(_:_:)-(Column,Column)``
- ``Column/between(_:_:)-(Column,SparkLiteral)``
- ``Column/between(_:_:)-(SparkLiteral,Column)``
- ``Column/between(_:_:)-(SparkLiteral,SparkLiteral)``

### Logical Operators

- ``Column/&&(_:_:)-(Column,Column)``
- ``Column/&&(_:_:)-(_,SparkLiteral)``
- ``Column/&&(_:_:)-(SparkLiteral,_)``
- ``Column/||(_:_:)-(Column,Column)``
- ``Column/||(_:_:)-(_,SparkLiteral)``
- ``Column/||(_:_:)-(SparkLiteral,_)``
- ``Column/!(_:)``

### Bitwise Operations

- ``Column/bitwiseAND(_:)-(Column)``
- ``Column/bitwiseAND(_:)-(SparkLiteral)``
- ``Column/bitwiseOR(_:)-(Column)``
- ``Column/bitwiseOR(_:)-(SparkLiteral)``
- ``Column/bitwiseXOR(_:)-(Column)``
- ``Column/bitwiseXOR(_:)-(SparkLiteral)``

### Null Checks

- ``Column/isNull()``
- ``Column/isNotNull()``

### String Matching

- ``Column/contains(_:)-(Column)``
- ``Column/contains(_:)-(SparkLiteral)``
- ``Column/startsWith(_:)-(String)``
- ``Column/startsWith(_:)-(Column)``
- ``Column/endsWith(_:)-(String)``
- ``Column/endsWith(_:)-(Column)``
- ``Column/like(_:)``
- ``Column/ilike(_:)``
- ``Column/rlike(_:)``

### Substrings

- ``Column/substr(_:_:)-(Column,_)``
- ``Column/substr(_:_:)-(Int,_)``

### Set Membership

- ``Column/isin(_:)``

### Conditional Expressions

- ``Column/when(_:_:)-(_,Column)``
- ``Column/when(_:_:)-(_,SparkLiteral)``
- ``Column/otherwise(_:)-(Column)``
- ``Column/otherwise(_:)-(SparkLiteral)``

### Type Conversion

- ``Column/cast(_:)``

### Complex Type Access

- ``Column/getField(_:)``
- ``Column/getItem(_:)``

### Naming

- ``Column/alias(_:)``

### Sort Order

- ``Column/asc()``
- ``Column/ascNullsFirst()``
- ``Column/ascNullsLast()``
- ``Column/desc()``
- ``Column/descNullsFirst()``
- ``Column/descNullsLast()``

### Window Frames

- ``Column/over()``
- ``Column/over(_:)``
