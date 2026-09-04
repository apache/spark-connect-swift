# Math Functions

Arithmetic, rounding, trigonometry, bitwise, and vector operations.

## Overview

```swift
try await df.select(round(col("value"), 2), sqrt(col("value")), abs(col("delta")))
  .show()
```

## Topics

### Arithmetic

- ``abs(_:)``
- ``greatest(_:_:_:)``
- ``least(_:_:_:)``
- ``negative(_:)``
- ``pmod(_:_:)``
- ``positive(_:)``
- ``pow(_:_:)``
- ``power(_:_:)``
- ``sign(_:)``
- ``signum(_:)``
- ``try_add(_:_:)``
- ``try_divide(_:_:)``
- ``try_mod(_:_:)``
- ``try_multiply(_:_:)``
- ``try_subtract(_:_:)``

### Rounding

- ``bround(_:)``
- ``bround(_:_:)``
- ``ceil(_:)``
- ``ceil(_:_:)``
- ``ceiling(_:)``
- ``ceiling(_:_:)``
- ``floor(_:)``
- ``floor(_:_:)``
- ``rint(_:)``
- ``round(_:)``
- ``round(_:_:)``

### Exponential and Logarithmic

- ``cbrt(_:)``
- ``e()``
- ``exp(_:)``
- ``expm1(_:)``
- ``ln(_:)``
- ``log(_:)``
- ``log(_:_:)``
- ``log10(_:)``
- ``log1p(_:)``
- ``log2(_:)``
- ``sqrt(_:)``

### Trigonometric

- ``acos(_:)``
- ``acosh(_:)``
- ``asin(_:)``
- ``asinh(_:)``
- ``atan(_:)``
- ``atan2(_:_:)``
- ``atanh(_:)``
- ``cos(_:)``
- ``cosh(_:)``
- ``cot(_:)``
- ``csc(_:)``
- ``degrees(_:)``
- ``hypot(_:_:)``
- ``pi()``
- ``radians(_:)``
- ``sec(_:)``
- ``sin(_:)``
- ``sinh(_:)``
- ``tan(_:)``
- ``tanh(_:)``

### Number Bases and Conversion

- ``bin(_:)``
- ``conv(_:_:_:)``
- ``factorial(_:)``
- ``hex(_:)``
- ``unhex(_:)``
- ``width_bucket(_:_:_:_:)``

### Random

- ``rand()``
- ``rand(_:)``
- ``randn()``
- ``randn(_:)``
- ``uniform(_:_:)``
- ``uniform(_:_:_:)``

### Bitwise

- ``bit_count(_:)``
- ``bit_get(_:_:)``
- ``bitwise_not(_:)``
- ``getbit(_:_:)``
- ``shiftleft(_:_:)``
- ``shiftright(_:_:)``
- ``shiftrightunsigned(_:_:)``

### Vectors

- ``vector_cosine_similarity(_:_:)``
- ``vector_inner_product(_:_:)``
- ``vector_l2_distance(_:_:)``
- ``vector_norm(_:)``
- ``vector_norm(_:_:)-(_,Column)``
- ``vector_norm(_:_:)-(_,Float)``
- ``vector_normalize(_:)``
- ``vector_normalize(_:_:)-(_,Column)``
- ``vector_normalize(_:_:)-(_,Float)``
