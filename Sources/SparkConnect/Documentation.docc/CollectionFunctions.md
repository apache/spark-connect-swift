# Collection Functions

Work with array, map, and struct columns, including higher-order functions.

## Overview

```swift
try await df.select(array_contains(col("tags"), "swift"),
                    transform(col("nums"), { $0 * 2 }),
                    map_keys(col("attrs")))
  .show()
```

## Topics

### Array Construction

- ``array(_:)``
- ``array_repeat(_:_:)-(_,Int32)``
- ``array_repeat(_:_:)-(_,Column)``
- ``arrays_zip(_:)``
- ``sequence(_:_:)``
- ``sequence(_:_:_:)``
- ``stack(_:)``

### Array Access

- ``array_contains(_:_:)-(_,Column)``
- ``array_contains(_:_:)-(_,SparkLiteral)``
- ``array_position(_:_:)-(_,Column)``
- ``array_position(_:_:)-(_,SparkLiteral)``
- ``arrays_overlap(_:_:)``
- ``element_at(_:_:)-(_,Column)``
- ``element_at(_:_:)-(_,SparkLiteral)``
- ``get(_:_:)``
- ``slice(_:_:_:)-(_,Column,_)``
- ``slice(_:_:_:)-(_,Int32,_)``
- ``try_element_at(_:_:)``

### Array Modification

- ``array_append(_:_:)-(_,Column)``
- ``array_append(_:_:)-(_,SparkLiteral)``
- ``array_compact(_:)``
- ``array_distinct(_:)``
- ``array_insert(_:_:_:)``
- ``array_prepend(_:_:)-(_,Column)``
- ``array_prepend(_:_:)-(_,SparkLiteral)``
- ``array_remove(_:_:)-(_,Column)``
- ``array_remove(_:_:)-(_,SparkLiteral)``
- ``array_sort(_:)``
- ``array_sort(_:_:)``
- ``reverse(_:)``
- ``shuffle(_:)``
- ``shuffle(_:_:)``
- ``sort_array(_:)``
- ``sort_array(_:_:)``

### Array Summaries

- ``array_join(_:_:)``
- ``array_join(_:_:_:)``
- ``array_max(_:)``
- ``array_min(_:)``
- ``array_size(_:)``
- ``cardinality(_:)``
- ``size(_:)``

### Set Operations

- ``array_except(_:_:)``
- ``array_intersect(_:_:)``
- ``array_union(_:_:)``

### Maps

- ``map(_:)``
- ``map_concat(_:)``
- ``map_contains_key(_:_:)-(_,Column)``
- ``map_contains_key(_:_:)-(_,SparkLiteral)``
- ``map_entries(_:)``
- ``map_filter(_:_:)``
- ``map_from_arrays(_:_:)``
- ``map_from_entries(_:)``
- ``map_keys(_:)``
- ``map_values(_:)``
- ``map_zip_with(_:_:_:)``
- ``str_to_map(_:)``
- ``str_to_map(_:_:)``
- ``str_to_map(_:_:_:)``

### Structs

- ``named_struct(_:)``
- ``struct(_:)``

### Flattening

- ``concat(_:)``
- ``explode(_:)``
- ``explode_outer(_:)``
- ``flatten(_:)``
- ``inline(_:)``
- ``inline_outer(_:)``
- ``posexplode(_:)``
- ``posexplode_outer(_:)``

### Higher-Order Functions

- ``aggregate(_:_:_:)``
- ``aggregate(_:_:_:finish:)``
- ``exists(_:_:)``
- ``filter(_:_:)-(_,(Column)->Column)``
- ``filter(_:_:)-(_,(Column,Column)->Column)``
- ``forall(_:_:)``
- ``reduce(_:_:_:)``
- ``reduce(_:_:_:finish:)``
- ``transform(_:_:)-(_,(Column)->Column)``
- ``transform(_:_:)-(_,(Column,Column)->Column)``
- ``transform_keys(_:_:)``
- ``transform_values(_:_:)``
- ``zip_with(_:_:_:)``
