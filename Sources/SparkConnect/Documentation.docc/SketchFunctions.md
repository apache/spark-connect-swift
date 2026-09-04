# Sketch Functions

Read and combine HyperLogLog, Theta, KLL, and Tuple sketches.

## Overview

Sketches are compact, mergeable summaries used for approximate distinct
counts, set operations, and quantiles. Build them with the sketch aggregates in
<doc:AggregateFunctions>, then use the scalar functions here to merge sketches
and read estimates out of them.

```swift
try await df.groupBy("day")
  .agg(hll_sketch_agg(col("user_id")).alias("sk"))
  .select(hll_sketch_estimate(col("sk")))
  .show()
```

## Topics

### HyperLogLog

- ``hll_sketch_estimate(_:)``
- ``hll_union(_:_:)``
- ``hll_union(_:_:_:)``

### Theta

- ``theta_difference(_:_:)``
- ``theta_intersection(_:_:)``
- ``theta_sketch_estimate(_:)``
- ``theta_union(_:_:)``
- ``theta_union(_:_:_:)-(_,_,Column)``
- ``theta_union(_:_:_:)-(_,_,Int32)``

### KLL

- ``kll_sketch_get_n_bigint(_:)``
- ``kll_sketch_get_n_double(_:)``
- ``kll_sketch_get_n_float(_:)``
- ``kll_sketch_get_quantile_bigint(_:_:)``
- ``kll_sketch_get_quantile_double(_:_:)``
- ``kll_sketch_get_quantile_float(_:_:)``
- ``kll_sketch_get_rank_bigint(_:_:)``
- ``kll_sketch_get_rank_double(_:_:)``
- ``kll_sketch_get_rank_float(_:_:)``
- ``kll_sketch_merge_bigint(_:_:)``
- ``kll_sketch_merge_double(_:_:)``
- ``kll_sketch_merge_float(_:_:)``
- ``kll_sketch_to_string_bigint(_:)``
- ``kll_sketch_to_string_double(_:)``
- ``kll_sketch_to_string_float(_:)``

### Tuple

- ``tuple_difference_double(_:_:)``
- ``tuple_difference_integer(_:_:)``
- ``tuple_difference_theta_double(_:_:)``
- ``tuple_difference_theta_integer(_:_:)``
- ``tuple_intersection_double(_:_:)``
- ``tuple_intersection_double(_:_:mode:)``
- ``tuple_intersection_integer(_:_:)``
- ``tuple_intersection_integer(_:_:mode:)``
- ``tuple_intersection_theta_double(_:_:)``
- ``tuple_intersection_theta_double(_:_:mode:)``
- ``tuple_intersection_theta_integer(_:_:)``
- ``tuple_intersection_theta_integer(_:_:mode:)``
- ``tuple_sketch_estimate_double(_:)``
- ``tuple_sketch_estimate_integer(_:)``
- ``tuple_sketch_summary_double(_:)``
- ``tuple_sketch_summary_double(_:mode:)``
- ``tuple_sketch_summary_integer(_:)``
- ``tuple_sketch_summary_integer(_:mode:)``
- ``tuple_sketch_theta_double(_:)``
- ``tuple_sketch_theta_integer(_:)``
- ``tuple_union_double(_:_:lgNomEntries:mode:)-(_,_,Column,_)``
- ``tuple_union_double(_:_:lgNomEntries:mode:)-(_,_,Int32,_)``
- ``tuple_union_integer(_:_:lgNomEntries:mode:)-(_,_,Column,_)``
- ``tuple_union_integer(_:_:lgNomEntries:mode:)-(_,_,Int32,_)``
- ``tuple_union_theta_double(_:_:lgNomEntries:mode:)-(_,_,Column,_)``
- ``tuple_union_theta_double(_:_:lgNomEntries:mode:)-(_,_,Int32,_)``
- ``tuple_union_theta_integer(_:_:lgNomEntries:mode:)-(_,_,Column,_)``
- ``tuple_union_theta_integer(_:_:lgNomEntries:mode:)-(_,_,Int32,_)``
