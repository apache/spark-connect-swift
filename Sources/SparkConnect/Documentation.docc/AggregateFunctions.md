# Aggregate Functions

Summarize groups of rows with counts, statistics, sketches, and collections.

## Overview

Aggregate functions reduce many rows to one value. Use them with
``DataFrame/groupBy(_:)`` or ``GroupedData/agg(_:_:)``, or as window functions with
``Column/over(_:)``.

```swift
try await df.groupBy("dept")
  .agg(count(lit(1)).alias("n"), avg(col("salary")), stddev(col("salary")))
  .show()
```

Sketch-based approximate aggregates are listed here as well; the scalar
functions that read their results live in <doc:SketchFunctions>.

## Topics

### General

- ``any_value(_:)``
- ``any_value(_:_:)``
- ``avg(_:)``
- ``count(_:)``
- ``countDistinct(_:_:)``
- ``count_distinct(_:_:)``
- ``count_if(_:)``
- ``first(_:)``
- ``first(_:_:)``
- ``first_value(_:)``
- ``first_value(_:_:)``
- ``grouping(_:)``
- ``grouping_id(_:)``
- ``last(_:)``
- ``last(_:_:)``
- ``last_value(_:)``
- ``last_value(_:_:)``
- ``max(_:)``
- ``max_by(_:_:)``
- ``mean(_:)``
- ``median(_:)``
- ``min(_:)``
- ``min_by(_:_:)``
- ``mode(_:)``
- ``mode(_:_:)``
- ``product(_:)``
- ``sum(_:)``
- ``sumDistinct(_:)``
- ``sum_distinct(_:)``
- ``try_avg(_:)``
- ``try_sum(_:)``

### Statistical

- ``approx_count_distinct(_:)``
- ``approx_count_distinct(_:_:)``
- ``approx_percentile(_:_:_:)-(_,Double,_)``
- ``approx_percentile(_:_:_:)-(_,[Double],_)``
- ``corr(_:_:)``
- ``count_min_sketch(_:_:_:)``
- ``count_min_sketch(_:_:_:_:)``
- ``covar_pop(_:_:)``
- ``covar_samp(_:_:)``
- ``histogram_numeric(_:_:)``
- ``kurtosis(_:)``
- ``percentile(_:_:_:)-(_,Double,_)``
- ``percentile(_:_:_:)-(_,[Double],_)``
- ``percentile_approx(_:_:_:)``
- ``skewness(_:)``
- ``std(_:)``
- ``stddev(_:)``
- ``stddev_pop(_:)``
- ``stddev_samp(_:)``
- ``var_pop(_:)``
- ``var_samp(_:)``
- ``variance(_:)``

### Regression

- ``regr_avgx(_:_:)``
- ``regr_avgy(_:_:)``
- ``regr_count(_:_:)``
- ``regr_intercept(_:_:)``
- ``regr_r2(_:_:)``
- ``regr_slope(_:_:)``
- ``regr_sxx(_:_:)``
- ``regr_sxy(_:_:)``
- ``regr_syy(_:_:)``

### Boolean and Bitwise

- ``bit_and(_:)``
- ``bit_or(_:)``
- ``bit_xor(_:)``
- ``bitmap_and_agg(_:)``
- ``bitmap_construct_agg(_:)``
- ``bitmap_or_agg(_:)``
- ``bool_and(_:)``
- ``bool_or(_:)``
- ``every(_:)``
- ``some(_:)``

### Collections

- ``array_agg(_:)``
- ``collect_list(_:)``
- ``collect_set(_:)``
- ``collect_union(_:)``
- ``listagg(_:)``
- ``listagg(_:_:)``
- ``listagg_distinct(_:)``
- ``listagg_distinct(_:_:)``
- ``string_agg(_:)``
- ``string_agg(_:_:)``
- ``string_agg_distinct(_:)``
- ``string_agg_distinct(_:_:)``

### Sketches

- ``hll_sketch_agg(_:)``
- ``hll_sketch_agg(_:_:)-(_,Column)``
- ``hll_sketch_agg(_:_:)-(_,Int32)``
- ``hll_union_agg(_:)``
- ``hll_union_agg(_:_:)-(_,Bool)``
- ``hll_union_agg(_:_:)-(_,Column)``
- ``kll_merge_agg_bigint(_:)``
- ``kll_merge_agg_bigint(_:_:)-(_,Column)``
- ``kll_merge_agg_bigint(_:_:)-(_,Int32)``
- ``kll_merge_agg_double(_:)``
- ``kll_merge_agg_double(_:_:)-(_,Column)``
- ``kll_merge_agg_double(_:_:)-(_,Int32)``
- ``kll_merge_agg_float(_:)``
- ``kll_merge_agg_float(_:_:)-(_,Column)``
- ``kll_merge_agg_float(_:_:)-(_,Int32)``
- ``kll_sketch_agg_bigint(_:)``
- ``kll_sketch_agg_bigint(_:_:)-(_,Column)``
- ``kll_sketch_agg_bigint(_:_:)-(_,Int32)``
- ``kll_sketch_agg_double(_:)``
- ``kll_sketch_agg_double(_:_:)-(_,Column)``
- ``kll_sketch_agg_double(_:_:)-(_,Int32)``
- ``kll_sketch_agg_float(_:)``
- ``kll_sketch_agg_float(_:_:)-(_,Column)``
- ``kll_sketch_agg_float(_:_:)-(_,Int32)``
- ``theta_intersection_agg(_:)``
- ``theta_sketch_agg(_:)``
- ``theta_sketch_agg(_:_:)-(_,Column)``
- ``theta_sketch_agg(_:_:)-(_,Int32)``
- ``theta_union_agg(_:)``
- ``theta_union_agg(_:_:)-(_,Column)``
- ``theta_union_agg(_:_:)-(_,Int32)``
- ``tuple_intersection_agg_double(_:)``
- ``tuple_intersection_agg_double(_:mode:)``
- ``tuple_intersection_agg_integer(_:)``
- ``tuple_intersection_agg_integer(_:mode:)``
- ``tuple_sketch_agg_double(_:_:lgNomEntries:mode:)-(_,_,Column,_)``
- ``tuple_sketch_agg_double(_:_:lgNomEntries:mode:)-(_,_,Int32,_)``
- ``tuple_sketch_agg_integer(_:_:lgNomEntries:mode:)-(_,_,Column,_)``
- ``tuple_sketch_agg_integer(_:_:lgNomEntries:mode:)-(_,_,Int32,_)``
- ``tuple_union_agg_double(_:lgNomEntries:mode:)-(_,Column,_)``
- ``tuple_union_agg_double(_:lgNomEntries:mode:)-(_,Int32,_)``
- ``tuple_union_agg_integer(_:lgNomEntries:mode:)-(_,Column,_)``
- ``tuple_union_agg_integer(_:lgNomEntries:mode:)-(_,Int32,_)``

### Vectors

- ``vector_avg(_:)``
- ``vector_sum(_:)``

### Variant

- ``schema_of_variant_agg(_:)``
