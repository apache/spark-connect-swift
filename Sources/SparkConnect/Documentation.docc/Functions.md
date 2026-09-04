# SQL Functions

The SQL functions available to ``Column`` expressions, grouped by category.

## Overview

`SparkConnect` exposes Spark SQL's function library as free Swift functions
that build ``Column`` expressions. Import the module and call them directly.

```swift
import SparkConnect

let spark = try await SparkSession.builder.getOrCreate()
let df = try await spark.sql("SELECT * FROM events")

try await df.select(col("user"), to_date(col("ts")).alias("day"))
  .groupBy("day")
  .agg(count_distinct(col("user")).alias("dau"))
  .orderBy(desc("dau"))
  .show()
```

Availability follows the server: functions marked as requiring a specific
Apache Spark version fail at runtime against older servers.

## Topics

### Building Columns

- <doc:ColumnFunctions>

### Aggregation

- <doc:AggregateFunctions>
- <doc:WindowFunctions>

### Scalar Functions by Data Type

- <doc:MathFunctions>
- <doc:StringFunctions>
- <doc:DateTimeFunctions>
- <doc:CollectionFunctions>
- <doc:SemiStructuredFunctions>
- <doc:GeospatialFunctions>

### Logic and Control Flow

- <doc:ConditionalFunctions>

### Approximation

- <doc:SketchFunctions>

### Everything Else

- <doc:MiscFunctions>
