# ``SparkConnect/DataFrameReader``

Interface for loading a ``DataFrame`` from external storage systems.

## Overview

`DataFrameReader` is obtained via ``SparkSession/read``. Configure it with ``format(_:)``, ``option(_:_:)``, and ``schema(_:)``, then call a format-specific loader (e.g., ``csv(_:)``, ``orc(_:)``) or the generic ``load()`` / ``load(_:)``.

```swift
// Format-specific reader
let csvDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("path/to/data.csv")

// Read from another DataFrame (CSV strings per row)
let lines: DataFrame = ...
let parsed = await spark.read.option("header", "true").csv(lines)

// Generic reader
let df = spark.read
    .format("orc")
    .load("path/to/data")
```

## Topics

### Configuration

- ``format(_:)``
- ``option(_:_:)``
- ``schema(_:)``

### Generic Loading

- ``load()``
- ``load(_:)``
- ``table(_:)``

### CSV

- ``csv(_:)``

### JSON

- ``json(_:)``

### XML

- ``xml(_:)``

### Parquet

- ``parquet(_:)``

### ORC

- ``orc(_:)``

### JDBC

- ``jdbc(_:_:_:)``
