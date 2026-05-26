# ``SparkConnect/DataFrameWriter``

Interface for writing a ``DataFrame`` to external storage systems.

## Overview

`DataFrameWriter` is obtained via ``DataFrame/write``. Configure it with ``format(_:)``, ``mode(_:)``, ``option(_:_:)``, and partitioning helpers, then call a format-specific writer (e.g., ``orc(_:)``, ``csv(_:)``), ``save()``, ``saveAsTable(_:)``, or ``insertInto(_:)``.

```swift
// Format-specific writer
try await df.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .orc("path/to/output")

// Save as a managed table
try await df.write
    .mode("append")
    .saveAsTable("events")
```

## Topics

### Configuration

- ``format(_:)``
- ``mode(_:)``
- ``option(_:_:)``
- ``partitionBy(_:)``
- ``bucketBy(numBuckets:_:)``
- ``sortBy(_:)``
- ``clusterBy(_:)``

### Saving Data

- ``save()``
- ``save(_:)``
- ``saveAsTable(_:)``
- ``insertInto(_:)``

### CSV

- ``csv(_:)``

### JSON

- ``json(_:)``

### XML

- ``xml(_:)``

### ORC

- ``orc(_:)``

### Parquet

- ``parquet(_:)``

### Text

- ``text(_:)``

### JDBC

- ``jdbc(_:_:_:)``
