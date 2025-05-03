# ``SparkConnect/SparkSession``

The entry point for SparkConnect functionality.

## Overview

`SparkSession` is the primary interaction point with Apache Spark. It provides an interface to create DataFrames, execute SQL queries, and manage cluster configurations.

### Creating a SparkSession

```swift
let spark = SparkSession.builder()
    .appName("My Swift Spark App")
    .remote("sc://localhost:15002")
    .build()
```

### Basic Usage

```swift
// Create a DataFrame from a range
let df = spark.range(1, 10)

// Execute SQL query
let result = spark.sql("SELECT * FROM table")

// Read data from files
let csvDf = spark.read.csv("path/to/file.csv")
```

## Topics

### Creating Sessions

- ``builder``
- ``stop()``

### DataFrame Operations

- ``range(_:_:_:)``
- ``sql(_:)``

### Data I/O

- ``read``

### Configuration

- ``conf``

### Catalog Operations

- ``catalog``

### Managing Operations

- ``addTag(_:)``
- ``removeTag(_:)``
- ``getTags()``
- ``clearTags()``
- ``interruptAll()``
- ``interruptTag(_:)``
- ``interruptOperation(_:)``
