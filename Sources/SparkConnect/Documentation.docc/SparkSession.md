# ``SparkConnect/SparkSession``

The entry point for SparkConnect functionality.

## Overview

`SparkSession` is the primary interaction point with Apache Spark. It provides an interface to create DataFrames, execute SQL queries, and manage cluster configurations.

### Creating a SparkSession

```swift
let spark = try await SparkSession
    .builder
    .appName("MySwiftApp")
    .remote("sc://localhost:15002")
    .getOrCreate()
```

### Basic Usage

```swift
// Create a DataFrame from a range
let df = try await spark.range(1, 10)

// Execute SQL query
let result = try await spark.sql("SELECT * FROM table")

// Read data from files
let csvDf = spark.read.csv("path/to/file.csv")
```

## Topics

### Creating Sessions

- ``builder``
- ``newSession()``
- ``stop()``

### Session Information

- ``version``

### DataFrame Operations

- ``emptyDataFrame``
- ``range(_:)``
- ``range(_:_:_:)``
- ``sql(_:)``
- ``table(_:)``

### Data I/O

- ``read``
- ``readStream``

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

### Artifacts & External Commands

- ``addArtifact(_:)``
- ``addArtifacts(_:)``
- ``executeCommand(_:_:_:)``

### Streaming

- ``streams``

### Utilities

- ``time(_:)``
