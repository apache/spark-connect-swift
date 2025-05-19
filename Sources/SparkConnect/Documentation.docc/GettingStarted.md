# Getting Started with SparkConnect

A step-by-step guide to get started with SparkConnect for Swift.

## Installation

Add SparkConnect to your Swift package dependencies:

```swift
dependencies: [
    .package(url: "https://github.com/apache/spark-connect-swift.git", from: "main")
]
```

Then, add it to your target dependencies:

```swift
targets: [
    .target(
        name: "YourApp",
        dependencies: ["SparkConnect"]
    )
]
```

## Prerequisites

- Swift 6.0 or later
- macOS 15+, iOS 18+, watchOS 11+, or tvOS 18+
- A running Apache Spark cluster with Spark Connect enabled

## Basic Usage

### 1. Create a SparkSession

```swift
import SparkConnect

let spark = try await SparkSession
    .builder
    .appName("MySwiftApp")
    .remote("sc://localhost:15002")
    .getOrCreate()
```

### 2. DataFrame Operations

```swift
// From a range
let df1 = try await spark.range(1, 10)

// Show data
try await df1.show()

// Select columns
try await df1.select("id").show()

let df2 = await df1.selectExpr("id", "id % 3 as value")
try await df2.show()

// Filter data
try await df2.filter("value == 0").show()

// Group and aggregate
try await df2
  .groupBy("value")
  .agg("count(*)", "sum(value)")
  .show()
```

### 3. SQL Queries

```swift
// Register a temporary view
try await df2.createOrReplaceTempView("v1")

// Run SQL Queries
let result = try await spark.sql("""
    SELECT id, sum(value) as value_sum
    FROM v1
    GROUP BY id
    ORDER BY value_sum DESC
""")

result.show()
```

### 4. Reading and Writing Data

```swift
// Read CSV
let csvDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("path/to/data.csv")

// Write ORC
csvDf.write
    .mode("overwrite")
    .orc("path/to/output")
```
