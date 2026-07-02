# ``SparkConnect``

Swift implementation of Apache Spark Connect client for distributed data processing.

## Overview

SparkConnect is a modern Swift library that provides a native interface to Apache Spark clusters using the Spark Connect protocol. This library enables Swift developers to leverage the power of Apache Spark for distributed data processing, machine learning, and analytical workloads directly from their Swift applications.

### Key Features

- Native Swift API for Apache Spark operations
- Support for DataFrame and SQL operations
- Support for grouped data operations and aggregations
- Efficient data serialization using Arrow format

## Topics

### Getting Started

- <doc:GettingStarted>
- <doc:Examples>
- <doc:SecurityGuide>

### Sessions

- ``SparkSession``

### DataFrames

- ``DataFrame``
- ``GroupedData``
- ``Row``
- ``StorageLevel``

### Data I/O

- ``DataFrameReader``
- ``DataFrameWriter``
- ``MergeIntoWriter``

### Catalog & Configuration

- ``Catalog``
- ``RuntimeConf``

### Streaming

- ``DataStreamReader``
- ``DataStreamWriter``
- ``StreamingQuery``
- ``StreamingQueryManager``
