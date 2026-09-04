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
- ``RuntimeConf``

### DataFrames

- ``DataFrame``
- ``GroupedData``
- ``DataFrameNaFunctions``
- ``DataFrameStatFunctions``
- ``Observation``
- ``Row``
- ``RowSchema``
- ``StorageLevel``

### Expressions

- ``Column``
- ``SparkLiteral``
- <doc:Functions>

### Window Frames

- ``Window``
- ``WindowSpec``

### Data Types

- ``DataType``
- ``StructType``
- ``StructField``
- ``UserDefinedType``
- ``LocalTime``
- ``TimestampNanos``
- ``YearMonthIntervalField``
- ``DayTimeIntervalField``

### Data I/O

- ``DataFrameReader``
- ``DataFrameWriter``
- ``DataFrameWriterV2``
- ``MergeIntoWriter``
- ``WhenMatched``
- ``WhenNotMatched``
- ``WhenNotMatchedBySource``

### Catalog

- ``Catalog``
- ``CatalogMetadata``
- ``Database``
- ``SparkTable``
- ``CatalogColumn``
- ``Function``
- ``TablePartition``

### Streaming

- ``DataStreamReader``
- ``DataStreamWriter``
- ``Trigger``
- ``StreamingQuery``
- ``StreamingQueryManager``
- ``StreamingQueryException``
- ``StreamingQueryStatus``
- ``StreamingQueryProgress``
- ``SourceProgress``
- ``SinkProgress``
- ``StateOperatorProgress``

### Error Handling

- ``SparkConnectError``
- ``~=(_:_:)``

### Low-Level and Utility APIs

- ``SparkConnectClient``
- ``CaseInsensitiveDictionary``
- ``ErrorUtils``
- ``ProtoUtils``
- ``SparkFileUtils``
- ``CRC32``
- ``SHA256``
