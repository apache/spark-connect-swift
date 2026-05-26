# ``SparkConnect/Catalog``

Interface for managing catalogs, databases, tables, views, functions, partitions, and caching.

## Overview

`Catalog` is accessible via ``SparkSession/catalog`` and provides a programmatic way to query and manipulate the metadata layer of a Spark cluster — including catalog selection, database lifecycle, table/view discovery, function lookup, partition recovery, and table-level caching and analysis.

```swift
let spark = try await SparkSession.builder.getOrCreate()

// Discover and switch
let dbs = try await spark.catalog.listDatabases()
try await spark.catalog.setCurrentDatabase("analytics")

// Create / drop
try await spark.catalog.createDatabase("demo", ifNotExists: true)
try await spark.catalog.dropDatabase("demo", ifExists: true, cascade: true)

// Inspect a function
let fn = try await spark.catalog.getFunction("to_date")
```

## Topics

### Catalog Management

- ``currentCatalog()``
- ``setCurrentCatalog(_:)``
- ``listCatalogs(pattern:)``

### Database Operations

- ``currentDatabase()``
- ``setCurrentDatabase(_:)``
- ``createDatabase(_:ifNotExists:properties:)``
- ``dropDatabase(_:ifExists:cascade:)``
- ``listDatabases(pattern:)``
- ``getDatabase(_:)``
- ``databaseExists(_:)``

### Table Operations

- ``listTables(dbName:pattern:)``
- ``getTable(_:)``
- ``getTableProperties(_:)``
- ``getCreateTableString(_:asSerde:)``
- ``tableExists(_:)``
- ``tableExists(_:_:)``
- ``createTable(_:_:source:description:options:)``
- ``dropTable(_:ifExists:purge:)``
- ``truncateTable(_:)``

### View Operations

- ``listViews(dbName:pattern:)``
- ``dropView(_:ifExists:)``
- ``dropTempView(_:)``
- ``dropGlobalTempView(_:)``

### Function Operations

- ``listFunctions(dbName:pattern:)``
- ``getFunction(_:)``
- ``getFunction(_:_:)``
- ``functionExists(_:)``
- ``functionExists(_:_:)``

### Column & Partition Operations

- ``listColumns(_:)``
- ``listPartitions(_:)``
- ``recoverPartitions(_:)``

### Caching

- ``cacheTable(_:_:)``
- ``isCached(_:)``
- ``uncacheTable(_:)``
- ``clearCache()``
- ``refreshTable(_:)``
- ``refreshByPath(_:)``

### Table Analysis

- ``analyzeTable(_:noScan:)``
