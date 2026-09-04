# Geospatial Functions

Construct and inspect GEOMETRY and GEOGRAPHY values.

## Overview

Geospatial support requires Apache Spark 4.2.0 or later and is controlled
by the `spark.sql.geospatial.enabled` configuration.

```swift
try await df.select(st_srid(st_geomfromwkb(col("wkb"), 4326))).show()
```

## Topics

### Construction

- ``st_geogfromwkb(_:)``
- ``st_geomfromwkb(_:)``
- ``st_geomfromwkb(_:_:)-(_,Column)``
- ``st_geomfromwkb(_:_:)-(_,Int32)``

### Serialization

- ``st_asbinary(_:)``
- ``st_asbinary(_:_:)-(_,Column)``
- ``st_asbinary(_:_:)-(_,String)``

### Spatial Reference Systems

- ``st_setsrid(_:_:)-(_,Column)``
- ``st_setsrid(_:_:)-(_,Int32)``
- ``st_srid(_:)``
