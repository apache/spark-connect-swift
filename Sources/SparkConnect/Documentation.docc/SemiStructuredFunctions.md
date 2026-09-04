# Semi-Structured Data Functions

Parse and generate JSON, XML, CSV, and VARIANT values.

## Overview

```swift
try await df.select(from_json(col("payload"), "id INT, name STRING"),
                    to_json(col("record")),
                    variant_get(parse_json(col("raw")), "$.id", "int"))
  .show()
```

## Topics

### JSON

- ``from_json(_:_:_:)-(_,Column,_)``
- ``from_json(_:_:_:)-(_,String,_)``
- ``from_json(_:_:_:)-(_,StructType,_)``
- ``get_json_object(_:_:)``
- ``json_array_length(_:)``
- ``json_object_keys(_:)``
- ``json_tuple(_:_:)``
- ``schema_of_json(_:_:)-(Column,_)``
- ``schema_of_json(_:_:)-(String,_)``
- ``to_json(_:_:)``

### XML

- ``from_xml(_:_:_:)-(_,Column,_)``
- ``from_xml(_:_:_:)-(_,String,_)``
- ``from_xml(_:_:_:)-(_,StructType,_)``
- ``schema_of_xml(_:_:)-(Column,_)``
- ``schema_of_xml(_:_:)-(String,_)``
- ``to_xml(_:_:)``
- ``xpath(_:_:)``
- ``xpath_boolean(_:_:)``
- ``xpath_double(_:_:)``
- ``xpath_float(_:_:)``
- ``xpath_int(_:_:)``
- ``xpath_long(_:_:)``
- ``xpath_number(_:_:)``
- ``xpath_short(_:_:)``
- ``xpath_string(_:_:)``

### CSV

- ``from_csv(_:_:_:)-(_,Column,_)``
- ``from_csv(_:_:_:)-(_,String,_)``
- ``schema_of_csv(_:_:)-(Column,_)``
- ``schema_of_csv(_:_:)-(String,_)``
- ``to_csv(_:_:)``

### Variant

- ``is_valid_variant(_:)``
- ``is_variant_null(_:)``
- ``parse_json(_:)``
- ``schema_of_variant(_:)``
- ``to_variant_object(_:)``
- ``try_parse_json(_:)``
- ``try_variant_array_append(_:_:_:)-(_,Column,_)``
- ``try_variant_array_append(_:_:_:)-(_,String,_)``
- ``try_variant_get(_:_:_:)-(_,Column,_)``
- ``try_variant_get(_:_:_:)-(_,String,_)``
- ``try_variant_insert(_:_:_:)-(_,Column,_)``
- ``try_variant_insert(_:_:_:)-(_,String,_)``
- ``try_variant_set(_:_:_:_:)-(_,Column,_,_)``
- ``try_variant_set(_:_:_:_:)-(_,String,_,_)``
- ``variant_array_append(_:_:_:)-(_,Column,_)``
- ``variant_array_append(_:_:_:)-(_,String,_)``
- ``variant_get(_:_:_:)-(_,Column,_)``
- ``variant_get(_:_:_:)-(_,String,_)``
- ``variant_insert(_:_:_:)-(_,Column,_)``
- ``variant_insert(_:_:_:)-(_,String,_)``
- ``variant_set(_:_:_:_:)-(_,Column,_,_)``
- ``variant_set(_:_:_:_:)-(_,String,_,_)``
- ``variant_strip_nulls(_:_:)``
