# Misc Functions

Session metadata, hashing, encryption, partition transforms, and UDF calls.

## Overview

```swift
try await spark.range(1).select(current_user(), version(), md5(lit("spark"))).show()
```

## Topics

### Session and Environment

- ``current_catalog()``
- ``current_database()``
- ``current_path()``
- ``current_schema()``
- ``current_user()``
- ``monotonically_increasing_id()``
- ``session_user()``
- ``spark_partition_id()``
- ``typeof(_:)``
- ``user()``
- ``uuid()``
- ``uuid(_:)-(Column)``
- ``uuid(_:)-(SparkLiteral)``
- ``version()``

### Input File Metadata

- ``input_file_block_length()``
- ``input_file_block_start()``
- ``input_file_name()``

### Bitmaps

- ``bitmap_bit_position(_:)``
- ``bitmap_bucket_number(_:)``
- ``bitmap_count(_:)``

### Assertions and Errors

- ``assert_true(_:)``
- ``assert_true(_:_:)-(_,Column)``
- ``assert_true(_:_:)-(_,String)``
- ``raise_error(_:)-(Column)``
- ``raise_error(_:)-(String)``

### Reflection

- ``java_method(_:)``
- ``reflect(_:)``
- ``try_reflect(_:)``

### Hashing

- ``crc32(_:)``
- ``hash(_:)``
- ``md5(_:)``
- ``sha(_:)``
- ``sha1(_:)``
- ``sha2(_:_:)``
- ``xxhash64(_:)``

### Encryption

- ``aes_decrypt(_:_:mode:padding:aad:)``
- ``aes_encrypt(_:_:mode:padding:iv:aad:)``
- ``hmac(_:_:)``
- ``hmac(_:_:_:)``
- ``try_aes_decrypt(_:_:mode:padding:aad:)``

### User-Defined Functions

- ``call_function(_:_:)``
- ``call_udf(_:_:)``

### Partition Transforms

- ``bucket(_:_:)-(Column,_)``
- ``bucket(_:_:)-(Int32,_)``
- ``days(_:)``
- ``hours(_:)``
- ``months(_:)``
- ``years(_:)``
