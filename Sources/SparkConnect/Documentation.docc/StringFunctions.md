# String Functions

Manipulate, search, encode, and validate string columns.

## Overview

```swift
try await df.select(upper(col("name")), substring(col("code"), 1, 3),
                    regexp_replace(col("text"), "\\s+", " "))
  .show()
```

## Topics

### Case Conversion

- ``initcap(_:)``
- ``lcase(_:)``
- ``lower(_:)``
- ``ucase(_:)``
- ``upper(_:)``

### Length and Position

- ``bit_length(_:)``
- ``char_length(_:)``
- ``character_length(_:)``
- ``find_in_set(_:_:)``
- ``instr(_:_:)-(_,Column)``
- ``instr(_:_:)-(_,String)``
- ``instr(_:_:_:)-(_,_,Column)``
- ``instr(_:_:_:)-(_,_,Int32)``
- ``instr(_:_:_:_:)-(_,_,Column,_)``
- ``instr(_:_:_:_:)-(_,_,Int32,_)``
- ``len(_:)``
- ``length(_:)``
- ``locate(_:_:)``
- ``locate(_:_:_:)``
- ``octet_length(_:)``
- ``position(_:_:)``
- ``position(_:_:_:)``

### Padding and Trimming

- ``btrim(_:)``
- ``btrim(_:_:)``
- ``lpad(_:_:_:)-(_,Column,_)``
- ``lpad(_:_:_:)-(_,Int32,_)``
- ``ltrim(_:)``
- ``ltrim(_:_:)-(_,Column)``
- ``ltrim(_:_:)-(_,String)``
- ``rpad(_:_:_:)-(_,Column,_)``
- ``rpad(_:_:_:)-(_,Int32,_)``
- ``rtrim(_:)``
- ``rtrim(_:_:)-(_,Column)``
- ``rtrim(_:_:)-(_,String)``
- ``trim(_:)``
- ``trim(_:_:)-(_,Column)``
- ``trim(_:_:)-(_,String)``

### Substrings and Splitting

- ``elt(_:)``
- ``left(_:_:)``
- ``right(_:_:)``
- ``split(_:_:)-(_,Column)``
- ``split(_:_:)-(_,String)``
- ``split(_:_:_:)-(_,Column,_)``
- ``split(_:_:_:)-(_,_,Int32)``
- ``split_part(_:_:_:)``
- ``substr(_:_:)``
- ``substr(_:_:_:)``
- ``substring(_:_:_:)-(_,Column,_)``
- ``substring(_:_:_:)-(_,Int32,_)``
- ``substring_index(_:_:_:)``

### Concatenation and Formatting

- ``concat_ws(_:_:)``
- ``format_number(_:_:)``
- ``format_string(_:_:)``
- ``printf(_:_:)``
- ``repeat(_:_:)-(_,Column)``
- ``repeat(_:_:)-(_,Int32)``

### Search and Replace

- ``contains(_:_:)``
- ``endswith(_:_:)``
- ``overlay(_:_:_:)``
- ``overlay(_:_:_:_:)``
- ``quote(_:)``
- ``replace(_:_:)``
- ``replace(_:_:_:)``
- ``startswith(_:_:)``
- ``translate(_:_:_:)``

### Regular Expressions

- ``regexp_count(_:_:)``
- ``regexp_extract(_:_:_:)``
- ``regexp_extract_all(_:_:)``
- ``regexp_extract_all(_:_:_:)``
- ``regexp_instr(_:_:)``
- ``regexp_instr(_:_:_:)``
- ``regexp_replace(_:_:_:)-(_,Column,_)``
- ``regexp_replace(_:_:_:)-(_,String,_)``
- ``regexp_replace(_:_:_:_:)-(_,Column,_,_)``
- ``regexp_replace(_:_:_:_:)-(_,_,_,Int32)``
- ``regexp_substr(_:_:)``

### Encoding and Decoding

- ``ascii(_:)``
- ``base64(_:)``
- ``char(_:)``
- ``chr(_:)``
- ``decode(_:_:)``
- ``encode(_:_:)``
- ``from_base32(_:)``
- ``to_base32(_:)``
- ``to_binary(_:)``
- ``to_binary(_:_:)``
- ``try_to_binary(_:)``
- ``try_to_binary(_:_:)``
- ``unbase64(_:)``

### UTF-8 Validation

- ``is_valid_utf8(_:)``
- ``make_valid_utf8(_:)``
- ``try_validate_utf8(_:)``
- ``validate_utf8(_:)``

### Collation

- ``collate(_:_:)``
- ``collation(_:)``

### Type Conversion

- ``to_char(_:_:)``
- ``to_number(_:_:)``
- ``to_varchar(_:_:)``
- ``try_to_number(_:_:)``

### Similarity

- ``jaro_winkler_similarity(_:_:)``
- ``levenshtein(_:_:)``
- ``levenshtein(_:_:_:)``
- ``sentences(_:)``
- ``sentences(_:_:)``
- ``sentences(_:_:_:)``
- ``soundex(_:)``

### Masking and Randomness

- ``mask(_:)``
- ``mask(_:_:)``
- ``mask(_:_:_:)``
- ``mask(_:_:_:_:)``
- ``mask(_:_:_:_:_:)``
- ``randstr(_:)``
- ``randstr(_:_:)``

### URLs

- ``parse_url(_:_:)-(_,Column)``
- ``parse_url(_:_:)-(_,String)``
- ``parse_url(_:_:_:)-(_,Column,_)``
- ``parse_url(_:_:_:)-(_,String,_)``
- ``try_parse_url(_:_:)-(_,Column)``
- ``try_parse_url(_:_:)-(_,String)``
- ``try_parse_url(_:_:_:)-(_,Column,_)``
- ``try_parse_url(_:_:_:)-(_,String,_)``
- ``try_url_decode(_:)``
- ``url_decode(_:)``
- ``url_encode(_:)``
