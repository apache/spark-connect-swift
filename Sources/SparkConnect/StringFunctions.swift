//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

/// Computes the numeric value of the first character of the string column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func ascii(_ col: Column) -> Column {
  return fn("ascii", col)
}

/// Computes the BASE64 encoding of a binary column and returns it as a string column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func base64(_ col: Column) -> Column {
  return fn("base64", col)
}

/// Calculates the bit length for the specified string column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func bit_length(_ col: Column) -> Column {
  return fn("bit_length", col)
}

/// Removes the leading and trailing space characters from `str`.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func btrim(_ str: Column) -> Column {
  return fn("btrim", str)
}

/// Removes the leading and trailing `trim` characters from `str`.
/// - Parameters:
///   - str: A ``Column`` to trim.
///   - trim: A ``Column`` of the trim string characters to trim.
/// - Returns: A ``Column``.
public func btrim(_ str: Column, _ trim: Column) -> Column {
  return fn("btrim", str, trim)
}

/// Returns the ASCII character having the binary equivalent to `n`. If n is larger than 256 the
/// result is equivalent to char(n % 256).
/// - Parameter n: A ``Column``.
/// - Returns: A ``Column``.
public func char(_ n: Column) -> Column {
  return fn("char", n)
}

/// Returns the character length of string data or number of bytes of binary data.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func char_length(_ str: Column) -> Column {
  return fn("char_length", str)
}

/// Returns the character length of string data or number of bytes of binary data.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func character_length(_ str: Column) -> Column {
  return fn("character_length", str)
}

/// Returns the ASCII character having the binary equivalent to `n`. If n is larger than 256 the
/// result is equivalent to chr(n % 256).
/// - Parameter n: A ``Column``.
/// - Returns: A ``Column``.
public func chr(_ n: Column) -> Column {
  return fn("chr", n)
}

/// Marks a given column with specified collation.
/// - Parameters:
///   - col: A ``Column``.
///   - collation: A collation name.
/// - Returns: A ``Column``.
public func collate(_ col: Column, _ collation: String) -> Column {
  return fn("collate", col, lit(collation))
}

/// Returns the collation name of a given column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func collation(_ col: Column) -> Column {
  return fn("collation", col)
}

/// Concatenates multiple input string columns together into a single string column, using the
/// given separator.
/// - Parameters:
///   - sep: A separator string.
///   - exprs: ``Column``s to concatenate.
/// - Returns: A ``Column``.
public func concat_ws(_ sep: String, _ exprs: Column...) -> Column {
  return fn("concat_ws", [lit(sep)] + exprs)
}

/// Returns a boolean. The value is true if `right` is found inside `left`. Returns NULL if
/// either input expression is NULL. Otherwise, returns false.
/// - Parameters:
///   - left: A ``Column`` to search in.
///   - right: A ``Column`` to search for.
/// - Returns: A ``Column``.
public func contains(_ left: Column, _ right: Column) -> Column {
  return fn("contains", left, right)
}

/// Computes the first argument into a string from a binary using the provided character set
/// (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
/// - Parameters:
///   - value: A ``Column`` to decode.
///   - charset: A character set name.
/// - Returns: A ``Column``.
public func decode(_ value: Column, _ charset: String) -> Column {
  return fn("decode", value, lit(charset))
}

/// Returns the `n`-th input, e.g., returns the second input when `n` is 2.
/// - Parameter inputs: An index ``Column`` followed by input ``Column``s.
/// - Returns: A ``Column``.
public func elt(_ inputs: Column...) -> Column {
  return fn("elt", inputs)
}

/// Computes the first argument into a binary from a string using the provided character set
/// (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
/// - Parameters:
///   - value: A ``Column`` to encode.
///   - charset: A character set name.
/// - Returns: A ``Column``.
public func encode(_ value: Column, _ charset: String) -> Column {
  return fn("encode", value, lit(charset))
}

/// Returns a boolean. The value is true if `str` ends with `suffix`. Returns NULL if either
/// input expression is NULL. Otherwise, returns false.
/// - Parameters:
///   - str: A ``Column`` to search in.
///   - suffix: A suffix ``Column``.
/// - Returns: A ``Column``.
public func endswith(_ str: Column, _ suffix: Column) -> Column {
  return fn("endswith", str, suffix)
}

/// Returns the index (1-based) of the given string `str` in the comma-delimited list `strArray`.
/// Returns 0 if the given string was not found or if it contains a comma.
/// - Parameters:
///   - str: A ``Column`` to search for.
///   - strArray: A comma-delimited list ``Column``.
/// - Returns: A ``Column``.
public func find_in_set(_ str: Column, _ strArray: Column) -> Column {
  return fn("find_in_set", str, strArray)
}

/// Formats numeric column `x` to a format like '#,###,###.##', rounded to `d` decimal places
/// with HALF_EVEN round mode, and returns the result as a string column.
/// - Parameters:
///   - x: A numeric ``Column``.
///   - d: The number of decimal places.
/// - Returns: A ``Column``.
public func format_number(_ x: Column, _ d: Int32) -> Column {
  return fn("format_number", x, lit(d))
}

/// Formats the arguments in printf-style and returns the result as a string column.
/// - Parameters:
///   - format: A printf-style format string.
///   - arguments: ``Column``s to format.
/// - Returns: A ``Column``.
public func format_string(_ format: String, _ arguments: Column...) -> Column {
  return fn("format_string", [lit(format)] + arguments)
}

/// Returns a new string column by converting the first letter of each word to uppercase.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func initcap(_ col: Column) -> Column {
  return fn("initcap", col)
}

/// Locate the position of the first occurrence of `substring` in `str`. The position is not
/// zero based, but 1 based index. Returns 0 if `substring` could not be found in `str`.
/// - Parameters:
///   - str: A ``Column`` to search in.
///   - substring: A substring to search for.
/// - Returns: A ``Column``.
public func instr(_ str: Column, _ substring: String) -> Column {
  return instr(str, lit(substring))
}

/// Locate the position of the first occurrence of `substring` in `str`. The position is not
/// zero based, but 1 based index. Returns 0 if `substring` could not be found in `str`.
/// - Parameters:
///   - str: A ``Column`` to search in.
///   - substring: A substring ``Column`` to search for.
/// - Returns: A ``Column``.
public func instr(_ str: Column, _ substring: Column) -> Column {
  return fn("instr", str, substring)
}

/// Locate the position of the first occurrence of `substring` in `str`, starting the search
/// from the `start` position.
/// - Parameters:
///   - str: A ``Column`` to search in.
///   - substring: A substring ``Column`` to search for.
///   - start: A start position of the search.
/// - Returns: A ``Column``.
public func instr(_ str: Column, _ substring: Column, _ start: Int32) -> Column {
  return fn("instr", str, substring, lit(start))
}

/// Locate the position of the first occurrence of `substring` in `str`, starting the search
/// from the `start` position.
/// - Parameters:
///   - str: A ``Column`` to search in.
///   - substring: A substring ``Column`` to search for.
///   - start: A start position ``Column`` of the search.
/// - Returns: A ``Column``.
public func instr(_ str: Column, _ substring: Column, _ start: Column) -> Column {
  return fn("instr", str, substring, start)
}

/// Locate the position of the `occurrence`-th occurrence of `substring` in `str`, starting the
/// search from the `start` position.
/// - Parameters:
///   - str: A ``Column`` to search in.
///   - substring: A substring ``Column`` to search for.
///   - start: A start position of the search.
///   - occurrence: The occurrence to find.
/// - Returns: A ``Column``.
public func instr(_ str: Column, _ substring: Column, _ start: Int32, _ occurrence: Int32)
  -> Column
{
  return fn("instr", str, substring, lit(start), lit(occurrence))
}

/// Locate the position of the `occurrence`-th occurrence of `substring` in `str`, starting the
/// search from the `start` position.
/// - Parameters:
///   - str: A ``Column`` to search in.
///   - substring: A substring ``Column`` to search for.
///   - start: A start position ``Column`` of the search.
///   - occurrence: An occurrence ``Column`` to find.
/// - Returns: A ``Column``.
public func instr(_ str: Column, _ substring: Column, _ start: Column, _ occurrence: Column)
  -> Column
{
  return fn("instr", str, substring, start, occurrence)
}

/// Returns true if the input is a valid UTF-8 string, otherwise returns false.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func is_valid_utf8(_ str: Column) -> Column {
  return fn("is_valid_utf8", str)
}

/// Computes the Jaro-Winkler similarity of the two given string columns.
/// - Parameters:
///   - l: A ``Column``.
///   - r: A ``Column``.
/// - Returns: A ``Column``.
public func jaro_winkler_similarity(_ l: Column, _ r: Column) -> Column {
  return fn("jaro_winkler_similarity", l, r)
}

/// Converts a string column to lowercase.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func lcase(_ str: Column) -> Column {
  return fn("lcase", str)
}

/// Returns the leftmost `len` characters from the string `str`. If `len` is less or equal than
/// 0 the result is an empty string.
/// - Parameters:
///   - str: A ``Column``.
///   - len: A length ``Column``.
/// - Returns: A ``Column``.
public func left(_ str: Column, _ len: Column) -> Column {
  return fn("left", str, len)
}

/// Computes the character length of a given string or number of bytes of a binary string.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func len(_ col: Column) -> Column {
  return fn("len", col)
}

/// Computes the character length of a given string or number of bytes of a binary string.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func length(_ col: Column) -> Column {
  return fn("length", col)
}

/// Computes the Levenshtein distance of the two given string columns.
/// - Parameters:
///   - l: A ``Column``.
///   - r: A ``Column``.
/// - Returns: A ``Column``.
public func levenshtein(_ l: Column, _ r: Column) -> Column {
  return fn("levenshtein", l, r)
}

/// Computes the Levenshtein distance of the two given string columns if it's less than or
/// equal to a given threshold. Returns -1 if the distance is larger than the threshold.
/// - Parameters:
///   - l: A ``Column``.
///   - r: A ``Column``.
///   - threshold: A threshold of the distance.
/// - Returns: A ``Column``.
public func levenshtein(_ l: Column, _ r: Column, _ threshold: Int32) -> Column {
  return fn("levenshtein", l, r, lit(threshold))
}

/// Locate the position of the first occurrence of `substr` in `str`. The position is not zero
/// based, but 1 based index. Returns 0 if `substr` could not be found in `str`.
/// - Parameters:
///   - substr: A substring to search for.
///   - str: A ``Column`` to search in.
/// - Returns: A ``Column``.
public func locate(_ substr: String, _ str: Column) -> Column {
  return fn("locate", lit(substr), str)
}

/// Locate the position of the first occurrence of `substr` in `str`, starting from the `pos`
/// position. The position is not zero based, but 1 based index. Returns 0 if `substr` could
/// not be found in `str`.
/// - Parameters:
///   - substr: A substring to search for.
///   - str: A ``Column`` to search in.
///   - pos: A start position of the search.
/// - Returns: A ``Column``.
public func locate(_ substr: String, _ str: Column, _ pos: Int32) -> Column {
  return fn("locate", lit(substr), str, lit(pos))
}

/// Converts a string column to lowercase.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func lower(_ col: Column) -> Column {
  return fn("lower", col)
}

/// Left-pad the string column with `pad` to a length of `len`. If the string column is longer
/// than `len`, the return value is shortened to `len` characters.
/// - Parameters:
///   - str: A ``Column`` to pad.
///   - len: A length to pad to.
///   - pad: A pad string.
/// - Returns: A ``Column``.
public func lpad(_ str: Column, _ len: Int32, _ pad: String) -> Column {
  return lpad(str, lit(len), lit(pad))
}

/// Left-pad the string column with `pad` to a length of `len`. If the string column is longer
/// than `len`, the return value is shortened to `len` characters.
/// - Parameters:
///   - str: A ``Column`` to pad.
///   - len: A length ``Column`` to pad to.
///   - pad: A pad ``Column``.
/// - Returns: A ``Column``.
public func lpad(_ str: Column, _ len: Column, _ pad: Column) -> Column {
  return fn("lpad", str, len, pad)
}

/// Trim the spaces from left end for the specified string value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func ltrim(_ col: Column) -> Column {
  return fn("ltrim", col)
}

/// Trim the specified character string from left end for the specified string column.
/// - Parameters:
///   - col: A ``Column`` to trim.
///   - trimString: The trim string characters to trim.
/// - Returns: A ``Column``.
public func ltrim(_ col: Column, _ trimString: String) -> Column {
  return ltrim(col, lit(trimString))
}

/// Trim the specified character string from left end for the specified string column.
/// - Parameters:
///   - col: A ``Column`` to trim.
///   - trim: A ``Column`` of the trim string characters to trim.
/// - Returns: A ``Column``.
public func ltrim(_ col: Column, _ trim: Column) -> Column {
  return fn("ltrim", trim, col)
}

/// Returns a new string in which all invalid UTF-8 byte sequences, if any, are replaced by the
/// Unicode replacement character (U+FFFD).
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func make_valid_utf8(_ str: Column) -> Column {
  return fn("make_valid_utf8", str)
}

/// Masks the given string value. The function replaces upper-case characters with 'X',
/// lower-case characters with 'x', and numbers with 'n'.
/// - Parameter input: A string ``Column``.
/// - Returns: A ``Column``.
public func mask(_ input: Column) -> Column {
  return fn("mask", input)
}

/// Masks the given string value. The function replaces upper-case characters with the
/// specific character, lower-case characters with 'x', and numbers with 'n'.
/// - Parameters:
///   - input: A string ``Column``.
///   - upperChar: A character ``Column`` to replace upper-case characters with.
/// - Returns: A ``Column``.
public func mask(_ input: Column, _ upperChar: Column) -> Column {
  return fn("mask", input, upperChar)
}

/// Masks the given string value. The function replaces upper-case and lower-case characters
/// with the characters specified respectively, and numbers with 'n'.
/// - Parameters:
///   - input: A string ``Column``.
///   - upperChar: A character ``Column`` to replace upper-case characters with.
///   - lowerChar: A character ``Column`` to replace lower-case characters with.
/// - Returns: A ``Column``.
public func mask(_ input: Column, _ upperChar: Column, _ lowerChar: Column) -> Column {
  return fn("mask", input, upperChar, lowerChar)
}

/// Masks the given string value. The function replaces upper-case, lower-case characters and
/// numbers with the characters specified respectively.
/// - Parameters:
///   - input: A string ``Column``.
///   - upperChar: A character ``Column`` to replace upper-case characters with.
///   - lowerChar: A character ``Column`` to replace lower-case characters with.
///   - digitChar: A character ``Column`` to replace digit characters with.
/// - Returns: A ``Column``.
public func mask(_ input: Column, _ upperChar: Column, _ lowerChar: Column, _ digitChar: Column)
  -> Column
{
  return fn("mask", input, upperChar, lowerChar, digitChar)
}

/// Masks the given string value. The function replaces upper-case, lower-case characters,
/// numbers and other characters with the characters specified respectively.
/// - Parameters:
///   - input: A string ``Column``.
///   - upperChar: A character ``Column`` to replace upper-case characters with.
///   - lowerChar: A character ``Column`` to replace lower-case characters with.
///   - digitChar: A character ``Column`` to replace digit characters with.
///   - otherChar: A character ``Column`` to replace all other characters with.
/// - Returns: A ``Column``.
public func mask(
  _ input: Column, _ upperChar: Column, _ lowerChar: Column, _ digitChar: Column,
  _ otherChar: Column
) -> Column {
  return fn("mask", input, upperChar, lowerChar, digitChar, otherChar)
}

/// Calculates the byte length for the specified string column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func octet_length(_ col: Column) -> Column {
  return fn("octet_length", col)
}

/// Overlay the specified portion of `src` with `replace`, starting from byte position `pos`
/// of `src`.
/// - Parameters:
///   - src: A ``Column`` to overlay.
///   - replace: A replacement ``Column``.
///   - pos: A position ``Column``.
/// - Returns: A ``Column``.
public func overlay(_ src: Column, _ replace: Column, _ pos: Column) -> Column {
  return fn("overlay", src, replace, pos)
}

/// Overlay the specified portion of `src` with `replace`, starting from byte position `pos`
/// of `src` and proceeding for `len` bytes.
/// - Parameters:
///   - src: A ``Column`` to overlay.
///   - replace: A replacement ``Column``.
///   - pos: A position ``Column``.
///   - len: A length ``Column``.
/// - Returns: A ``Column``.
public func overlay(_ src: Column, _ replace: Column, _ pos: Column, _ len: Column) -> Column {
  return fn("overlay", src, replace, pos, len)
}

/// Returns the position of the first occurrence of `substr` in `str` after position `1`. The
/// return value are 1-based.
/// - Parameters:
///   - substr: A substring ``Column`` to search for.
///   - str: A ``Column`` to search in.
/// - Returns: A ``Column``.
public func position(_ substr: Column, _ str: Column) -> Column {
  return fn("position", substr, str)
}

/// Returns the position of the first occurrence of `substr` in `str` after position `start`.
/// The given `start` and return value are 1-based.
/// - Parameters:
///   - substr: A substring ``Column`` to search for.
///   - str: A ``Column`` to search in.
///   - start: A start position ``Column`` of the search.
/// - Returns: A ``Column``.
public func position(_ substr: Column, _ str: Column, _ start: Column) -> Column {
  return fn("position", substr, str, start)
}

/// Formats the arguments in printf-style and returns the result as a string column.
/// - Parameters:
///   - format: A printf-style format ``Column``.
///   - arguments: ``Column``s to format.
/// - Returns: A ``Column``.
public func printf(_ format: Column, _ arguments: Column...) -> Column {
  return fn("printf", [format] + arguments)
}

/// Returns `str` enclosed by single quotes and each instance of single quote in it is
/// preceded by a backslash.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func quote(_ str: Column) -> Column {
  return fn("quote", str)
}

/// Returns a string of the specified length whose characters are chosen uniformly at random
/// from the following pool of characters: 0-9, a-z, A-Z.
/// - Parameter length: A length ``Column``.
/// - Returns: A ``Column``.
public func randstr(_ length: Column) -> Column {
  return fn("randstr", length)
}

/// Returns a string of the specified length whose characters are chosen uniformly at random
/// from the following pool of characters: 0-9, a-z, A-Z, with the chosen random seed.
/// - Parameters:
///   - length: A length ``Column``.
///   - seed: A random seed ``Column``.
/// - Returns: A ``Column``.
public func randstr(_ length: Column, _ seed: Column) -> Column {
  return fn("randstr", length, seed)
}

/// Returns a count of the number of times that the regular expression pattern `regexp` is
/// matched in the string `str`.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
/// - Returns: A ``Column``.
public func regexp_count(_ str: Column, _ regexp: Column) -> Column {
  return fn("regexp_count", str, regexp)
}

/// Extract a specific group matched by a Java regex, from the specified string column. If the
/// regex did not match, or the specified group did not match, an empty string is returned.
/// - Parameters:
///   - col: A ``Column`` to match.
///   - exp: A regular expression.
///   - groupIdx: A group index.
/// - Returns: A ``Column``.
public func regexp_extract(_ col: Column, _ exp: String, _ groupIdx: Int32) -> Column {
  return fn("regexp_extract", col, lit(exp), lit(groupIdx))
}

/// Extract all strings in the `str` that match the `regexp` expression and corresponding to
/// the first regex group index.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
/// - Returns: A ``Column``.
public func regexp_extract_all(_ str: Column, _ regexp: Column) -> Column {
  return fn("regexp_extract_all", str, regexp)
}

/// Extract all strings in the `str` that match the `regexp` expression and corresponding to
/// the regex group index.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
///   - idx: A group index ``Column``.
/// - Returns: A ``Column``.
public func regexp_extract_all(_ str: Column, _ regexp: Column, _ idx: Column) -> Column {
  return fn("regexp_extract_all", str, regexp, idx)
}

/// Searches a string for a regular expression and returns an integer that indicates the
/// beginning position of the matched substring. Positions are 1-based, not 0-based. If no
/// match is found, returns 0.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
/// - Returns: A ``Column``.
public func regexp_instr(_ str: Column, _ regexp: Column) -> Column {
  return fn("regexp_instr", str, regexp)
}

/// Searches a string for a regular expression and returns an integer that indicates the
/// beginning position of the matched substring. Positions are 1-based, not 0-based. If no
/// match is found, returns 0.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
///   - idx: A group index ``Column``.
/// - Returns: A ``Column``.
public func regexp_instr(_ str: Column, _ regexp: Column, _ idx: Column) -> Column {
  return fn("regexp_instr", str, regexp, idx)
}

/// Replace all substrings of the specified string value that match regexp with rep.
/// - Parameters:
///   - col: A ``Column`` to match.
///   - pattern: A regular expression.
///   - replacement: A replacement string.
/// - Returns: A ``Column``.
public func regexp_replace(_ col: Column, _ pattern: String, _ replacement: String) -> Column {
  return regexp_replace(col, lit(pattern), lit(replacement))
}

/// Replace all substrings of the specified string value that match regexp with rep, starting
/// at the specified 1-based position `pos`.
/// - Parameters:
///   - col: A ``Column`` to match.
///   - pattern: A regular expression.
///   - replacement: A replacement string.
///   - pos: A start position of the search.
/// - Returns: A ``Column``.
public func regexp_replace(
  _ col: Column, _ pattern: String, _ replacement: String, _ pos: Int32
) -> Column {
  return regexp_replace(col, lit(pattern), lit(replacement), lit(pos))
}

/// Replace all substrings of the specified string value that match regexp with rep.
/// - Parameters:
///   - col: A ``Column`` to match.
///   - pattern: A regular expression ``Column``.
///   - replacement: A replacement ``Column``.
/// - Returns: A ``Column``.
public func regexp_replace(_ col: Column, _ pattern: Column, _ replacement: Column) -> Column {
  return fn("regexp_replace", col, pattern, replacement)
}

/// Replace all substrings of the specified string value that match regexp with rep, starting
/// at the specified 1-based position `pos`.
/// - Parameters:
///   - col: A ``Column`` to match.
///   - pattern: A regular expression ``Column``.
///   - replacement: A replacement ``Column``.
///   - pos: A start position ``Column`` of the search.
/// - Returns: A ``Column``.
public func regexp_replace(
  _ col: Column, _ pattern: Column, _ replacement: Column, _ pos: Column
) -> Column {
  return fn("regexp_replace", col, pattern, replacement, pos)
}

/// Returns the substring that matches the regular expression `regexp` within the string `str`.
/// If the regular expression is not found, the result is null.
/// - Parameters:
///   - str: A ``Column`` to match.
///   - regexp: A regular expression ``Column``.
/// - Returns: A ``Column``.
public func regexp_substr(_ str: Column, _ regexp: Column) -> Column {
  return fn("regexp_substr", str, regexp)
}

/// Repeats a string column `n` times, and returns it as a new string column.
/// - Parameters:
///   - str: A ``Column`` to repeat.
///   - n: The number of times to repeat.
/// - Returns: A ``Column``.
public func `repeat`(_ str: Column, _ n: Int32) -> Column {
  return fn("repeat", str, lit(n))
}

/// Repeats a string column `n` times, and returns it as a new string column.
/// - Parameters:
///   - str: A ``Column`` to repeat.
///   - n: A ``Column`` for the number of times to repeat.
/// - Returns: A ``Column``.
public func `repeat`(_ str: Column, _ n: Column) -> Column {
  return fn("repeat", str, n)
}

/// Removes all occurrences of `search` from `src`.
/// - Parameters:
///   - src: A ``Column`` to replace.
///   - search: A ``Column`` to search for.
/// - Returns: A ``Column``.
public func replace(_ src: Column, _ search: Column) -> Column {
  return fn("replace", src, search)
}

/// Replaces all occurrences of `search` with `replace`.
/// - Parameters:
///   - src: A ``Column`` to replace.
///   - search: A ``Column`` to search for.
///   - replace: A replacement ``Column``.
/// - Returns: A ``Column``.
public func replace(_ src: Column, _ search: Column, _ replace: Column) -> Column {
  return fn("replace", src, search, replace)
}

/// Returns the rightmost `len` characters from the string `str`. If `len` is less or equal
/// than 0 the result is an empty string.
/// - Parameters:
///   - str: A ``Column``.
///   - len: A length ``Column``.
/// - Returns: A ``Column``.
public func right(_ str: Column, _ len: Column) -> Column {
  return fn("right", str, len)
}

/// Right-pad the string column with `pad` to a length of `len`. If the string column is longer
/// than `len`, the return value is shortened to `len` characters.
/// - Parameters:
///   - str: A ``Column`` to pad.
///   - len: A length to pad to.
///   - pad: A pad string.
/// - Returns: A ``Column``.
public func rpad(_ str: Column, _ len: Int32, _ pad: String) -> Column {
  return rpad(str, lit(len), lit(pad))
}

/// Right-pad the string column with `pad` to a length of `len`. If the string column is longer
/// than `len`, the return value is shortened to `len` characters.
/// - Parameters:
///   - str: A ``Column`` to pad.
///   - len: A length ``Column`` to pad to.
///   - pad: A pad ``Column``.
/// - Returns: A ``Column``.
public func rpad(_ str: Column, _ len: Column, _ pad: Column) -> Column {
  return fn("rpad", str, len, pad)
}

/// Trim the spaces from right end for the specified string value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func rtrim(_ col: Column) -> Column {
  return fn("rtrim", col)
}

/// Trim the specified character string from right end for the specified string column.
/// - Parameters:
///   - col: A ``Column`` to trim.
///   - trimString: The trim string characters to trim.
/// - Returns: A ``Column``.
public func rtrim(_ col: Column, _ trimString: String) -> Column {
  return rtrim(col, lit(trimString))
}

/// Trim the specified character string from right end for the specified string column.
/// - Parameters:
///   - col: A ``Column`` to trim.
///   - trim: A ``Column`` of the trim string characters to trim.
/// - Returns: A ``Column``.
public func rtrim(_ col: Column, _ trim: Column) -> Column {
  return fn("rtrim", trim, col)
}

/// Splits a string into arrays of sentences, where each sentence is an array of words. The
/// default locale is used.
/// - Parameter string: A string ``Column`` to be split.
/// - Returns: A ``Column``.
public func sentences(_ string: Column) -> Column {
  return fn("sentences", string)
}

/// Splits a string into arrays of sentences, where each sentence is an array of words. The
/// default `country`('') is used.
/// - Parameters:
///   - string: A string ``Column`` to be split.
///   - language: A language ``Column`` of the locale.
/// - Returns: A ``Column``.
public func sentences(_ string: Column, _ language: Column) -> Column {
  return fn("sentences", string, language)
}

/// Splits a string into arrays of sentences, where each sentence is an array of words.
/// - Parameters:
///   - string: A string ``Column`` to be split.
///   - language: A language ``Column`` of the locale.
///   - country: A country ``Column`` of the locale.
/// - Returns: A ``Column``.
public func sentences(_ string: Column, _ language: Column, _ country: Column) -> Column {
  return fn("sentences", string, language, country)
}

/// Returns the soundex code for the specified expression.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func soundex(_ col: Column) -> Column {
  return fn("soundex", col)
}

/// Splits str around matches of the given pattern.
/// - Parameters:
///   - str: A string ``Column`` to split.
///   - pattern: A regular expression to split.
/// - Returns: A ``Column``.
public func split(_ str: Column, _ pattern: String) -> Column {
  return fn("split", str, lit(pattern))
}

/// Splits str around matches of the given pattern.
/// - Parameters:
///   - str: A string ``Column`` to split.
///   - pattern: A regular expression ``Column`` to split.
/// - Returns: A ``Column``.
public func split(_ str: Column, _ pattern: Column) -> Column {
  return fn("split", str, pattern)
}

/// Splits str around matches of the given pattern up to the `limit`. If `limit` is positive,
/// the resulting array's length will not be more than `limit`. If `limit` is negative, the
/// resulting array can be of any size.
/// - Parameters:
///   - str: A string ``Column`` to split.
///   - pattern: A regular expression to split.
///   - limit: A limit of the split.
/// - Returns: A ``Column``.
public func split(_ str: Column, _ pattern: String, _ limit: Int32) -> Column {
  return fn("split", str, lit(pattern), lit(limit))
}

/// Splits str around matches of the given pattern up to the `limit`. If `limit` is positive,
/// the resulting array's length will not be more than `limit`. If `limit` is negative, the
/// resulting array can be of any size.
/// - Parameters:
///   - str: A string ``Column`` to split.
///   - pattern: A regular expression ``Column`` to split.
///   - limit: A limit ``Column`` of the split.
/// - Returns: A ``Column``.
public func split(_ str: Column, _ pattern: Column, _ limit: Column) -> Column {
  return fn("split", str, pattern, limit)
}

/// Splits `str` by delimiter and return requested part of the split (1-based). If any input is
/// null, returns null. If the index is out of range of split parts, returns empty string.
/// - Parameters:
///   - str: A string ``Column`` to split.
///   - delimiter: A delimiter ``Column``.
///   - partNum: A part number ``Column``.
/// - Returns: A ``Column``.
public func split_part(_ str: Column, _ delimiter: Column, _ partNum: Column) -> Column {
  return fn("split_part", str, delimiter, partNum)
}

/// Returns a boolean. The value is true if `str` starts with `prefix`. Returns NULL if either
/// input expression is NULL. Otherwise, returns false.
/// - Parameters:
///   - str: A ``Column`` to search in.
///   - prefix: A prefix ``Column``.
/// - Returns: A ``Column``.
public func startswith(_ str: Column, _ prefix: Column) -> Column {
  return fn("startswith", str, prefix)
}

/// Returns the substring of `str` that starts at `pos`, or the slice of byte array that starts
/// at `pos`.
/// - Parameters:
///   - str: A ``Column``.
///   - pos: A position ``Column``.
/// - Returns: A ``Column``.
public func substr(_ str: Column, _ pos: Column) -> Column {
  return fn("substr", str, pos)
}

/// Returns the substring of `str` that starts at `pos` and is of length `len`, or the slice of
/// byte array that starts at `pos` and is of length `len`.
/// - Parameters:
///   - str: A ``Column``.
///   - pos: A position ``Column``.
///   - len: A length ``Column``.
/// - Returns: A ``Column``.
public func substr(_ str: Column, _ pos: Column, _ len: Column) -> Column {
  return fn("substr", str, pos, len)
}

/// Substring starts at `pos` and is of length `len` when str is String type or returns the
/// slice of byte array that starts at `pos` in byte and is of length `len` when str is Binary
/// type.
/// - Parameters:
///   - str: A ``Column``.
///   - pos: A position.
///   - len: A length.
/// - Returns: A ``Column``.
public func substring(_ str: Column, _ pos: Int32, _ len: Int32) -> Column {
  return fn("substring", str, lit(pos), lit(len))
}

/// Substring starts at `pos` and is of length `len` when str is String type or returns the
/// slice of byte array that starts at `pos` in byte and is of length `len` when str is Binary
/// type.
/// - Parameters:
///   - str: A ``Column``.
///   - pos: A position ``Column``.
///   - len: A length ``Column``.
/// - Returns: A ``Column``.
public func substring(_ str: Column, _ pos: Column, _ len: Column) -> Column {
  return fn("substring", str, pos, len)
}

/// Returns the substring from string str before count occurrences of the delimiter delim. If
/// count is positive, everything the left of the final delimiter (counting from left) is
/// returned. If count is negative, every to the right of the final delimiter (counting from
/// the right) is returned.
/// - Parameters:
///   - str: A ``Column``.
///   - delim: A delimiter string.
///   - count: The number of occurrences.
/// - Returns: A ``Column``.
public func substring_index(_ str: Column, _ delim: String, _ count: Int32) -> Column {
  return fn("substring_index", str, lit(delim), lit(count))
}

/// Converts the input `col` to a binary value based on the default format 'hex'.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func to_binary(_ col: Column) -> Column {
  return fn("to_binary", col)
}

/// Converts the input `col` to a binary value based on the supplied `format`. The `format` can
/// be a case-insensitive string literal of 'hex', 'utf-8', 'utf8', or 'base64'.
/// - Parameters:
///   - col: A ``Column``.
///   - format: A format ``Column``.
/// - Returns: A ``Column``.
public func to_binary(_ col: Column, _ format: Column) -> Column {
  return fn("to_binary", col, format)
}

/// Convert `col` to a string based on the `format`.
/// - Parameters:
///   - col: A ``Column``.
///   - format: A format ``Column``.
/// - Returns: A ``Column``.
public func to_char(_ col: Column, _ format: Column) -> Column {
  return fn("to_char", col, format)
}

/// Convert string `col` to a number based on the string format `format`.
/// - Parameters:
///   - col: A ``Column``.
///   - format: A format ``Column``.
/// - Returns: A ``Column``.
public func to_number(_ col: Column, _ format: Column) -> Column {
  return fn("to_number", col, format)
}

/// Convert `col` to a string based on the `format`.
/// - Parameters:
///   - col: A ``Column``.
///   - format: A format ``Column``.
/// - Returns: A ``Column``.
public func to_varchar(_ col: Column, _ format: Column) -> Column {
  return fn("to_varchar", col, format)
}

/// Translate any character in the src by a character in replaceString. The characters in
/// replaceString correspond to the characters in matchingString. The translate will happen
/// when any character in the string matches the character in the `matchingString`.
/// - Parameters:
///   - src: A ``Column`` to translate.
///   - matchingString: A matching string.
///   - replaceString: A replacement string.
/// - Returns: A ``Column``.
public func translate(_ src: Column, _ matchingString: String, _ replaceString: String) -> Column
{
  return fn("translate", src, lit(matchingString), lit(replaceString))
}

/// Trim the spaces from both ends for the specified string column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func trim(_ col: Column) -> Column {
  return fn("trim", col)
}

/// Trim the specified character from both ends for the specified string column.
/// - Parameters:
///   - col: A ``Column`` to trim.
///   - trimString: The trim string characters to trim.
/// - Returns: A ``Column``.
public func trim(_ col: Column, _ trimString: String) -> Column {
  return trim(col, lit(trimString))
}

/// Trim the specified character from both ends for the specified string column.
/// - Parameters:
///   - col: A ``Column`` to trim.
///   - trim: A ``Column`` of the trim string characters to trim.
/// - Returns: A ``Column``.
public func trim(_ col: Column, _ trim: Column) -> Column {
  return fn("trim", trim, col)
}

/// Converts the input `col` to a binary value based on the default format 'hex'. The function
/// returns NULL if at least one of the input parameters is NULL.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func try_to_binary(_ col: Column) -> Column {
  return fn("try_to_binary", col)
}

/// Converts the input `col` to a binary value based on the supplied `format`. The `format` can
/// be a case-insensitive string literal of 'hex', 'utf-8', 'utf8', or 'base64'. The function
/// returns NULL if at least one of the input parameters is NULL.
/// - Parameters:
///   - col: A ``Column``.
///   - format: A format ``Column``.
/// - Returns: A ``Column``.
public func try_to_binary(_ col: Column, _ format: Column) -> Column {
  return fn("try_to_binary", col, format)
}

/// Convert string `col` to a number based on the string format `format`. Returns NULL if the
/// string `col` does not match the expected format.
/// - Parameters:
///   - col: A ``Column``.
///   - format: A format ``Column``.
/// - Returns: A ``Column``.
public func try_to_number(_ col: Column, _ format: Column) -> Column {
  return fn("try_to_number", col, format)
}

/// Returns the input value if it corresponds to a valid UTF-8 string, or NULL otherwise.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func try_validate_utf8(_ str: Column) -> Column {
  return fn("try_validate_utf8", str)
}

/// Converts a string column to uppercase.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func ucase(_ str: Column) -> Column {
  return fn("ucase", str)
}

/// Decodes a BASE64 encoded string column and returns it as a binary column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func unbase64(_ col: Column) -> Column {
  return fn("unbase64", col)
}

/// Converts a string column to uppercase.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func upper(_ col: Column) -> Column {
  return fn("upper", col)
}

/// Returns the input value if it corresponds to a valid UTF-8 string, or emits an error
/// otherwise.
/// - Parameter str: A ``Column``.
/// - Returns: A ``Column``.
public func validate_utf8(_ str: Column) -> Column {
  return fn("validate_utf8", str)
}
