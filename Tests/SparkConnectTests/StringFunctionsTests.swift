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

import Testing

@testable import SparkConnect

/// A test suite for `StringFunctions`
@Suite(.serialized)
struct StringFunctionsTests {

  @Test
  func stringFunctions() throws {
    for (column, name) in [
      (ascii(col("a")), "ascii"),
      (base64(col("a")), "base64"),
      (bit_length(col("a")), "bit_length"),
      (btrim(col("a")), "btrim"),
      (char(col("a")), "char"),
      (char_length(col("a")), "char_length"),
      (character_length(col("a")), "character_length"),
      (chr(col("a")), "chr"),
      (collation(col("a")), "collation"),
      (initcap(col("a")), "initcap"),
      (is_valid_utf8(col("a")), "is_valid_utf8"),
      (lcase(col("a")), "lcase"),
      (len(col("a")), "len"),
      (length(col("a")), "length"),
      (lower(col("a")), "lower"),
      (ltrim(col("a")), "ltrim"),
      (make_valid_utf8(col("a")), "make_valid_utf8"),
      (mask(col("a")), "mask"),
      (octet_length(col("a")), "octet_length"),
      (quote(col("a")), "quote"),
      (randstr(col("a")), "randstr"),
      (rtrim(col("a")), "rtrim"),
      (sentences(col("a")), "sentences"),
      (soundex(col("a")), "soundex"),
      (to_binary(col("a")), "to_binary"),
      (trim(col("a")), "trim"),
      (try_to_binary(col("a")), "try_to_binary"),
      (try_validate_utf8(col("a")), "try_validate_utf8"),
      (ucase(col("a")), "ucase"),
      (unbase64(col("a")), "unbase64"),
      (upper(col("a")), "upper"),
      (validate_utf8(col("a")), "validate_utf8"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
    }
  }

  @Test
  func stringFunctionArguments() throws {
    for (column, name) in [
      (btrim(col("a"), col("b")), "btrim"),
      (contains(col("a"), col("b")), "contains"),
      (endswith(col("a"), col("b")), "endswith"),
      (find_in_set(col("a"), col("b")), "find_in_set"),
      (instr(col("a"), col("b")), "instr"),
      (jaro_winkler_similarity(col("a"), col("b")), "jaro_winkler_similarity"),
      (left(col("a"), col("b")), "left"),
      (levenshtein(col("a"), col("b")), "levenshtein"),
      (mask(col("a"), col("b")), "mask"),
      (position(col("a"), col("b")), "position"),
      (randstr(col("a"), col("b")), "randstr"),
      (regexp_count(col("a"), col("b")), "regexp_count"),
      (regexp_extract_all(col("a"), col("b")), "regexp_extract_all"),
      (regexp_instr(col("a"), col("b")), "regexp_instr"),
      (regexp_substr(col("a"), col("b")), "regexp_substr"),
      (`repeat`(col("a"), col("b")), "repeat"),
      (replace(col("a"), col("b")), "replace"),
      (right(col("a"), col("b")), "right"),
      (sentences(col("a"), col("b")), "sentences"),
      (split(col("a"), col("b")), "split"),
      (startswith(col("a"), col("b")), "startswith"),
      (substr(col("a"), col("b")), "substr"),
      (to_binary(col("a"), col("b")), "to_binary"),
      (to_char(col("a"), col("b")), "to_char"),
      (to_number(col("a"), col("b")), "to_number"),
      (to_varchar(col("a"), col("b")), "to_varchar"),
      (try_to_binary(col("a"), col("b")), "try_to_binary"),
      (try_to_number(col("a"), col("b")), "try_to_number"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == 2)
    }

    for (column, name, count) in [
      (instr(col("a"), col("b"), 2), "instr", 3),
      (instr(col("a"), col("b"), lit(2)), "instr", 3),
      (instr(col("a"), col("b"), 2, 2), "instr", 4),
      (instr(col("a"), col("b"), lit(2), lit(2)), "instr", 4),
      (mask(col("a"), lit("X"), lit("x")), "mask", 3),
      (mask(col("a"), lit("X"), lit("x"), lit("n")), "mask", 4),
      (mask(col("a"), lit("X"), lit("x"), lit("n"), lit("o")), "mask", 5),
      (overlay(col("a"), col("b"), lit(1)), "overlay", 3),
      (overlay(col("a"), col("b"), lit(1), lit(2)), "overlay", 4),
      (position(col("a"), col("b"), lit(1)), "position", 3),
      (regexp_extract_all(col("a"), col("b"), lit(1)), "regexp_extract_all", 3),
      (regexp_instr(col("a"), col("b"), lit(1)), "regexp_instr", 3),
      (regexp_replace(col("a"), col("b"), col("c")), "regexp_replace", 3),
      (regexp_replace(col("a"), col("b"), col("c"), lit(1)), "regexp_replace", 4),
      (sentences(col("a"), col("b"), col("c")), "sentences", 3),
      (split(col("a"), col("b"), lit(2)), "split", 3),
      (split_part(col("a"), col("b"), lit(1)), "split_part", 3),
      (substr(col("a"), col("b"), col("c")), "substr", 3),
      (substring(col("a"), col("b"), col("c")), "substring", 3),
      (elt(lit(1), col("a"), col("b")), "elt", 3),
      (printf(lit("%s"), col("a")), "printf", 2),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
    }
  }

  @Test
  func stringFunctionLiteralArguments() throws {
    for (column, name, literal) in [
      (collate(col("a"), "UTF8_BINARY"), "collate", "UTF8_BINARY"),
      (decode(col("a"), "UTF-8"), "decode", "UTF-8"),
      (encode(col("a"), "UTF-8"), "encode", "UTF-8"),
      (instr(col("a"), "b"), "instr", "b"),
      (split(col("a"), ","), "split", ","),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[1].literal.string == literal)
    }

    let concatenated = concat_ws("|", col("a"), col("b")).expr
    #expect(concatenated.unresolvedFunction.functionName == "concat_ws")
    #expect(concatenated.unresolvedFunction.arguments[0].literal.string == "|")
    #expect(concatenated.unresolvedFunction.arguments.count == 3)

    let formatted = format_string("%s-%s", col("a"), col("b")).expr
    #expect(formatted.unresolvedFunction.functionName == "format_string")
    #expect(formatted.unresolvedFunction.arguments[0].literal.string == "%s-%s")
    #expect(formatted.unresolvedFunction.arguments.count == 3)

    let located = locate("b", col("a")).expr
    #expect(located.unresolvedFunction.functionName == "locate")
    #expect(located.unresolvedFunction.arguments[0].literal.string == "b")

    // The trim string argument comes first for `ltrim`, `rtrim` and `trim`.
    for (column, name) in [
      (ltrim(col("a"), "x"), "ltrim"),
      (ltrim(col("a"), lit("x")), "ltrim"),
      (rtrim(col("a"), "x"), "rtrim"),
      (rtrim(col("a"), lit("x")), "rtrim"),
      (trim(col("a"), "x"), "trim"),
      (trim(col("a"), lit("x")), "trim"),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments[0].literal.string == "x")
      #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "a")
    }

    for (column, name, literal) in [
      (format_number(col("a"), 2), "format_number", Int32(2)),
      (levenshtein(col("a"), col("b"), 3), "levenshtein", Int32(3)),
      (locate("b", col("a"), 2), "locate", Int32(2)),
      (`repeat`(col("a"), 2), "repeat", Int32(2)),
      (split(col("a"), ",", 2), "split", Int32(2)),
      (substring_index(col("a"), ".", 2), "substring_index", Int32(2)),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.last!.literal.integer == literal)
    }

    let padded = lpad(col("a"), 5, "*").expr
    #expect(padded.unresolvedFunction.functionName == "lpad")
    #expect(padded.unresolvedFunction.arguments[1].literal.integer == 5)
    #expect(padded.unresolvedFunction.arguments[2].literal.string == "*")
    #expect(rpad(col("a"), 5, "*").expr.unresolvedFunction.functionName == "rpad")

    let extracted = regexp_extract(col("a"), "(\\d+)", 1).expr
    #expect(extracted.unresolvedFunction.functionName == "regexp_extract")
    #expect(extracted.unresolvedFunction.arguments[1].literal.string == "(\\d+)")
    #expect(extracted.unresolvedFunction.arguments[2].literal.integer == 1)

    let replaced = regexp_replace(col("a"), "(\\d+)", "num").expr
    #expect(replaced.unresolvedFunction.functionName == "regexp_replace")
    #expect(replaced.unresolvedFunction.arguments[2].literal.string == "num")
    #expect(
      regexp_replace(col("a"), "(\\d+)", "num", 5).expr.unresolvedFunction.arguments.count == 4)

    let translated = translate(col("a"), "abc", "123").expr
    #expect(translated.unresolvedFunction.functionName == "translate")
    #expect(translated.unresolvedFunction.arguments[1].literal.string == "abc")
    #expect(translated.unresolvedFunction.arguments[2].literal.string == "123")

    let substringed = substring(col("a"), 2, 3).expr
    #expect(substringed.unresolvedFunction.functionName == "substring")
    #expect(substringed.unresolvedFunction.arguments[1].literal.integer == 2)
    #expect(substringed.unresolvedFunction.arguments[2].literal.integer == 3)
  }

  @Test
  func selectBasicStringFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let cases = try await df.select(
      lower(lit("ABC")), lcase(lit("ABC")), upper(lit("abc")), ucase(lit("abc")),
      initcap(lit("spark sql"))
    ).collect()
    #expect(cases == [Row("abc", "abc", "ABC", "ABC", "Spark Sql")])

    let lengths = try await df.select(
      length(lit("Spark")), len(lit("Spark")), char_length(lit("Spark")),
      character_length(lit("Spark")), bit_length(lit("abc")), octet_length(lit("abc"))
    ).collect()
    #expect(lengths == [Row(5, 5, 5, 5, 24, 3)])

    let characters = try await df.select(
      ascii(lit("A")), char(lit(65)), chr(lit(65)), soundex(lit("Miller"))
    ).collect()
    #expect(characters == [Row(65, "A", "A", "M460")])
    await spark.stop()
  }

  @Test
  func selectTrimAndPadFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let spaces = try await df.select(
      trim(lit("  abc  ")), ltrim(lit("  abc")), rtrim(lit("abc  ")), btrim(lit("  abc  "))
    ).collect()
    #expect(spaces == [Row("abc", "abc", "abc", "abc")])

    let trimmed = try await df.select(
      trim(lit("xxabcxx"), "x"), ltrim(lit("xxabc"), "x"), rtrim(lit("abcxx"), "x"),
      btrim(lit("xxabcxx"), lit("x")), trim(lit("yabcy"), lit("y"))
    ).collect()
    #expect(trimmed == [Row("abc", "abc", "abc", "abc", "abc")])

    let padded = try await df.select(
      lpad(lit("hi"), 5, "??"), rpad(lit("hi"), 5, "??"),
      lpad(lit("hi"), lit(5), lit("*")), rpad(lit("hi"), lit(5), lit("*"))
    ).collect()
    #expect(padded == [Row("???hi", "hi???", "***hi", "hi***")])
    await spark.stop()
  }

  @Test
  func selectSearchFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let positions = try await df.select(
      instr(lit("SparkSQL"), "SQL"), instr(lit("SparkSQL"), lit("SQL")),
      locate("SQL", lit("SparkSQL")), locate("SQL", lit("SparkSQLSQL"), 7),
      position(lit("SQL"), lit("SparkSQL")), position(lit("SQL"), lit("SparkSQLSQL"), lit(7))
    ).collect()
    #expect(positions == [Row(6, 6, 6, 9, 6, 9)])

    let predicates = try await df.select(
      contains(lit("Spark"), lit("par")), startswith(lit("Spark"), lit("Spa")),
      endswith(lit("Spark"), lit("ark")), find_in_set(lit("ab"), lit("abc,b,ab,c,def"))
    ).collect()
    #expect(predicates == [Row(true, true, true, 3)])

    let distances = try await df.select(
      levenshtein(lit("kitten"), lit("sitting")), levenshtein(lit("kitten"), lit("sitting"), 3)
    ).collect()
    #expect(distances == [Row(3, 3)])
    await spark.stop()
  }

  @Test
  func selectTransformFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let combined = try await df.select(
      concat_ws("|", lit("a"), lit("b")), format_string("%s-%s", lit("a"), lit("b")),
      printf(lit("%s-%s"), lit("a"), lit("b")), format_number(lit(12345.678), 2),
      elt(lit(2), lit("scala"), lit("java"))
    ).collect()
    #expect(combined == [Row("a|b", "a-b", "a-b", "12,345.68", "java")])

    let substrings = try await df.select(
      substring(lit("Spark SQL"), 5, 1), substring(lit("Spark SQL"), lit(7), lit(3)),
      substr(lit("Spark SQL"), lit(5)), substr(lit("Spark SQL"), lit(5), lit(1)),
      substring_index(lit("a.b.c"), ".", 2), left(lit("Spark SQL"), lit(3)),
      right(lit("Spark SQL"), lit(3))
    ).collect()
    #expect(substrings == [Row("k", "SQL", "k SQL", "k", "a.b", "Spa", "SQL")])

    let replaced = try await df.select(
      replace(lit("ABCabc"), lit("abc"), lit("DEF")), replace(lit("ABCabc"), lit("abc")),
      translate(lit("AaBbCc"), "abc", "123"), `repeat`(lit("ab"), 2), `repeat`(lit("ab"), lit(3)),
      overlay(lit("SPARK_SQL"), lit("CORE"), lit(7)),
      overlay(lit("SPARK_SQL"), lit("ANSI "), lit(7), lit(0))
    ).collect()
    #expect(replaced == [Row("ABCDEF", "ABC", "A1B2C3", "abab", "ababab", "SPARK_CORE", "SPARK_ANSI SQL")])

    let masked = try await df.select(
      mask(lit("AbCD123-@$#")), mask(lit("AbCD123-@$#"), lit("Q")),
      mask(lit("AbCD123-@$#"), lit("Q"), lit("q"), lit("d"), lit("o"))
    ).collect()
    #expect(masked == [Row("XxXXnnn-@$#", "QxQQnnn-@$#", "QqQQdddoooo")])
    await spark.stop()
  }

  @Test
  func selectRegexpFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let extracted = try await df.select(
      regexp_count(lit("banana"), lit("a")), regexp_extract(lit("100-200"), "(\\d+)-(\\d+)", 1),
      regexp_extract_all(lit("100-200"), lit("(\\d+)")).cast("string"),
      regexp_instr(lit("user@spark.apache.org"), lit("@[^.]*")),
      regexp_substr(lit("100-200"), lit("(\\d+)"))
    ).collect()
    #expect(extracted == [Row(3, "100", "[100, 200]", 5, "100")])

    let replaced = try await df.select(
      regexp_replace(lit("100-200"), "(\\d+)", "num"),
      regexp_replace(lit("100-200"), lit("(\\d+)"), lit("num")),
      regexp_replace(lit("100-200"), "(\\d+)", "num", 5)
    ).collect()
    #expect(replaced == [Row("num-num", "num-num", "100-num")])

    let separated = try await df.select(
      split(lit("oneAtwoBthree"), "[AB]").cast("string"),
      split(lit("oneAtwoBthree"), lit("[AB]")).cast("string"),
      split(lit("oneAtwoBthree"), "[AB]", 2).cast("string"),
      split_part(lit("11.12.13"), lit("."), lit(3)),
      sentences(lit("Hi there! Good morning.")).cast("string")
    ).collect()
    #expect(
      separated == [
        Row(
          "[one, two, three]", "[one, two, three]", "[one, twoBthree]", "13",
          "[[Hi, there], [Good, morning]]")
      ])
    await spark.stop()
  }

  @Test
  func selectConversionFunctions() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.range(1)
    let encoded = try await df.select(
      base64(lit("abc")), unbase64(lit("YWJj")).cast("string"),
      decode(encode(lit("abc"), "UTF-8"), "UTF-8"), encode(lit("abc"), "UTF-8").cast("string")
    ).collect()
    #expect(encoded == [Row("YWJj", "abc", "abc", "abc")])

    let converted = try await df.select(
      to_binary(lit("616263")).cast("string"), to_binary(lit("abc"), lit("utf-8")).cast("string"),
      try_to_binary(lit("616263")).cast("string"),
      try_to_binary(lit("abc"), lit("utf-8")).cast("string"),
      to_char(lit(454), lit("999")), to_varchar(lit(454), lit("999")),
      to_number(lit("454"), lit("999")).cast("int"),
      try_to_number(lit("454"), lit("999")).cast("int")
    ).collect()
    #expect(converted == [Row("abc", "abc", "abc", "abc", "454", "454", 454, 454)])

    let utf8 = try await df.select(
      is_valid_utf8(lit("abc")), make_valid_utf8(lit("abc")), validate_utf8(lit("abc")),
      try_validate_utf8(lit("abc"))
    ).collect()
    #expect(utf8 == [Row(true, "abc", "abc", "abc")])

    let others = try await df.select(
      collate(lit("abc"), "UTF8_BINARY"), char_length(randstr(lit(5))),
      char_length(randstr(lit(5), lit(0)))
    ).collect()
    #expect(others == [Row("abc", 5, 5)])
    await spark.stop()
  }
}
