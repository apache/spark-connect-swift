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

/// A test suite for `CryptoFunctions`
@Suite(.serialized)
struct CryptoFunctionsTests {

  /// An obvious dummy 16-byte AES key used only by these tests.
  let key = "0000000000000000"

  @Test
  func aesFunctions() throws {
    for (column, name, count) in [
      (aes_encrypt(col("a"), col("k")), "aes_encrypt", 6),
      (aes_decrypt(col("a"), col("k")), "aes_decrypt", 5),
      (try_aes_decrypt(col("a"), col("k")), "try_aes_decrypt", 5),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.functionName == name)
      #expect(expr.unresolvedFunction.arguments.count == count)
      #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "a")
      #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "k")
      #expect(expr.unresolvedFunction.arguments[2].literal.string == "GCM")
      #expect(expr.unresolvedFunction.arguments[3].literal.string == "DEFAULT")
    }
  }

  /// The omitted optional arguments are filled with their default literals, so the argument
  /// count never changes.
  @Test
  func aesOptionalArguments() throws {
    let encrypt = aes_encrypt(
      col("a"), col("k"), mode: lit("CBC"), padding: lit("PKCS"), iv: col("iv"), aad: col("aad")
    ).expr
    #expect(encrypt.unresolvedFunction.arguments.count == 6)
    #expect(encrypt.unresolvedFunction.arguments[2].literal.string == "CBC")
    #expect(encrypt.unresolvedFunction.arguments[3].literal.string == "PKCS")
    #expect(encrypt.unresolvedFunction.arguments[4].unresolvedAttribute.unparsedIdentifier == "iv")
    #expect(encrypt.unresolvedFunction.arguments[5].unresolvedAttribute.unparsedIdentifier == "aad")

    // `aes_encrypt` has an `iv` argument, but `aes_decrypt` and `try_aes_decrypt` do not.
    let defaultIv = aes_encrypt(col("a"), col("k")).expr.unresolvedFunction.arguments[4]
    #expect(defaultIv.literal.string == "")
    for column in [
      aes_decrypt(col("a"), col("k"), mode: lit("ECB"), padding: lit("PKCS"), aad: col("aad")),
      try_aes_decrypt(col("a"), col("k"), mode: lit("ECB"), padding: lit("PKCS"), aad: col("aad")),
    ] {
      let expr = column.expr
      #expect(expr.unresolvedFunction.arguments.count == 5)
      #expect(expr.unresolvedFunction.arguments[2].literal.string == "ECB")
      #expect(expr.unresolvedFunction.arguments[3].literal.string == "PKCS")
      #expect(expr.unresolvedFunction.arguments[4].unresolvedAttribute.unparsedIdentifier == "aad")
    }
    let defaultAad = aes_decrypt(col("a"), col("k")).expr.unresolvedFunction.arguments[4]
    #expect(defaultAad.literal.string == "")
  }

  /// Unlike the AES functions, `hmac` omits the optional `algorithm` argument instead of
  /// filling it with a default literal.
  @Test
  func hmacFunction() throws {
    let expr = hmac(col("k"), col("m")).expr
    #expect(expr.unresolvedFunction.functionName == "hmac")
    #expect(expr.unresolvedFunction.arguments.count == 2)
    #expect(expr.unresolvedFunction.arguments[0].unresolvedAttribute.unparsedIdentifier == "k")
    #expect(expr.unresolvedFunction.arguments[1].unresolvedAttribute.unparsedIdentifier == "m")

    let withAlgorithm = hmac(col("k"), col("m"), lit("SHA-1")).expr
    #expect(withAlgorithm.unresolvedFunction.functionName == "hmac")
    #expect(withAlgorithm.unresolvedFunction.arguments.count == 3)
    #expect(withAlgorithm.unresolvedFunction.arguments[2].literal.string == "SHA-1")
  }

  /// GCM uses a random initialization vector, so only the round trip is deterministic.
  @Test
  func aesRoundTrip() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 'Spark' AS a")
    let rows = try await df.select(
      aes_decrypt(aes_encrypt(col("a"), lit(key)), lit(key)).cast("STRING"),
      aes_decrypt(
        aes_encrypt(col("a"), lit(key), mode: lit("CBC"), padding: lit("PKCS")),
        lit(key), mode: lit("CBC"), padding: lit("PKCS")
      ).cast("STRING"),
      try_aes_decrypt(aes_encrypt(col("a"), lit(key)), lit(key)).cast("STRING")
    ).collect()
    #expect(rows == [Row("Spark", "Spark", "Spark")])
    await spark.stop()
  }

  /// ECB has neither an initialization vector nor authentication, so its ciphertext is
  /// deterministic.
  @Test
  func aesEcbIsDeterministic() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 'Spark' AS a")
    let encrypt = aes_encrypt(col("a"), lit(key), mode: lit("ECB"), padding: lit("PKCS"))
    let rows = try await df.select(
      hex(encrypt),
      aes_decrypt(encrypt, lit(key), mode: lit("ECB"), padding: lit("PKCS")).cast("STRING")
    ).collect()
    #expect(rows == [Row("0776185876454AAB6963D68360C120D9", "Spark")])
    await spark.stop()
  }

  /// A wrong key makes `aes_decrypt` fail, while `try_aes_decrypt` returns `NULL`.
  @Test
  func tryAesDecryptReturnsNullOnWrongKey() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    let df = try await spark.sql("SELECT 'Spark' AS a")
    let encrypted = aes_encrypt(col("a"), lit(key))
    let wrongKey = lit("1111111111111111")
    let rows = try await df.select(try_aes_decrypt(encrypted, wrongKey).cast("STRING")).collect()
    #expect(rows == [Row(nil)])

    try await #require(throws: Error.self) {
      try await df.select(aes_decrypt(encrypted, wrongKey).cast("STRING")).collect()
    }
    await spark.stop()
  }

  @Test
  func hmacRoundTrip() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    if await isSparkVersionAtLeast(spark.version, "4.3") {
      let df = try await spark.sql("SELECT 'key' AS k, 'message' AS m")
      let rows = try await df.select(
        hex(hmac(col("k"), col("m"))),
        hex(hmac(col("k"), col("m"), lit("SHA-1")))
      ).collect()
      #expect(
        rows == [
          Row(
            "6E9EF29B75FFFC5B7ABAE527D58FDADB2FE42E7219011976917343065F58ED4A",
            "2088DF74D5F2146B48146CAF4965377E9D0BE3A4"
          )
        ])
    }
    await spark.stop()
  }
}
