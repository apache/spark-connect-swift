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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// A pure Swift implementation of the SHA-256 hash function (FIPS 180-4).
public struct SHA256 {

  /// The first 32 bits of the fractional parts of the cube roots of the first 64 prime numbers.
  private static let k: [UInt32] = [
    0x428a_2f98, 0x7137_4491, 0xb5c0_fbcf, 0xe9b5_dba5, 0x3956_c25b, 0x59f1_11f1, 0x923f_82a4,
    0xab1c_5ed5, 0xd807_aa98, 0x1283_5b01, 0x2431_85be, 0x550c_7dc3, 0x72be_5d74, 0x80de_b1fe,
    0x9bdc_06a7, 0xc19b_f174, 0xe49b_69c1, 0xefbe_4786, 0x0fc1_9dc6, 0x240c_a1cc, 0x2de9_2c6f,
    0x4a74_84aa, 0x5cb0_a9dc, 0x76f9_88da, 0x983e_5152, 0xa831_c66d, 0xb003_27c8, 0xbf59_7fc7,
    0xc6e0_0bf3, 0xd5a7_9147, 0x06ca_6351, 0x1429_2967, 0x27b7_0a85, 0x2e1b_2138, 0x4d2c_6dfc,
    0x5338_0d13, 0x650a_7354, 0x766a_0abb, 0x81c2_c92e, 0x9272_2c85, 0xa2bf_e8a1, 0xa81a_664b,
    0xc24b_8b70, 0xc76c_51a3, 0xd192_e819, 0xd699_0624, 0xf40e_3585, 0x106a_a070, 0x19a4_c116,
    0x1e37_6c08, 0x2748_774c, 0x34b0_bcb5, 0x391c_0cb3, 0x4ed8_aa4a, 0x5b9c_ca4f, 0x682e_6ff3,
    0x748f_82ee, 0x78a5_636f, 0x84c8_7814, 0x8cc7_0208, 0x90be_fffa, 0xa450_6ceb, 0xbef9_a3f7,
    0xc671_78f2,
  ]

  private static func rotr(_ x: UInt32, _ n: UInt32) -> UInt32 {
    return (x >> n) | (x << (32 - n))
  }

  /// Calculates the SHA-256 digest for the given Data.
  ///
  /// - Parameter data: The Data object for which to calculate the digest.
  /// - Returns: The calculated 32-byte SHA-256 digest as a [UInt8].
  public static func digest(data: Data) -> [UInt8] {
    var h: [UInt32] = [
      0x6a09_e667, 0xbb67_ae85, 0x3c6e_f372, 0xa54f_f53a,
      0x510e_527f, 0x9b05_688c, 0x1f83_d9ab, 0x5be0_cd19,
    ]

    var message = [UInt8](data)
    let bitLength = UInt64(message.count) &* 8
    message.append(0x80)
    while message.count % 64 != 56 {
      message.append(0)
    }
    for shift in stride(from: 56, through: 0, by: -8) {
      message.append(UInt8(truncatingIfNeeded: bitLength >> UInt64(shift)))
    }

    var w = [UInt32](repeating: 0, count: 64)
    for chunkStart in stride(from: 0, to: message.count, by: 64) {
      for i in 0..<16 {
        let offset = chunkStart + i * 4
        w[i] =
          (UInt32(message[offset]) << 24) | (UInt32(message[offset + 1]) << 16)
          | (UInt32(message[offset + 2]) << 8) | UInt32(message[offset + 3])
      }
      for i in 16..<64 {
        let s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3)
        let s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10)
        w[i] = w[i - 16] &+ s0 &+ w[i - 7] &+ s1
      }

      var a = h[0]
      var b = h[1]
      var c = h[2]
      var d = h[3]
      var e = h[4]
      var f = h[5]
      var g = h[6]
      var hh = h[7]
      for i in 0..<64 {
        let s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)
        let ch = (e & f) ^ (~e & g)
        let temp1 = hh &+ s1 &+ ch &+ k[i] &+ w[i]
        let s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)
        let maj = (a & b) ^ (a & c) ^ (b & c)
        let temp2 = s0 &+ maj
        hh = g
        g = f
        f = e
        e = d &+ temp1
        d = c
        c = b
        b = a
        a = temp1 &+ temp2
      }
      h[0] &+= a
      h[1] &+= b
      h[2] &+= c
      h[3] &+= d
      h[4] &+= e
      h[5] &+= f
      h[6] &+= g
      h[7] &+= hh
    }

    var digest = [UInt8]()
    digest.reserveCapacity(32)
    for value in h {
      digest.append(UInt8(truncatingIfNeeded: value >> 24))
      digest.append(UInt8(truncatingIfNeeded: value >> 16))
      digest.append(UInt8(truncatingIfNeeded: value >> 8))
      digest.append(UInt8(truncatingIfNeeded: value))
    }
    return digest
  }

  /// Calculates the SHA-256 digest for the given Data as a lowercase hex string.
  ///
  /// - Parameter data: The Data object for which to calculate the digest.
  /// - Returns: The calculated SHA-256 digest as a 64-character lowercase hex String.
  public static func hexString(data: Data) -> String {
    let hexDigits = Array("0123456789abcdef".utf8)
    var chars = [UInt8]()
    chars.reserveCapacity(64)
    for byte in digest(data: data) {
      chars.append(hexDigits[Int(byte >> 4)])
      chars.append(hexDigits[Int(byte & 0x0F)])
    }
    return String(decoding: chars, as: UTF8.self)
  }
}
