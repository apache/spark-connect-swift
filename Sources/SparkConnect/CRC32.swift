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

import Foundation

public struct CRC32 {

  /// Pre-computed CRC32 table
  private static let crcTable: [UInt32] = {
    var table = [UInt32](repeating: 0, count: 256)
    let polynomial: UInt32 = 0xEDB8_8320  // IEEE 802.3 polynomial

    for i in 0..<256 {
      var c = UInt32(i)
      for _ in 0..<8 {
        if (c & 1) == 1 {
          c = polynomial ^ (c >> 1)
        } else {
          c = c >> 1
        }
      }
      table[i] = c
    }
    return table
  }()

  /// Calculates the CRC32 checksum for the given Data.
  ///
  /// - Parameter data: The Data object for which to calculate the checksum.
  /// - Returns: The calculated CRC32 checksum as a UInt32.
  public static func checksum(data: Data) -> UInt32 {
    var crc: UInt32 = 0xFFFF_FFFF

    data.withUnsafeBytes { (pointer: UnsafeRawBufferPointer) in
      for byte in pointer.bindMemory(to: UInt8.self) {
        crc = (crc >> 8) ^ crcTable[Int((crc ^ UInt32(byte)) & 0xFF)]
      }
    }
    return ~crc
  }

  /// Calculates the CRC32 checksum for the given String.
  ///
  /// - Parameter string: The String object for which to calculate the checksum.
  /// - Parameter encoding: The encoding to use when converting the string to Data (defaults to .utf8).
  /// - Returns: The calculated CRC32 checksum as a UInt32. Returns nil if the string cannot be converted to Data.
  public static func checksum(string: String, encoding: String.Encoding = .utf8) -> UInt32? {
    guard let data = string.data(using: encoding) else {
      return nil
    }
    return checksum(data: data)
  }

  /// Calculates the CRC32 checksum for the given array of bytes.
  ///
  /// - Parameter bytes: The [UInt8] array for which to calculate the checksum.
  /// - Returns: The calculated CRC32 checksum as a UInt32.
  public static func checksum(bytes: [UInt8]) -> UInt32 {
    var crc: UInt32 = 0xFFFF_FFFF

    for byte in bytes {
      crc = (crc >> 8) ^ crcTable[Int((crc ^ UInt32(byte)) & 0xFF)]
    }
    return ~crc
  }
}
