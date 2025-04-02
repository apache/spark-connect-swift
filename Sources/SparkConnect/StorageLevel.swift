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

/// Flags for controlling the storage of an `RDD`. Each ``StorageLevel`` records whether to use memory,
/// or `ExternalBlockStore`, whether to drop the `RDD` to disk if it falls out of memory or
/// `ExternalBlockStore`, whether to keep the data in memory in a serialized format, and whether
/// to replicate the `RDD` partitions on multiple nodes.
public struct StorageLevel: Sendable {
  /// Whether the cache should use disk or not.
  var useDisk: Bool

  /// Whether the cache should use memory or not.
  var useMemory: Bool

  /// Whether the cache should use off-heap or not.
  var useOffHeap: Bool

  /// Whether the cached data is deserialized or not.
  var deserialized: Bool

  /// The number of replicas.
  var replication: Int32

  init(useDisk: Bool, useMemory: Bool, useOffHeap: Bool, deserialized: Bool, replication: Int32 = 1)
  {
    self.useDisk = useDisk
    self.useMemory = useMemory
    self.useOffHeap = useOffHeap
    self.deserialized = deserialized
    self.replication = replication
  }

  public static let NONE = StorageLevel(
    useDisk: false, useMemory: false, useOffHeap: false, deserialized: false)
  public static let DISK_ONLY = StorageLevel(
    useDisk: true, useMemory: false, useOffHeap: false, deserialized: false)
  public static let DISK_ONLY_2 = StorageLevel(
    useDisk: true, useMemory: false, useOffHeap: false, deserialized: false, replication: 2)
  public static let DISK_ONLY_3 = StorageLevel(
    useDisk: true, useMemory: false, useOffHeap: false, deserialized: false, replication: 3)
  public static let MEMORY_ONLY = StorageLevel(
    useDisk: false, useMemory: true, useOffHeap: false, deserialized: false)
  public static let MEMORY_ONLY_2 = StorageLevel(
    useDisk: false, useMemory: true, useOffHeap: false, deserialized: false, replication: 2)
  public static let MEMORY_AND_DISK = StorageLevel(
    useDisk: true, useMemory: true, useOffHeap: false, deserialized: false)
  public static let MEMORY_AND_DISK_2 = StorageLevel(
    useDisk: true, useMemory: true, useOffHeap: false, deserialized: false, replication: 2)
  public static let OFF_HEAP = StorageLevel(
    useDisk: true, useMemory: true, useOffHeap: true, deserialized: false)
  public static let MEMORY_AND_DISK_DESER = StorageLevel(
    useDisk: true, useMemory: true, useOffHeap: false, deserialized: true)
}

extension StorageLevel {
  var toSparkConnectStorageLevel: Spark_Connect_StorageLevel {
    var level = Spark_Connect_StorageLevel()
    level.useDisk = self.useDisk
    level.useMemory = self.useMemory
    level.useOffHeap = self.useOffHeap
    level.deserialized = self.deserialized
    level.replication = self.replication
    return level
  }

  public static func == (lhs: StorageLevel, rhs: StorageLevel) -> Bool {
    return lhs.useDisk == rhs.useDisk && lhs.useMemory == rhs.useMemory
      && lhs.useOffHeap == rhs.useOffHeap && lhs.deserialized == rhs.deserialized
      && lhs.replication == rhs.replication
  }
}

extension StorageLevel: CustomStringConvertible {
  public var description: String {
    return
      "StorageLevel(useDisk: \(useDisk), useMemory: \(useMemory), useOffHeap: \(useOffHeap), deserialized: \(deserialized), replication: \(replication))"
  }
}

extension Spark_Connect_StorageLevel {
  var toStorageLevel: StorageLevel {
    return StorageLevel(
      useDisk: self.useDisk,
      useMemory: self.useMemory,
      useOffHeap: self.useOffHeap,
      deserialized: self.deserialized,
      replication: self.replication
    )
  }
}
