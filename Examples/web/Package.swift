// swift-tools-version:6.0
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
import PackageDescription

let package = Package(
  name: "SparkConnectSwiftWeb",
  platforms: [
    .macOS(.v15)
  ],
  dependencies: [
    // 💧 A server-side Swift web framework.
    .package(url: "https://github.com/vapor/vapor.git", from: "4.115.0"),
    // 🔵 Non-blocking, event-driven networking for Swift. Used for custom executors
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.65.0"),
    .package(url: "https://github.com/apache/spark-connect-swift.git", branch: "main"),
  ],
  targets: [
    .executableTarget(
      name: "SparkConnectSwiftWeb",
      dependencies: [
        .product(name: "Vapor", package: "vapor"),
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio"),
        .product(name: "SparkConnect", package: "spark-connect-swift"),
      ],
      swiftSettings: swiftSettings
    )
  ]
)

var swiftSettings: [SwiftSetting] {
  [
    .enableUpcomingFeature("ExistentialAny")
  ]
}
