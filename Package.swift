// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.
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
  name: "SparkConnect",
  platforms: [
    .macOS(.v15),
    .iOS(.v18),
    .watchOS(.v11),
    .tvOS(.v18),
  ],
  products: [
    .library(
      name: "SparkConnect",
      targets: ["SparkConnect"])
  ],
  dependencies: [
    .package(url: "https://github.com/grpc/grpc-swift-2.git", exact: "2.1.0"),
    .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", exact: "2.1.1"),
    .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", exact: "2.1.1"),
    .package(url: "https://github.com/google/flatbuffers.git", branch: "v25.2.10"),
  ],
  targets: [
    .target(
      name: "SparkConnect",
      dependencies: [
        .product(name: "GRPCCore", package: "grpc-swift-2"),
        .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
        .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
        .product(name: "FlatBuffers", package: "flatbuffers"),
      ],
      resources: [
        .process("Documentation.docc")
      ]
    ),
    .testTarget(
      name: "SparkConnectTests",
      dependencies: ["SparkConnect"],
      resources: [
        .copy("Resources/queries")
      ]
    ),
  ]
)
