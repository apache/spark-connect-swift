# Apache Spark Connect Client for Swift

[![GitHub Actions Build](https://github.com/apache/spark-connect-swift/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/apache/spark-connect-swift/blob/main/.github/workflows/build_and_test.yml)
[![Swift Version Compatibility](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fapache%2Fspark-connect-swift%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/apache/spark-connect-swift)
[![Platform Compatibility](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fapache%2Fspark-connect-swift%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/apache/spark-connect-swift)

This is an experimental Swift library to show how to connect to a remote Apache Spark Connect Server and run SQL statements to manipulate remote data.

So far, this library project is tracking the upstream changes of [Apache Arrow](https://arrow.apache.org) project's Swift-support.

## Resources

- [Homepage](https://apache.github.io/spark-connect-swift/)
- [Swift Package Index](https://swiftpackageindex.com/apache/spark-connect-swift/)
- [Library Documentation](https://swiftpackageindex.com/apache/spark-connect-swift/main/documentation/sparkconnect)

## Requirement

- [Apache Spark 4.0.0 (May 2025)](https://github.com/apache/spark/releases/tag/v4.0.0)
- [Swift 6.0 (2024) or 6.1 (2025)](https://swift.org)
- [gRPC Swift 2.1 (July 2025)](https://github.com/grpc/grpc-swift-2/releases/tag/2.1.0)
- [gRPC Swift Protobuf 2.1 (August 2025)](https://github.com/grpc/grpc-swift-protobuf/releases/tag/2.1.1)
- [gRPC Swift NIO Transport 2.1 (August 2025)](https://github.com/grpc/grpc-swift-nio-transport/releases/tag/2.1.0)
- [FlatBuffers v25.2.10 (February 2025)](https://github.com/google/flatbuffers/releases/tag/v25.2.10)
- [Apache Arrow Swift](https://github.com/apache/arrow-swift)

## How to use in your apps

Create a Swift project.

```bash
mkdir SparkConnectSwiftApp
cd SparkConnectSwiftApp
swift package init --name SparkConnectSwiftApp --type executable
```

Add `SparkConnect` package to the dependency like the following

```bash
$ cat Package.swift
import PackageDescription

let package = Package(
  name: "SparkConnectSwiftApp",
  platforms: [
    .macOS(.v15)
  ],
  dependencies: [
    .package(url: "https://github.com/apache/spark-connect-swift.git", branch: "main")
  ],
  targets: [
    .executableTarget(
      name: "SparkConnectSwiftApp",
      dependencies: [.product(name: "SparkConnect", package: "spark-connect-swift")]
    )
  ]
)
```

Use `SparkSession` of `SparkConnect` module in Swift.

```bash
$ cat Sources/main.swift

import SparkConnect

let spark = try await SparkSession.builder.getOrCreate()
print("Connected to Apache Spark \(await spark.version) Server")

let statements = [
  "DROP TABLE IF EXISTS t",
  "CREATE TABLE IF NOT EXISTS t(a INT) USING ORC",
  "INSERT INTO t VALUES (1), (2), (3)",
]

for s in statements {
  print("EXECUTE: \(s)")
  _ = try await spark.sql(s).count()
}
print("SELECT * FROM t")
try await spark.sql("SELECT * FROM t").cache().show()

try await spark.range(10).filter("id % 2 == 0").write.mode("overwrite").orc("/tmp/orc")
try await spark.read.orc("/tmp/orc").show()

await spark.stop()
```

Run your Swift application.

```bash
$ swift run
...
Connected to Apache Spark 4.0.0 Server
EXECUTE: DROP TABLE IF EXISTS t
EXECUTE: CREATE TABLE IF NOT EXISTS t(a INT)
EXECUTE: INSERT INTO t VALUES (1), (2), (3)
SELECT * FROM t
+---+
| a |
+---+
| 2 |
| 1 |
| 3 |
+---+
+----+
| id |
+----+
| 2  |
| 6  |
| 0  |
| 8  |
| 4  |
+----+
```

You can find more complete examples including `Spark SQL REPL`, `Web Server` and `Streaming` applications in the [Examples](https://github.com/apache/spark-connect-swift/tree/main/Examples) directory.

This library also supports `SPARK_REMOTE` environment variable to specify the [Spark Connect connection string](https://spark.apache.org/docs/latest/spark-connect-overview.html#set-sparkremote-environment-variable) in order to provide more options.
