# Apache Spark Connect Client for Swift

[![GitHub Actions Build](https://github.com/apache/spark-connect-swift/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/apache/spark-connect-swift/blob/main/.github/workflows/build_and_test.yml)
[![Swift Version Compatibility](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fapache%2Fspark-connect-swift%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/apache/spark-connect-swift)
[![Platform Compatibility](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fapache%2Fspark-connect-swift%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/apache/spark-connect-swift)

This is an experimental Swift library to show how to connect to a remote Apache Spark Connect Server and run SQL statements to manipulate remote data.

So far, this library project is tracking the upstream changes like the [Apache Spark](https://spark.apache.org) 4.0.0 RC4 release and [Apache Arrow](https://arrow.apache.org) project's Swift-support.

## Requirement

- [Apache Spark 4.0.0 RC4 (April 2025)](https://dist.apache.org/repos/dist/dev/spark/v4.0.0-rc4-bin/)
- [Swift 6.0 (2024) or 6.1 (2025)](https://swift.org)
- [gRPC Swift 2.1 (March 2025)](https://github.com/grpc/grpc-swift/releases/tag/2.1.2)
- [gRPC Swift Protobuf 1.2 (April 2025)](https://github.com/grpc/grpc-swift-protobuf/releases/tag/1.2.0)
- [gRPC Swift NIO Transport 1.0 (March 2025)](https://github.com/grpc/grpc-swift-nio-transport/releases/tag/1.0.3)
- [FlatBuffers v25.2.10 (February 2025)](https://github.com/google/flatbuffers/releases/tag/v25.2.10)
- [Apache Arrow Swift](https://github.com/apache/arrow/tree/main/swift)

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

You can find this example in the following repository.

- <https://github.com/dongjoon-hyun/spark-connect-swift-app>

## How to use `Spark SQL REPL` via `Spark Connect for Swift`

This project also provides `Spark SQL REPL`. You can run it directly from this repository.

```bash
$ swift run
...
Build of product 'SparkSQLRepl' complete! (2.33s)
Connected to Apache Spark 4.0.0 Server
spark-sql (default)> SHOW DATABASES;
+---------+
|namespace|
+---------+
|  default|
+---------+

Time taken: 30 ms
spark-sql (default)> CREATE DATABASE db1;
++
||
++
++

Time taken: 31 ms
spark-sql (default)> USE db1;
++
||
++
++

Time taken: 27 ms
spark-sql (db1)> CREATE TABLE t1 AS SELECT * FROM RANGE(10);
++
||
++
++

Time taken: 99 ms
spark-sql (db1)> SELECT * FROM t1;
+---+
| id|
+---+
|  1|
|  5|
|  3|
|  0|
|  6|
|  9|
|  4|
|  8|
|  7|
|  2|
+---+

Time taken: 80 ms
spark-sql (db1)> USE default;
++
||
++
++

Time taken: 26 ms
spark-sql (default)> DROP DATABASE db1 CASCADE;
++
||
++
++
spark-sql (default)> exit;
```

Apache Spark 4 supports [SQL Pipe Syntax](https://dist.apache.org/repos/dist/dev/spark/v4.0.0-rc4-docs/_site/sql-pipe-syntax.html).

```
$ swift run
...
Build of product 'SparkSQLRepl' complete! (2.33s)
Connected to Apache Spark 4.0.0 Server
spark-sql (default)>
FROM ORC.`/opt/spark/examples/src/main/resources/users.orc`
|> AGGREGATE COUNT(*) cnt
   GROUP BY name
|> ORDER BY cnt DESC, name ASC
;
+------+---+
|  name|cnt|
+------+---+
|Alyssa|  1|
|   Ben|  1|
+------+---+

Time taken: 159 ms
```

You can use `SPARK_REMOTE` to specify the [Spark Connect connection string](https://spark.apache.org/docs/latest/spark-connect-overview.html#set-sparkremote-environment-variable) in order to provide more options.

```bash
SPARK_REMOTE=sc://localhost swift run
```
