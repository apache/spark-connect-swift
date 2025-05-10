# A Swift Application with Apache Spark Connect Swift Client

This project is designed to illustrate a Swift-based HTTP WebServer with Apache Spark Connect.

- https://swiftpackageindex.com/apache/spark-connect-swift
- https://swiftpackageindex.com/vapor/vapor

## Create a Swift project

```
brew install vapor
vapor new spark-connect-swift-web -n
```

## Use `Apache Spark Connect Swift Client` package.

```
$ git diff HEAD
diff --git a/Package.swift b/Package.swift
index 477bcbd..3e7bb06 100644
--- a/Package.swift
+++ b/Package.swift
@@ -4,13 +4,14 @@ import PackageDescription
 let package = Package(
     name: "SparkConnectSwiftWebapp",
     platforms: [
-       .macOS(.v13)
+       .macOS(.v15)
     ],
     dependencies: [
         // 💧 A server-side Swift web framework.
         .package(url: "https://github.com/vapor/vapor.git", from: "4.110.1"),
         // 🔵 Non-blocking, event-driven networking for Swift. Used for custom executors
         .package(url: "https://github.com/apple/swift-nio.git", from: "2.65.0"),
+        .package(url: "https://github.com/apache/spark-connect-swift.git", branch: "main"),
     ],
     targets: [
         .executableTarget(
@@ -19,6 +20,7 @@ let package = Package(
                 .product(name: "Vapor", package: "vapor"),
                 .product(name: "NIOCore", package: "swift-nio"),
                 .product(name: "NIOPosix", package: "swift-nio"),
+                .product(name: "SparkConnect", package: "spark-connect-swift"),
             ],
             swiftSettings: swiftSettings
         ),
diff --git a/Sources/SparkConnectSwiftWebapp/routes.swift b/Sources/SparkConnectSwiftWebapp/routes.swift
index 2edcc8f..22313c8 100644
--- a/Sources/SparkConnectSwiftWebapp/routes.swift
+++ b/Sources/SparkConnectSwiftWebapp/routes.swift
@@ -1,4 +1,5 @@
 import Vapor
+import SparkConnect

 func routes(_ app: Application) throws {
     app.get { req async in
@@ -6,6 +7,15 @@ func routes(_ app: Application) throws {
     }

     app.get("hello") { req async -> String in
-        "Hello, world!"
+        return await Task {
+            do {
+                let spark = try await SparkSession.builder.getOrCreate()
+                let response = "Hi, this is powered by the Apache Spark \(await spark.version)."
+                await spark.stop()
+                return response
+            } catch {
+                return "Fail to connect: \(error)"
+            }
+        }.value
     }
 }
```

## Run this Swift application.

```
$ swift run
```

## Connect to the Swift Web Server to talk with `Apache Spark`.

```
$ curl http://127.0.0.1:8080/
Welcome to the Swift world. Say hello!%
$ curl http://127.0.0.1:8080/hello
Hi, this is powered by the Apache Spark 4.0.0.%
```
