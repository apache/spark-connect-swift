# A Swift Application with Apache Spark Connect Swift Client

This project is designed to illustrate a Swift-based HTTP WebServer with Apache Spark Connect.

- <https://swiftpackageindex.com/vapor/vapor>

## Create a Swift project

```bash
brew install vapor
vapor new spark-connect-swift-web -n
```

## Use `Apache Spark Connect Swift Client` package

```bash
$ git diff HEAD
diff --git a/Package.swift b/Package.swift
index 2edcc8f..dd918a9 100644
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
         .package(url: "https://github.com/vapor/vapor.git", from: "4.115.0"),
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
diff --git a/Sources/SparkConnectSwiftWeb/configure.swift b/Sources/SparkConnectSwiftWeb/configure.swift
index 7715d7c..eea2f95 100644
--- a/Sources/SparkConnectSwiftWeb/configure.swift
+++ b/Sources/SparkConnectSwiftWeb/configure.swift
@@ -2,6 +2,7 @@ import Vapor

 // configures your application
 public func configure(_ app: Application) async throws {
+    app.http.server.configuration.hostname = "0.0.0.0"
     // uncomment to serve files from /Public folder
     // app.middleware.use(FileMiddleware(publicDirectory: app.directory.publicDirectory))

diff --git a/Sources/SparkConnectSwiftWeb/routes.swift b/Sources/SparkConnectSwiftWeb/routes.swift
index 2edcc8f..dd918a9 100644
--- a/Sources/SparkConnectSwiftWeb/routes.swift
+++ b/Sources/SparkConnectSwiftWeb/routes.swift
@@ -1,11 +1,21 @@
 import Vapor
+import SparkConnect

 func routes(_ app: Application) throws {
     app.get { req async in
-        "It works!"
+        "Welcome to the Swift world. Say hello!"
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

## How to run

Prepare `Spark Connect Server` via running Docker image.

```bash
docker run --rm -p 15002:15002 apache/spark:4.1.0 bash -c "/opt/spark/sbin/start-connect-server.sh --wait"
```

Build an application Docker image.

```bash
$ docker build -t apache/spark-connect-swift:web .
$ docker images apache/spark-connect-swift:web
IMAGE                            ID             DISK USAGE   CONTENT SIZE   EXTRA
apache/spark-connect-swift:web   a3865bf1867e        600MB          139MB
```

Run `web` docker image

```bash
$ docker run -it --rm -p 8080:8080 -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:web
[ NOTICE ] Server started on http://127.0.0.1:8080
```

Connect to the Swift Web Server to talk with `Apache Spark`.

```bash
$ curl http://127.0.0.1:8080/
Welcome to the Swift world. Say hello!%

$ curl http://127.0.0.1:8080/hello
Hi, this is powered by the Apache Spark 4.1.0.%
```

Run from source code.

```bash
swift run
```
