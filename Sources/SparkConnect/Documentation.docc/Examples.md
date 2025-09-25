# Spark Connect Swift Examples

This document provides an overview of the example applications inside [Examples](https://github.com/apache/spark-connect-swift/tree/main/Examples) directory. These examples demonstrate how to use Spark Connect Swift to interact with Apache Spark in different contexts.

## Prepare Spark Connect Server

Start a Spark Connect Server:

```bash
docker run -it --rm -p 15002:15002 apache/spark:4.0.1 bash -c "/opt/spark/sbin/start-connect-server.sh --wait -c spark.log.level=ERROR"
```

## Basic Application Example

The basic application example demonstrates fundamental operations with Apache Spark Connect, including:

- Connecting to a Spark server
- Creating and manipulating tables with SQL
- Using DataFrame operations
- Reading and writing data in the ORC format

### Key Features

- SQL execution for table operations
- DataFrame transformations with filter operations
- Data persistence with ORC format
- Basic session management

### How to Run

Build and run the application:

```bash
# Using Docker
docker build -t apache/spark-connect-swift:app .
docker run -it --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:app

# From source code
swift run
```

## Spark SQL REPL(Read-Eval-Print Loop) Example

The Spark SQL REPL application example demonstrates interactive operations with ad-hoc Spark SQL queries with Apache Spark Connect, including:

- Connecting to a Spark server
- Receiving ad-hoc Spark SQL queries from users
- Show the SQL results interactively

### Key Features

- Spark SQL execution for table operations
- User interactions

### How to Run

Build and run the application:

```bash
# Using Docker
docker build -t apache/spark-connect-swift:spark-sql .
docker run -it --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:spark-sql

# From source code
swift run
```

## Pi Calculation Example

The Pi calculation example shows how to use Spark Connect Swift for computational tasks by calculating an approximation of π (pi) using the Monte Carlo method.

### Key Features

- Command-line argument handling
- Mathematical computations with Spark
- Random number generation
- Filtering and counting operations

### How to Run

Build and run the application:

```bash
# Using Docker
docker build -t apache/spark-connect-swift:pi .
docker run --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:pi

# From source code
swift run
```

## Streaming Example

The streaming example demonstrates how to process streaming data using Spark Connect Swift client, specifically for counting words from a network socket stream.

### Key Features

- Stream processing with Spark Connect
- Network socket data source
- Word counting with string operations
- Real-time console output

### How to Run

Start a Netcat server as the data source:

```bash
nc -lk 9999
```

Build and run the application:

```bash
# Using Docker
docker build -t apache/spark-connect-swift:stream .
docker run --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 -e TARGET_HOST=host.docker.internal apache/spark-connect-swift:stream

# From source code
swift run
```

Type text into the Netcat terminal to see real-time word counting from `Spark Connect Server` container.

## Web Application Example

The web application example showcases how to integrate Spark Connect Swift with a web server using the Vapor framework.

### Key Features

- HTTP server integration with Vapor
- REST API endpoints
- Spark session management within web requests
- Version information retrieval

### How to Run

Build and run the application:

```bash
# Using Docker
docker build -t apache/spark-connect-swift:web .
docker run -it --rm -p 8080:8080 -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:web

# From source code
swift run
```

Access the web application:

```bash
# Root endpoint
$ curl http://127.0.0.1:8080/
Welcome to the Swift world. Say hello!%

# Spark-powered endpoint
curl http://127.0.0.1:8080/hello
Hi, this is powered by the Apache Spark 4.0.1.%
```

## Development Environment

All examples include:

- A Dockerfile for containerized execution
- A Package.swift file for Swift Package Manager configuration
- A README.md with detailed instructions
- Source code in the Sources directory

These examples are designed to be used with Apache Spark 4.0 or newer, using the Spark Connect protocol for client-server interaction.
