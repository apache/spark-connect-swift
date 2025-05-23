# A Swift Application with Apache Spark Connect Swift Client

This is an example Swift application to show how to use Apache Spark Connect Swift Client library.

## How to run

Prepare `Spark Connect Server` via running Docker image.

```
docker run --rm -p 15002:15002 apache/spark:4.0.0 bash -c "/opt/spark/sbin/start-connect-server.sh --wait"
```

Build an application Docker image.

```
$ docker build -t apache/spark-connect-swift:app .
$ docker images apache/spark-connect-swift:app
REPOSITORY                   TAG       IMAGE ID       CREATED         SIZE
apache/spark-connect-swift   app       e132e1b38348   5 seconds ago   368MB
```

Run `app` docker image.

```
$ docker run --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:app
Connected to Apache Spark 4.0.0 Server
EXECUTE: DROP TABLE IF EXISTS t
EXECUTE: CREATE TABLE IF NOT EXISTS t(a INT) USING ORC
EXECUTE: INSERT INTO t VALUES (1), (2), (3)
SELECT * FROM t
+---+
|  a|
+---+
|  2|
|  1|
|  3|
+---+

+---+
| id|
+---+
|  0|
|  8|
|  6|
|  2|
|  4|
+---+
```

Run from source code.

```
$ swift run
...
Connected to Apache Spark 4.0.0 Server
EXECUTE: DROP TABLE IF EXISTS t
EXECUTE: CREATE TABLE IF NOT EXISTS t(a INT) USING ORC
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
