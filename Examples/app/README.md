# A Swift Application with Apache Spark Connect Swift Client

This is an example Swift application to show how to use Apache Spark Connect Swift Client library.

## How to run

Prepare `Spark Connect Server` via running Docker image.

```bash
docker run --rm -p 15002:15002 apache/spark:4.1.0 bash -c "/opt/spark/sbin/start-connect-server.sh --wait"
```

Build an application Docker image.

```bash
$ docker build -t apache/spark-connect-swift:app .
$ docker images apache/spark-connect-swift:app
IMAGE                            ID             DISK USAGE   CONTENT SIZE   EXTRA
apache/spark-connect-swift:app   fa24c1e88713        550MB          128MB
```

Run `app` docker image.

```bash
$ docker run --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:app
Connected to Apache Spark 4.1.0 Server
EXECUTE: DROP TABLE IF EXISTS t
EXECUTE: CREATE TABLE IF NOT EXISTS t(a INT) USING ORC
EXECUTE: INSERT INTO t VALUES (1), (2), (3)
SELECT * FROM t
+---+
|  a|
+---+
|  3|
|  1|
|  2|
+---+

+---+
| id|
+---+
|  0|
|  4|
|  2|
|  8|
|  6|
+---+
```

Run from source code.

```bash
$ swift run
...
Connected to Apache Spark 4.1.0 Server
EXECUTE: DROP TABLE IF EXISTS t
EXECUTE: CREATE TABLE IF NOT EXISTS t(a INT) USING ORC
EXECUTE: INSERT INTO t VALUES (1), (2), (3)
SELECT * FROM t
+---+
|  a|
+---+
|  1|
|  2|
|  3|
+---+

+---+
| id|
+---+
|  8|
|  4|
|  0|
|  6|
|  2|
+---+
```
