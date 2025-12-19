# A `Spark SQL REPL` Application with Apache Spark Connect Swift Client

This is an example Swift application to show how to develop a Spark SQL REPL(Read-eval-print Loop) with Apache Spark Connect Swift Client library.

## How to run

Prepare `Spark Connect Server` via running Docker image.

```bash
docker run -it --rm -p 15002:15002 apache/spark:4.1.0 bash -c "/opt/spark/sbin/start-connect-server.sh --wait"
```

Build an application Docker image.

```bash
$ docker build -t apache/spark-connect-swift:spark-sql .
$ docker images apache/spark-connect-swift:spark-sql
IMAGE                                ID             DISK USAGE   CONTENT SIZE   EXTRA
apache/spark-connect-swift:spark-sql f97a7af7b0ff        550MB          128MB
```

Run `spark-sql` docker image.

```bash
$ docker run -it --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:spark-sql
Connected to Apache Spark 4.1.0 Server
spark-sql (default)> SHOW DATABASES;
+---------+
|namespace|
+---------+
|default  |
+---------+

Time taken: 276 ms
spark-sql (default)> CREATE DATABASE db1;
++
||
++
++

Time taken: 63 ms
spark-sql (default)> USE db1;
++
||
++
++

Time taken: 41 ms
spark-sql (db1)> CREATE TABLE t1 AS SELECT * FROM RANGE(10);
++
||
++
++

Time taken: 893 ms
spark-sql (db1)> SELECT * FROM t1;
+---+
|id |
+---+
|3  |
|8  |
|5  |
|9  |
|1  |
|6  |
|7  |
|0  |
|4  |
|2  |
+---+

Time taken: 267 ms
spark-sql (db1)> USE default;
++
||
++
++

Time taken: 28 ms
spark-sql (default)> DROP DATABASE db1 CASCADE;
++
||
++
++

Time taken: 46 ms
spark-sql (default)> exit;
```

Apache Spark 4 supports [SQL Pipe Syntax](https://spark.apache.org/docs/4.1.0/sql-pipe-syntax.html).
Run from source code at this time.

```bash
$ swift run
...
Build of product 'SparkSQLRepl' complete! (2.33s)
Connected to Apache Spark 4.1.0 Server
spark-sql (default)>
FROM ORC.`/opt/spark/examples/src/main/resources/users.orc`
|> AGGREGATE COUNT(*) cnt
   GROUP BY name
|> ORDER BY cnt DESC, name ASC
;
+------+---+
|name  |cnt|
+------+---+
|Alyssa|1  |
|Ben   |1  |
+------+---+

Time taken: 550 ms
```

