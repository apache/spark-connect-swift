# A `Spark SQL REPL` Application with Apache Spark Connect Swift Client

This is an example Swift application to show how to develop a Spark SQL REPL(Read-eval-print Loop) with Apache Spark Connect Swift Client library.

## How to run

Prepare `Spark Connect Server` via running Docker image.

```
docker run -it --rm -p 15002:15002 apache/spark:4.0.0 bash -c "/opt/spark/sbin/start-connect-server.sh --wait"
```

Build an application Docker image.

```
$ docker build -t apache/spark-connect-swift:spark-sql .
$ docker images apache/spark-connect-swift:spark-sql
REPOSITORY                   TAG         IMAGE ID       CREATED         SIZE
apache/spark-connect-swift   spark-sql   265ddfec650d   7 seconds ago   390MB
```

Run `spark-sql` docker image.

```
$ docker run -it --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:spark-sql
Connected to Apache Spark 4.0.0 Server
spark-sql (default)> SHOW DATABASES;
+---------+
|namespace|
+---------+
|default  |
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

Apache Spark 4 supports [SQL Pipe Syntax](https://spark.apache.org/docs/4.0.0/sql-pipe-syntax.html).

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

Run from source code.

```
$ swift run
...
Connected to Apache Spark 4.0.0 Server
spark-sql (default)>
```
