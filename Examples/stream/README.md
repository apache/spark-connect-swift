# A Swift Network Word Count Application with Apache Spark Connect Swift Client

This is an example Swift stream processing application to show how to count words with Apache Spark Connect Swift Client library.

## Run `Spark Connect Server`

```bash
docker run --rm -p 15002:15002 apache/spark:4.0.0-preview2 bash -c "/opt/spark/sbin/start-connect-server.sh --wait -c spark.log.level=ERROR"
```

## Run `Netcat` as a streaming input server

You will first need to run Netcat (a small utility found in most Unix-like systems) as a data server by using

```bash
nc -lk 9999
```

## Build and run from docker image

Build an application Docker image.

```
$ docker build -t apache/spark-connect-swift:stream .
$ docker images apache/spark-connect-swift:stream
REPOSITORY                   TAG       IMAGE ID       CREATED         SIZE
apache/spark-connect-swift   stream    a4daa10ad9c5   7 seconds ago   369MB
```

Run `stream` docker image.

```
$ docker run --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 -e TARGET_HOST=host.docker.internal apache/spark-connect-swift:stream
```

## Send input and check output

Then, any lines typed in the terminal running the `Netcat` server will be counted and printed on screen every second.

```bash
$ nc -lk 9999
apache spark
apache hadoop
```

`Spark Connect Server` output will look something like the following.

```bash
-------------------------------------------
Batch: 0
-------------------------------------------
+----+--------+
|word|count(1)|
+----+--------+
+----+--------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------+--------+
|  word|count(1)|
+------+--------+
|apache|       1|
| spark|       1|
+------+--------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------+--------+
|  word|count(1)|
+------+--------+
|apache|       2|
| spark|       1|
|hadoop|       1|
+------+--------+
```

## Run from source code

```bash
$ swift run
...
Connected to Apache Spark 4.0.0 Server
```
