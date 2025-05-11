# A Swift Network Word Count Application with Apache Spark Connect Swift Client

This is an example Swift stream processing application to show how to count words with Apache Spark Connect Swift Client library.

## Run `Spark Connect Server`

```bash
./sbin/start-connect-server.sh --wait -c spark.log.level=ERROR
```

## Run `Netcat` as a streaming input server

You will first need to run Netcat (a small utility found in most Unix-like systems) as a data server by using

```bash
nc -lk 9999
```

## Start streaming processing application

```bash
$ swift run
...
Connected to Apache Spark 4.0.0 Server
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
