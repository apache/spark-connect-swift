# A Swift Application with Apache Spark Connect Swift Client

This is an example Swift application to show how to use Apache Spark Connect Swift Client library.

## How to run

Run this Swift application.

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
