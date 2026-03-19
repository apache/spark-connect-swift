# A Python application computing an approximation to pi with Apache Spark Connect Python Client

This is a reference example Python application to show how to use Apache Spark Connect Python Client library and compare it with Swift Client.

## How to run

Prepare `Spark Connect Server` via running Docker image.

```bash
docker run --rm -p 15002:15002 apache/spark:4.1.1 bash -c "/opt/spark/sbin/start-connect-server.sh --wait"
```

Build an application Docker image.

```bash
$ docker build -t apache/spark-connect-swift:pyspark-connect .
$ docker images apache/spark-connect-swift:pyspark-connect
IMAGE                                        ID             DISK USAGE   CONTENT SIZE   EXTRA
apache/spark-connect-swift:pyspark-connect   d2abec2df6f5       1.63GB          609MB
```

Run `pyspark-connect` docker image.

```bash
$ docker run --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:pyspark-connect
Pi is roughly 3.142020
```
