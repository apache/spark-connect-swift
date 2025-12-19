# A Swift Application computing an approximation to pi with Apache Spark Connect Swift Client

This is an example Swift application to show how to use Apache Spark Connect Swift Client library.

## How to run

Prepare `Spark Connect Server` via running Docker image.

```bash
docker run --rm -p 15002:15002 apache/spark:4.1.0 bash -c "/opt/spark/sbin/start-connect-server.sh --wait"
```

Build an application Docker image.

```bash
$ docker build -t apache/spark-connect-swift:pi .
$ docker images apache/spark-connect-swift:pi
IMAGE                           ID             DISK USAGE   CONTENT SIZE   EXTRA
apache/spark-connect-swift:pi   cae3fd3b9833        550MB          128MB
```

Run `pi` docker image.

```bash
$ docker run --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:pi
Pi is roughly 3.1434711434711433
```

Run from source code.

```bash
$ swift run
...
Pi is roughly 3.143995143995144
```
