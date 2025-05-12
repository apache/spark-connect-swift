# A Swift Application computing an approximation to pi with Apache Spark Connect Swift Client

This is an example Swift application to show how to use Apache Spark Connect Swift Client library.

## How to run

Prepare `Spark Connect Server` via running Docker image.
```
docker run --rm -p 15002:15002 apache/spark:4.0.0-preview2 bash -c "/opt/spark/sbin/start-connect-server.sh --wait"
```

Build an application Docker image.

```
$ docker build -t apache/spark-connect-swift:pi .
$ docker images apache/spark-connect-swift:pi
REPOSITORY                   TAG       IMAGE ID       CREATED         SIZE
apache/spark-connect-swift   pi        d03952577564   4 seconds ago   369MB
```

Run `pi` docker image.

```
$ docker run --rm -e SPARK_REMOTE=sc://host.docker.internal:15002 apache/spark-connect-swift:pi
Pi is roughly 3.1412831412831412
```

Run from source code.

```
$ swift run
...
Pi is roughly 3.1423711423711422
```
