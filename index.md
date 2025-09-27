# Apache Spark™ Connect Client for Swift

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Apache Spark™ Connect for Swift is a subproject of [Apache Spark](https://spark.apache.org/) and
aims to provide a modern Swift library to enable Swift developers to leverage the power of
Apache Spark for distributed data processing, machine learning, and analytical workloads directly
from their Swift applications.

## Releases

- [0.4.0 RC1 (2025-09-27)](https://github.com/apache/spark-connect-swift/releases/tag/0.4.0-rc.1)
- [0.3.0 (2025-06-04)](https://github.com/apache/spark-connect-swift/releases/tag/0.3.0)
- [0.2.0 (2025-05-20)](https://github.com/apache/spark-connect-swift/releases/tag/0.2.0)
- [0.1.0 (2025-05-07)](https://github.com/apache/spark-connect-swift/releases/tag/v0.1.0)

## Compatibility

[![Swift Version Compatibility](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fapache%2Fspark-connect-swift%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/apache/spark-connect-swift)

[![Platform Compatibility](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fapache%2Fspark-connect-swift%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/apache/spark-connect-swift)

## Swift Package Index

- <https://swiftpackageindex.com/apache/spark-connect-swift/>

## Run an example

### Install [Spark K8s Operator](https://apache.github.io/spark-kubernetes-operator/) Helm Chart

```bash
helm repo add spark https://apache.github.io/spark-kubernetes-operator
helm install spark spark/spark-kubernetes-operator
```

### Launch Spark Connect Server

```bash
$ kubectl apply -f https://apache.github.io/spark-kubernetes-operator/spark-connect-server.yaml
sparkapplication.spark.apache.org/spark-connect-server created

$ kubectl get sparkapp
NAME                   CURRENT STATE    AGE
spark-connect-server   RunningHealthy   14s
```

### Launch `Swift-based SparkPi` Application

```bash
$ kubectl apply -f https://apache.github.io/spark-kubernetes-operator/pi-swift.yaml
job.batch/spark-connect-swift-pi created

$ kubectl logs -f job/spark-connect-swift-pi
Pi is roughly 3.1426151426151425
```

## Library Documentation

- [main](https://swiftpackageindex.com/apache/spark-connect-swift/main/documentation/sparkconnect/)
- [0.3.0](https://swiftpackageindex.com/apache/spark-connect-swift/0.3.0/documentation/sparkconnect)
- [0.2.0](https://swiftpackageindex.com/apache/spark-connect-swift/0.2.0/documentation/sparkconnect)
- [0.1.0](https://swiftpackageindex.com/apache/spark-connect-swift/v0.1.0/documentation/sparkconnect)

## Articles

- [Getting Started with SparkConnect](https://swiftpackageindex.com/apache/spark-connect-swift/main/documentation/sparkconnect/gettingstarted)
- [Spark Connect Swift Examples](https://swiftpackageindex.com/apache/spark-connect-swift/main/documentation/sparkconnect/examples)
  - [Basic Application](https://github.com/apache/spark-connect-swift/tree/main/Examples/app)
  - [Pi Calculation](https://github.com/apache/spark-connect-swift/tree/main/Examples/pi)
  - [Structured Streaming](https://github.com/apache/spark-connect-swift/tree/main/Examples/stream)
  - [HTTP Web Server](https://github.com/apache/spark-connect-swift/tree/main/Examples/web)
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html#spark-connect-overview)
