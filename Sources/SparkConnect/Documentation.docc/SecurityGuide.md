# Security Best Practices

Recommendations for connecting to Apache Spark Connect servers securely in production.

## Transport Security (TLS)

Following the
[Spark Connect connection string specification](https://github.com/apache/spark/blob/master/sql/connect/docs/client-connection-string.md),
the connection is **plaintext** unless the connection string contains
`use_ssl=true`. Always set `use_ssl=true` when connecting to any server other
than `localhost`.

```swift
let spark = try await SparkSession
    .builder
    .remote("sc://spark.example.com:15002/;use_ssl=true")
    .getOrCreate()
```

## Authentication Tokens

The client sends the authentication token as an `Authorization: Bearer` header
on every request. Prefer the `SPARK_CONNECT_AUTHENTICATE_TOKEN` environment
variable over the `token=` connection string parameter, because connection
strings can be exposed through shell history, process listings, and logs.

```bash
export SPARK_CONNECT_AUTHENTICATE_TOKEN=...
```

The environment variable is used only when the connection string has no
`token=` parameter. Since a bearer token sent over a plaintext connection can
be intercepted, always combine token authentication with `use_ssl=true`.

## Certificate Validation

When TLS is enabled, the server certificate is validated against the operating
system trust store. Custom CA bundles, mutual TLS (client certificates), and
certificate pinning are not currently supported, so the server certificate
must chain to a root CA trusted by the operating system.
