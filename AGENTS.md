# Agent Guide — Apache Spark Connect Client for Swift

Guidance for AI coding agents working in this repository. For end-user usage,
see [README.md](README.md).

## Project overview

`SparkConnect` is a modern Swift client library for the
[Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
protocol, developed as a subproject of Apache Spark. It lets Swift applications
drive a remote Apache Spark cluster (DataFrame/SQL operations, streaming,
catalog, ML) over gRPC, exchanging results in Apache Arrow format.

- Single library product: `SparkConnect`.
- Public API mirrors PySpark/Spark SQL (`SparkSession`, `DataFrame`,
  `DataFrameReader`/`Writer`, `Catalog`, `RuntimeConf`, streaming, etc.).
- License: Apache-2.0. JIRA project: `SPARK` (issues.apache.org/jira).

## Tech stack & requirements

- **Swift 6.3.2+** with Swift Package Manager (`swift-tools-version: 6.3.2`).
- **Platforms**: macOS 15+, iOS 18+, watchOS 11+, tvOS 18+, and Linux
  (built and tested on Ubuntu x86_64 & arm64 with the Swift 6.3.2 toolchain).
- **Server**: Apache Spark 4.x Connect server (tested against 4.0.2 / 4.1.2 / 4.2.0-preview).
- **Dependencies** (all pinned with `exact` in [Package.swift](Package.swift)):
  `grpc-swift-2`, `grpc-swift-protobuf`, `grpc-swift-nio-transport`,
  `flatbuffers`, `swift-system`. Keep version bumps as `exact` pins, never
  `branch`/`from` (see `git log` for `SPARK-57094`).

## Repository layout

- `Sources/SparkConnect/` — the library.
  - Hand-written API: `SparkSession.swift`, `DataFrame*.swift`, `Catalog.swift`,
    `RuntimeConf.swift`, `DataStream*.swift`, `StreamingQuery*.swift`,
    `MergeIntoWriter.swift`, `SparkConnectClient.swift`, etc.
  - **Generated — do not hand-edit**: `*.pb.swift` (protobuf), `base.grpc.swift`
    (gRPC), `Flight.pb.swift`, and FlatBuffers `*_generated.swift`
    (`File_generated`, `Message_generated`, `Schema_generated`, `Tensor_generated`,
    `SparseTensor_generated`). These derive from upstream Spark Connect protos
    and Arrow schemas.
  - `Arrow*.swift` — a vendored Arrow implementation tracking
    [apache/arrow-swift](https://github.com/apache/arrow-swift); treat as upstream.
  - `Documentation.docc/` — DocC docs (published to Swift Package Index).
- `Tests/SparkConnectTests/` — test suite (Swift Testing). `Resources/queries/`
  holds golden SQL result files.
- `Examples/` — runnable sample apps (`pi`, `spark-sql`, `stream`, `web`, `app`,
  `pyspark-connect`), each with its own `Package.swift` and `Dockerfile`.
- `dev/` — Python maintainer scripts (JIRA + PR merge tooling).
- `.github/workflows/build_main.yml` — CI entry point: runs on push to all
  branches and calls the reusable `build_and_test.yml` (license check,
  multi-platform build, integration tests). `notify_test_workflow.yml` and
  `update_build_status.yml` mirror forked-repo CI results onto PR checks.

## Build

```bash
swift build                 # debug build
swift build -c release      # what CI builds
```

## Test

Tests are **integration tests**: they require a live Spark Connect server and
run **serially**. Without a reachable server they will fail.

```bash
# Build the test target without running (quick compile check):
swift test --filter NOTHING -c release

# Run the full suite (needs a running server, see below):
swift test --no-parallel -c release

# Run a single suite:
swift test --no-parallel --filter DataFrameTests
```

Start a local Spark Connect server first (default endpoint `sc://localhost:15002`):

```bash
docker run -it --rm -p 15002:15002 -e SPARK_NO_DAEMONIZE=1 \
  apache/spark:4.2.0-preview5 bash -c /opt/spark/sbin/start-connect-server.sh
```

Environment variables that gate behavior:

- `SPARK_REMOTE` — Spark Connect connection string (default `sc://localhost:15002`).
- `SPARK_CONNECT_AUTHENTICATE_TOKEN` — exercises bearer-token auth.
- `SPARK_ICEBERG_TEST_ENABLED` — enables Iceberg tests (server needs Iceberg packages).
- `SPARK_GENERATE_GOLDEN_FILES` — regenerates golden files under `Tests/.../Resources/queries`.

Use the Swift Testing framework (`import Testing`, `@Test`, `@Suite`,
`#expect`/`#require`) — **not** XCTest. Tests use `@testable import SparkConnect`,
and `SQLHelper` provides `withTable`/`withDatabase` scoping helpers.

## Coding conventions

- **ASF license header is mandatory** on source files. CI's "License Check"
  (`skywalking-eyes`) enforces it. Swift files use the `//`-style header — copy
  it from the top of any existing `.swift` file. Exempt paths (see
  `.github/.licenserc.yaml`): `**/*.md`, `Package.swift`, `**/*pb.swift`,
  `.github/**`, `Tests/.../Resources/queries/**`, `LICENSE`, `NOTICE`, `.asf.yaml`.
- Match the surrounding style: 2-space indentation, no reformatting of untouched code.
- Markdown: `markdownlint` config in `.markdownlint.yaml` (only `MD013`/line-length
  is disabled).
- Follow the project's own coding philosophy — minimal, surgical changes; no
  speculative abstractions; every changed line should trace to the task.

## Contribution workflow

This repo follows standard Apache Spark process.

- **One JIRA per change.** Titles use `[SPARK-XXXXX] Summary` for both commits and
  PRs (PRs auto-link `SPARK-` ids via `.asf.yaml`).
- **PR description in English**, filling out the
  [.github/PULL_REQUEST_TEMPLATE](.github/PULL_REQUEST_TEMPLATE) sections: *What
  changes*, *Why*, *Does this PR introduce any user-facing change*, *How was this
  patch tested*, and the generative-AI disclosure.
- **Merge style**: squash or rebase only — merge commits are disabled.
- Maintainer scripts in `dev/` (require `JIRA_ACCESS_TOKEN`):
  - `python dev/create_jira_and_branch.py "Title" [-p PARENT] [-t TYPE] [-v VERSION]`
    — creates a SPARK JIRA issue and a local branch named after its id.
  - `python dev/merge_spark_pr.py` — committer PR-merge tool.

## Gotchas

- Never edit generated `*.pb.swift` / `*_generated.swift` / `base.grpc.swift`
  by hand — changes belong upstream (Spark Connect protos / Arrow).
- Test failures are usually "no server" or version mismatch, not code bugs —
  confirm a Spark Connect server is running and reachable at `SPARK_REMOTE`.
- Keep dependency versions as `exact` pins.

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md`
for the project's threat model, in-scope / out-of-scope
declarations, and known non-findings before reporting issues.
