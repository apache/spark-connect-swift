#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM swift:6.1 AS builder

WORKDIR /app

COPY . .

RUN swift build -c release

FROM swift:6.1-slim

ARG SPARK_UID=185

LABEL org.opencontainers.image.authors="Apache Spark project <dev@spark.apache.org>"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Apache Spark Connect for Swift"

ENV SPARK_SWIFT_HOME=/opt/spark-swift
ENV SPARK_SWIFT_APP=SparkConnectSwiftNetworkWordCount

WORKDIR $SPARK_SWIFT_HOME

RUN groupadd --system --gid=$SPARK_UID spark && \
    useradd --system --home-dir $SPARK_SWIFT_HOME --uid=$SPARK_UID --gid=spark spark && \
    chown -R spark:spark $SPARK_SWIFT_HOME

COPY --from=builder --chown=spark:spark /app/.build/*-unknown-linux-gnu/release/$SPARK_SWIFT_APP .

USER spark

ENTRYPOINT ["/bin/sh", "-c", "$SPARK_SWIFT_HOME/$SPARK_SWIFT_APP"]
