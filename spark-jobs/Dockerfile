# syntax=docker/dockerfile:1.7
# ============================================================
# Spark + dependencies — Wikipedia pipeline (Phase 2+)
# ============================================================
# Multi-stage build: Maven resolves the FULL tree of
# transitive dependencies (Kafka, Delta Lake, S3A) to
# rule out ClassNotFoundException at runtime.
#
# Stage 1 — Reuses the Spark image as the resolution
#           environment (only downloads Maven CLI, ~10 MB).
# Stage 2 — Clean Spark image + resolved JARs.
#
# Result: an immutable, self-contained image with no
#         dependency on Maven Central at runtime.
# ============================================================

# ── Stage 1: Dependency resolution ───────────────────────────
FROM apache/spark:3.5.4 AS deps

USER root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG MAVEN_VERSION=3.9.6
ARG MAVEN_BASE_URL=https://archive.apache.org/dist/maven/maven-3

RUN curl -fsSL \
      ${MAVEN_BASE_URL}/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    | tar xzf - -C /opt

COPY pom.xml /build/pom.xml
WORKDIR /build

RUN --mount=type=cache,target=/root/.m2 \
    /opt/apache-maven-${MAVEN_VERSION}/bin/mvn \
      dependency:copy-dependencies \
      -DoutputDirectory=/deps \
      -DincludeScope=runtime \
    -Dmaven.wagon.http.retryHandler.count=5 \
    -Dmaven.wagon.http.connectionTimeout=30000 \
    -Dmaven.wagon.http.readTimeout=120000 \
    --batch-mode

# ── Stage 2: Final Spark image ──────────────────────────────
FROM apache/spark:3.5.4

USER root
COPY --from=deps /deps/ /opt/spark/jars/
RUN pip install --no-cache-dir pymongo==4.10.1
RUN chown -R spark:spark /opt/spark/jars/ && \
    chmod 644 /opt/spark/jars/*.jar
USER spark
