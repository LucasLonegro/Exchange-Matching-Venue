# syntax=docker/dockerfile:1.7
# LMB distribution image: every service on one classpath, started by deploy/bin/lmb-service.
# Build from the repository root after `./mvnw -DskipTests install`:
#   docker build -f deploy/docker/Dockerfile -t lmb:0.1.0 .
# Run (Kubernetes passes the service as the argument):  lmb-service node | gateway | marketdata | ...
# Pin the base image by digest in the release pipeline; the tag below is the readable default.
ARG JRE_IMAGE=eclipse-temurin:21-jre-noble
FROM ${JRE_IMAGE}

# tini: PID 1 that forwards SIGTERM and reaps (the thread-pinning helper is detached).
# util-linux: taskset/setsid for CPU pinning; procps: ps -L for diagnostics; linuxptp is NOT here
# (time sync is a host concern).
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini util-linux procps ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 lmb \
 && useradd --system --uid 10001 --gid lmb --home-dir /var/lib/lmb --shell /usr/sbin/nologin lmb \
 && mkdir -p /opt/lmb/lib /opt/lmb/bin /etc/lmb /var/lib/lmb /var/log/lmb \
 && chown -R lmb:lmb /var/lib/lmb /var/log/lmb

COPY dist/target/lmb/lib/ /opt/lmb/lib/
COPY deploy/bin/lmb-service deploy/bin/lmb-pin-threads /opt/lmb/bin/

ENV LMB_HOME=/opt/lmb \
    LMB_TOPOLOGY=/etc/lmb/topology.properties \
    LMB_DATA_DIR=/var/lib/lmb \
    LMB_LOG_DIR=/var/log/lmb \
    PATH=/opt/lmb/bin:$PATH

USER 10001:10001
WORKDIR /var/lib/lmb
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/opt/lmb/bin/lmb-service"]
CMD ["node"]
