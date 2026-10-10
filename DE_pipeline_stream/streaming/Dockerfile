# ─────────────────────────────────────────────────────────────────────────────
# spark image: Spark 4.2 (JDK 21, Python 3.10) + the Kafka connector + the job.
#
# Built by docker-compose.yml (service spark) with the repo root as context.
# Spark settings come from streaming/conf/, mounted at runtime and not baked in,
# so they can be edited without a rebuild.
# ─────────────────────────────────────────────────────────────────────────────
FROM spark:4.2.0-scala2.13-java21-python3-ubuntu

USER root

# Spark does not ship its Kafka connector. It is resolved here, at BUILD time,
# from Maven Central: spark-submit resolves --packages (with the transitive
# dependencies) before running any script, and a no-op script then exits.
# Only the four connector jars are kept, next to Spark's own. Spark already has
# the rest, and a second copy of a jar on the classpath means trouble. At
# runtime the job needs no network and no Ivy cache.
ARG KAFKA_CONNECTOR=org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
RUN echo 'print("connector resolved")' > /tmp/noop.py \
 && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --packages "${KAFKA_CONNECTOR}" /tmp/noop.py \
 && ls /tmp/ivy/jars \
 && cp /tmp/ivy/jars/org.apache.spark_spark-sql-kafka-0-10_*.jar \
       /tmp/ivy/jars/org.apache.spark_spark-token-provider-kafka-0-10_*.jar \
       /tmp/ivy/jars/org.apache.kafka_kafka-clients-*.jar \
       /tmp/ivy/jars/org.apache.commons_commons-pool2-*.jar \
       /opt/spark/jars/ \
 && rm -rf /tmp/ivy /tmp/noop.py

COPY --chmod=755 streaming/entrypoint.sh /opt/streaming/
COPY streaming/stream_analyzer.py /opt/streaming/
COPY streaming/analyzer/ /opt/streaming/analyzer/

# HOME: compose runs the container as your UID, which has no home directory here.
ENV HOME=/tmp \
    PYTHONDONTWRITEBYTECODE=1

USER spark
WORKDIR /opt/streaming
# tini, PID 1: forwards SIGTERM to spark-submit and reaps zombies.
# entrypoint.sh: gives an arbitrary UID a user name (see the script).
ENTRYPOINT ["/usr/bin/tini", "-s", "--", "/opt/streaming/entrypoint.sh"]
CMD ["/opt/spark/bin/spark-submit", "/opt/streaming/stream_analyzer.py"]
