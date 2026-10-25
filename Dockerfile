# Local runtime that mirrors a Microsoft Fabric Lakehouse: Spark 3.5 + Delta Lake.
FROM spark:3.5.3-python3

USER root
RUN pip install --no-cache-dir delta-spark==3.2.1 pyyaml==6.0.2 \
    && ln -sf /usr/bin/python3 /usr/local/bin/python \
    && mkdir -p /opt/ivy2 && chown -R spark /opt/ivy2
USER spark

# Download the Delta Lake jars at build time so pipeline runs need no network.
COPY --chown=spark src/spark_session.py /tmp/warmup/spark_session.py
RUN cd /tmp/warmup && python -c "from spark_session import get_spark; get_spark('warmup').stop()"

WORKDIR /workspace
