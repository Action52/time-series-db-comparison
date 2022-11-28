# Implements spark-3.3.0 with hadoop3 in the image.
# This image is used to create a spark master node, as well as worker nodes.

FROM openjdk:8-alpine
RUN apk --update add wget tar bash
RUN wget http://apache.mirror.anlx.net/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
RUN tar -xzf spark-3.3.0-bin-hadoop3.tgz && \
    mv spark-3.3.0-bin-hadoop3 /spark && \
    rm spark-3.3.0-bin-hadoop3.tgz
COPY spark-image/raise_spark.sh .
CMD /bin/sh raise_spark.sh $SPARK_NODE_TYPE