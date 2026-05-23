FROM apache/flink:1.19.1-scala_2.12-java17

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar /opt/flink/lib/flink-connector-kafka-3.2.0-1.19.jar
ADD https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/flink/lib/kafka-clients-3.4.0.jar
ADD https://repo.maven.apache.org/maven2/com/google/guava/guava/32.1.2-jre/guava-32.1.2-jre.jar /opt/flink/lib/guava-32.1.2-jre.jar
RUN chmod 0644 /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
    /opt/flink/lib/flink-connector-kafka-3.2.0-1.19.jar \
    /opt/flink/lib/kafka-clients-3.4.0.jar \
    /opt/flink/lib/guava-32.1.2-jre.jar

WORKDIR /app
COPY anomaly.py processor.py ./

CMD ["python", "processor.py"]
