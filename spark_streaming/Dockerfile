FROM python:3.10-slim-bullseye

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN pip install --no-cache-dir pyspark==3.3.2

COPY trabajo.py /app/trabajo.py

RUN curl -o /spark-sql-kafka.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar
RUN curl -o /kafka-clients.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar
RUN curl -o /elasticsearch-spark.jar https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.10.2/elasticsearch-spark-30_2.12-8.10.2.jar
RUN curl -o /commons-pool2.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

ENV JARS="/spark-sql-kafka.jar,/kafka-clients.jar,/elasticsearch-spark.jar,/commons-pool2.jar"

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2", "--jars", "${JARS}", "/app/trabajo.py"]
