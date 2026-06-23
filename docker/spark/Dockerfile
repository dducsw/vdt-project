# Declare ARGs before first FROM for global scope
ARG PYTHON_VERSION=3.11
ARG SPARK_VERSION=3.5.6
ARG SPARK_SCALA_VERSION=2.12
ARG POSTGRESQL_JAR_VERSION=42.7.3
ARG MYSQL_JDBC_VERSION=8.3.0
ARG KAFKA_VERSION=3.7.0
ARG SPARK_KAFKA_VERSION=3.5.6

# Stage 1: Base Image
FROM python:${PYTHON_VERSION}-slim-bookworm AS spark-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \
    curl \
    wget \
    vim \
    unzip \
    rsync \
    software-properties-common \
    openjdk-17-jdk \
    build-essential \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Stage 2: Spark Installation
FROM spark-base AS spark

# Redeclare ARGs inside stage if needed
ARG SPARK_VERSION
ARG SPARK_SCALA_VERSION
ARG POSTGRESQL_JAR_VERSION
ARG MYSQL_JDBC_VERSION
ARG KAFKA_VERSION
ARG SPARK_KAFKA_VERSION

ENV SPARK_HOME="/root/spark"
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Install Kafka, Protobuf, and MySQL JDBC JARs
RUN wget "https://repo1.maven.org/maven2/org/apache/spark/spark-protobuf_${SPARK_SCALA_VERSION}/${SPARK_KAFKA_VERSION}/spark-protobuf_${SPARK_SCALA_VERSION}-${SPARK_KAFKA_VERSION}.jar" \
    -O $SPARK_HOME/jars/spark-protobuf_${SPARK_SCALA_VERSION}-${SPARK_KAFKA_VERSION}.jar && \
    wget "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SPARK_SCALA_VERSION}/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_${SPARK_SCALA_VERSION}-${SPARK_KAFKA_VERSION}.jar" \
    -O $SPARK_HOME/jars/spark-sql-kafka-0-10_${SPARK_SCALA_VERSION}-${SPARK_KAFKA_VERSION}.jar && \
    wget "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SPARK_SCALA_VERSION}/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_${SPARK_SCALA_VERSION}-${SPARK_KAFKA_VERSION}.jar" \
    -O $SPARK_HOME/jars/spark-token-provider-kafka-0-10_${SPARK_SCALA_VERSION}-${SPARK_KAFKA_VERSION}.jar && \
    wget "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar" \
    -O $SPARK_HOME/jars/kafka-clients-${KAFKA_VERSION}.jar && \
    wget "https://repo1.maven.org/maven2/org/apache/kafka/kafka_${SPARK_SCALA_VERSION}/${KAFKA_VERSION}/kafka_${SPARK_SCALA_VERSION}-${KAFKA_VERSION}.jar" \
    -O $SPARK_HOME/jars/kafka_${SPARK_SCALA_VERSION}-${KAFKA_VERSION}.jar && \
    wget "https://repo1.maven.org/maven2/commons-pool/commons-pool/1.5.4/commons-pool-1.5.4.jar" \
    -O $SPARK_HOME/jars/commons-pool-1.5.4.jar && \
    wget "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar" \
    -O $SPARK_HOME/jars/commons-pool2-2.12.1.jar && \
    wget "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_JDBC_VERSION}/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar" \
    -O $SPARK_HOME/jars/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar

# Stage 3: Spark Dependencies
FROM spark AS spark-deps

# Copy scripts
COPY ./common/ ./common/
COPY ./init/ ./init/

# Fix line endings (CRLF to LF)
RUN find ./init ./common -name "*.sh" -exec sed -i 's/\r$//' {} +

# Run dependency script
RUN chmod u+x ./init/dependency.sh && ./init/dependency.sh

# Stage 4: Python Libraries
FROM spark-deps AS spark-python

COPY ./requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV PYSPARK_PYTHON=python3

RUN chmod u+x ./sbin/* && chmod u+x ./bin/*
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

RUN chmod u+x ./init/init.sh
ENTRYPOINT [ "./init/init.sh" ]