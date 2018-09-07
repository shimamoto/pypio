FROM ubuntu
MAINTAINER shimamoto

ENV SPARK_VERSION 2.2.2
ENV HADOOP_VERSION hadoop2.7
ENV lIVY_VERSION 0.5.0-incubating

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME /spark

ARG SPARK_MASTER

RUN apt-get update \
    && apt-get install -y --auto-remove --no-install-recommends curl git vim zip unzip openjdk-8-jdk libgfortran3 python-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -O http://apache.cs.utah.edu/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz -C / \
    && rm spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz

RUN curl -O http://archive.apache.org/dist/incubator/livy/${lIVY_VERSION}/livy-${lIVY_VERSION}-bin.zip \
    && unzip livy-${lIVY_VERSION}-bin.zip \
    && rm livy-${lIVY_VERSION}-bin.zip \
    && echo "livy.spark.master = ${SPARK_MASTER}" >> /livy-${lIVY_VERSION}-bin/conf/livy.conf \
    && echo "livy.file.local-dir-whitelist = ${SPARK_HOME}" >> /livy-${lIVY_VERSION}-bin/conf/livy.conf

RUN groupadd -r spark --gid=999 \
    && useradd -r -g spark --uid=999 -m spark \
    && chown -R spark:spark /spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} \
    && chown -R spark:spark /livy-${lIVY_VERSION}-bin

RUN ln -s spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}/ spark \
    && ln -s livy-${lIVY_VERSION}-bin/ livy

USER spark
