FROM ubuntu
MAINTAINER shimamoto

ENV SPARK_VERSION 2.2.2
ENV HADOOP_VERSION hadoop2.7

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME /spark

ENV PYSPARK_PYTHON=/usr/bin/python
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip=0.0.0.0'

RUN apt-get update \
    && apt-get install -y --auto-remove --no-install-recommends curl git vim openjdk-8-jdk libgfortran3 python \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python get-pip.py \
    && rm get-pip.py

RUN pip install --upgrade pip \
    && pip install mleap \
    && pip install jupyter

RUN curl -O http://apache.cs.utah.edu/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz -C / \
    && rm spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz

RUN groupadd -r spark --gid=999 \
    && useradd -r -g spark --uid=999 -m spark \
    && chown -R spark:spark /spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}

RUN ln -s spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}/ spark

USER spark
