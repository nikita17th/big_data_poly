FROM ubuntu:18.04

# prepare system
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN apt-get update -y && \
    apt-get install -y \
    vim \
    wget \
    ssh \
    less \
    unzip \
    openjdk-8-jdk \
    sudo \
    python3-pip

RUN cd /usr/bin && \
    sudo ln -s python3 python && \
    python3 -m pip install --upgrade pip

# create user
RUN useradd -m hduser && \
    echo "hduser:supergroup" | chpasswd && \
    adduser hduser sudo && \
    echo "hduser     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers 

WORKDIR /home/hduser

USER hduser

# start ssh
COPY ssh_config /etc/ssh/ssh_config

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# install hadoop
ENV HADOOP_VERSION 2.10.1
ENV HADOOP_HOME /home/hduser/hadoop

RUN wget -q https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar xzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

ENV HDFS_NAMENODE_USER hduser
ENV HDFS_DATANODE_USER hduser
ENV HDFS_SECONDARYNAMENODE_USER hduser

ENV YARN_RESOURCEMANAGER_USER hduser
ENV YARN_NODEMANAGER_USER hduser

RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
COPY core-site.xml $HADOOP_HOME/etc/hadoop/
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/
COPY yarn-site.xml $HADOOP_HOME/etc/hadoop/

# install spark
ENV SPARK_VERSION 2.4.8
ENV SPARK_HADOOP hadoop2.7
ENV SPARK_HOME /home/hduser/spark
ENV HADOOP_CONF_DIR /home/hduser/hadoop/etc/hadoop/

RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-$SPARK_HADOOP.tgz && \
    tar xzf spark-$SPARK_VERSION-bin-$SPARK_HADOOP.tgz && \
    rm spark-$SPARK_VERSION-bin-$SPARK_HADOOP.tgz && \
    mv spark-$SPARK_VERSION-bin-$SPARK_HADOOP $SPARK_HOME

COPY spark-env.sh $SPARK_HOME/conf/


# install hive
ENV HIVE_VERSION 2.3.9
ENV HIVE_HOME /home/hduser/hive

RUN wget https://apache-mirror.rbc.ru/pub/apache/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz && \
    tar xzf apache-hive-$HIVE_VERSION-bin.tar.gz && \
    mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME && \
    rm apache-hive-$HIVE_VERSION-bin.tar.gz && \
    $HIVE_HOME/bin/schematool -dbType derby -initSchema

COPY hive-site.xml $HIVE_HOME/conf/

# install zeppelin
ENV ZEPPELIN_VERSION 0.10.0
ENV ZEPPELIN_HOME /home/hduser/zeppelin

RUN wget https://artfiles.org/apache.org/zeppelin/zeppelin-$ZEPPELIN_VERSION/zeppelin-$ZEPPELIN_VERSION-bin-all.tgz && \
    tar xzf zeppelin-$ZEPPELIN_VERSION-bin-all.tgz && \
    rm zeppelin-$ZEPPELIN_VERSION-bin-all.tgz && \
    mv zeppelin-$ZEPPELIN_VERSION-bin-all zeppelin

COPY zeppelin-env.sh $ZEPPELIN_HOME/conf/

COPY big_data_poly-assembly-0.1.0-SNAPSHOT.jar /home/hduser/
COPY Analytics_VK.zpln /home/hduser/

# set up ports and scripts
ENV TZ=UTC
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HIVE_HOME/bin:$ZEPPELIN_HOME/bin:/home/hduser/.local/bin

EXPOSE 50090 50070 50075 50010 50020 50090 10000 8020 9000 9864 9870 10020 19888 8888 8088 8080 8030 8031 8032 8033 8040 8042 22

COPY docker-entrypoint.sh $HADOOP_HOME/etc/hadoop/
RUN sudo chmod +x $HADOOP_HOME/etc/hadoop/docker-entrypoint.sh
ENTRYPOINT ["/home/hduser/hadoop/etc/hadoop/docker-entrypoint.sh"]
