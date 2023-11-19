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

RUN wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
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

# set up ports and scripts
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/home/hduser/.local/bin

EXPOSE 50090 50070 50075 50010 50020 50090 10000 8020 9000 9864 9870 10020 19888 8888 8088 8080 8030 8031 8032 8033 8040 8042 22

COPY docker-entrypoint.sh $HADOOP_HOME/etc/hadoop/
RUN sudo chmod +x $HADOOP_HOME/etc/hadoop/docker-entrypoint.sh
ENTRYPOINT ["/home/hduser/hadoop/etc/hadoop/docker-entrypoint.sh"]
