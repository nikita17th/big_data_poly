#!/bin/bash

sudo service ssh start

if [ ! -d "/tmp/hadoop-hduser/dfs/name" ]; then
        $HADOOP_HOME/bin/hdfs namenode -format
fi

function finish {
    $ZEPPELIN_HOME/bin/zeppelin-daemon.sh stop
    $HADOOP_HOME/sbin/stop-dfs.sh
    $HADOOP_HOME/sbin/stop-yarn.sh
    echo 'Bye, bye!'
}

trap finish SIGTERM
trap finish SIGINT
trap finish EXIT

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
$ZEPPELIN_HOME/bin/zeppelin-daemon.sh start

hdfs dfs -mkdir -p /tmp /logs /user/hduser /user/hive/warehouse
hdfs dfs -chmod +w /tmp /logs /user/hduser /user/hive/warehouse

bash
