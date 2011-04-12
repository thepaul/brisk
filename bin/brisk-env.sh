#!/bin/sh

export CASSANDRA_HOME=`pwd`/`dirname $0`/../resources/cassandra

. $CASSANDRA_HOME/bin/cassandra.in.sh
 
for jar in `pwd`/`dirname $0`/../build/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

for jar in `pwd`/`dirname $0`/../lib/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

for jar in `pwd`/`dirname $0`/../resources/hive/lib/hive-cassandra*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

#if you have lzo libs
export JAVA_LIBRARY_PATH=/home/ubuntu/hadoop-lzo-0.4.10/lib/native/Linux-amd64-64/
for jar in /home/ubuntu/hadoop-lzo-0.4.10/*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

export HADOOP_CLASSPATH=$CLASSPATH

#hadoop requires absolute home
export HADOOP_HOME=`pwd`/`dirname $0`/../resources/hadoop
#export HADOOP_LOG_DIR=

#export PIG_HOME=
export PIG_CLASSPATH=$HADOOP_HOME/conf:$CLASSPATH

export HIVE_HOME=`dirname $0`/../resources/hive

