#!/bin/sh

export CASSANDRA_HOME=`dirname $0`/../resources/cassandra

. $CASSANDRA_HOME/bin/cassandra.in.sh
 
for jar in `dirname $0`/../build/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done
export HADOOP_CLASSPATH=$CLASSPATH

#zomg hadoop is so annoying requires absolute home
export HADOOP_HOME=`pwd`/`dirname $0`/../resources/hadoop
#export HADOOP_LOG_DIR=

export HIVE_HOME=`dirname $0`/../resources/hive



