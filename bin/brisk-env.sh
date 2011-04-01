#!/bin/sh

export CASSANDRA_HOME=`pwd`/`dirname $0`/../resources/cassandra

. $CASSANDRA_HOME/bin/cassandra.in.sh
 
for jar in `pwd`/`dirname $0`/../build/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

export HADOOP_CLASSPATH=$CLASSPATH

#hadoop requires absolute home
export HADOOP_HOME=`pwd`/`dirname $0`/../resources/hadoop
#export HADOOP_LOG_DIR=

#export PIG_HOME=
export PIG_CLASSPATH=$HADOOP_HOME/conf:$CLASSPATH

export HIVE_HOME=`dirname $0`/../resources/hive

export SQOOP_HOME=`dirname $0`/../resources/sqoop

export HBASE_HOME=$HADOOP_HOME
export ZOOKEEPER_HOME=$HADOOP_HOME

