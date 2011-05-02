#!/bin/sh

export CASSANDRA_HOME=`pwd`/`dirname $0`/../resources/cassandra
export CASSANDRA_BIN=$CASSANDRA_HOME/bin

. $CASSANDRA_BIN/cassandra.in.sh
 
for jar in `pwd`/`dirname $0`/../build/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

for jar in `pwd`/`dirname $0`/../lib/brisk*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

for jar in `pwd`/`dirname $0`/../resources/hive/lib/hive-cassandra*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

export HADOOP_CLASSPATH=$CLASSPATH

#hadoop requires absolute home
export HADOOP_HOME=`pwd`/`dirname $0`/../resources/hadoop
export HADOOP_BIN=$HADOOP_HOME/bin
#export HADOOP_LOG_DIR=

#export PIG_HOME=
export PIG_CLASSPATH=$HADOOP_HOME/conf:$CLASSPATH

export HIVE_HOME=`pwd`/`dirname $0`/../resources/hive
export HIVE_BIN=$HIVE_HOME/bin

