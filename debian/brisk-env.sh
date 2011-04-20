#!/bin/sh

export CASSANDRA_HOME=/usr

if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
    for include in /usr/share/cassandra/cassandra.in.sh \
                   /usr/local/share/cassandra/cassandra.in.sh \
                   /opt/cassandra/cassandra.in.sh \
                   ~/.cassandra.in.sh \
                   `dirname $0`/cassandra.in.sh; do
        if [ -r $include ]; then
            . $include
            break
        fi
    done
elif [ -r $CASSANDRA_INCLUDE ]; then
    . $CASSANDRA_INCLUDE
fi

 
for jar in `find /usr/share/brisk`; do
    export CLASSPATH=$CLASSPATH:$jar
done

export HADOOP_CLASSPATH=$CLASSPATH

#hadoop requires absolute home
export HADOOP_HOME=/usr
#export HADOOP_LOG_DIR=

#export PIG_HOME=
export PIG_CLASSPATH=$HADOOP_HOME/conf:$CLASSPATH

export HIVE_HOME=/usr

