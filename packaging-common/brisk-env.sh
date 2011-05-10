#!/bin/sh

if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=$(readlink -f `which java` | sed "s:bin/java::")
fi

export CASSANDRA_HOME=/usr/share/brisk/cassandra
export CASSANDRA_BIN=/usr/sbin

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

# jasper hack
for jar in `find /usr/share/brisk/hadoop/lib/jasper*.jar`; do
    export CLASSPATH=$jar:$CLASSPATH
done
for jar in `find /usr/share/brisk/*/lib`; do
    export CLASSPATH=$CLASSPATH:$jar
done

if [ -n "$HADOOP_NATIVE_ROOT" ]; then
    for jar in $HADOOP_NATIVE_ROOT/*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
    done

    JAVA_PLATFORM=`$HADOOP_HOME/bin/hadoop org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`

    export JAVA_LIBRARY_PATH=$HADOOP_NATIVE_ROOT/lib/native/${JAVA_PLATFORM}/
fi

export HADOOP_CLASSPATH=$CLASSPATH

#hadoop requires absolute home
export HADOOP_HOME=/usr/share/brisk/hadoop
CLASSPATH=$CLASSPATH:$HADOOP_HOME
export HADOOP_CONF_DIR=/etc/brisk/hadoop
export HADOOP_BIN=$HADOOP_HOME/bin
export HADOOP_LOG_DIR=/var/log/hadoop

#make the hadoop command accessible
export PATH=$HADOOP_BIN:$PATH


export HIVE_HOME=/usr/share/brisk/hive
export HIVE_CONF_DIR=/etc/brisk/hive
export HIVE_BIN=$HIVE_HOME/bin
export HIVE_LOG_ROOT=/var/log/hive
